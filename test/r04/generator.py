#!/usr/bin/env python3
"""Small iBGP laboratory generator; FRR independently performs reflection."""
import ipaddress
import json
import os
from pathlib import Path
import socket
import struct
import threading
import time

root = Path(os.environ.get('LAB_DIR', '/lab'))
peer = int(os.environ['PEER_NUMBER'])
rr = os.environ.get('RR_ADDRESS', '10.231.84.2')
address = f'10.231.84.{4+peer}'
next6 = ipaddress.ip_address(f'fd42:231:84::{4+peer}').packed

def frame(kind, body=b''):
    return b'\xff'*16 + struct.pack('!HB', 19+len(body), kind) + body

def exact(conn, n):
    out = b''
    while len(out) < n:
        data = conn.recv(n-len(out))
        if not data:
            raise EOFError('reflector disconnected')
        out += data
    return out

def prefix(family, number):
    text = f'10.{peer}.{number >> 8}.{number & 255}/32' if family == 4 else f'fd10:{peer}::{number:x}/128'
    network = ipaddress.ip_network(text)
    return bytes([network.prefixlen]) + network.network_address.packed

def update(family, number, withdrawn=False, origin=0):
    nlri = prefix(family, number)
    attrs = bytes([64, 1, 1, origin, 64, 2, 0, 64, 5, 4]) + struct.pack('!I', 100)  # iBGP requires LOCAL_PREF
    if family == 4:
        if withdrawn:
            body = struct.pack('!H', len(nlri)) + nlri + b'\0\0'
        else:
            attrs += bytes([64, 3, 4]) + socket.inet_aton(address)
            body = b'\0\0' + struct.pack('!H', len(attrs)) + attrs + nlri
    else:
        value = struct.pack('!HB', 2, 1) + (b'' if withdrawn else bytes([16]) + next6 + b'\0') + nlri
        attrs = bytes([128, 15 if withdrawn else 14, len(value)]) + value if withdrawn else attrs + bytes([128, 14, len(value)]) + value
        body = b'\0\0' + struct.pack('!H', len(attrs)) + attrs
    return frame(2, body)

try:
    connection = None
    caps = bytes.fromhex('0200010400010001010400020001') + bytes([65, 4]) + struct.pack('!I', 65000)
    options = bytes([2, len(caps)]) + caps
    opening = frame(1, struct.pack('!BHH4sB', 4, 65000, 90, socket.inet_aton(address), len(options)) + options) + frame(4)
    last_error = None
    for _ in range(120):
        candidate = None
        try:
            candidate = socket.create_connection((rr, 179), timeout=2)
            candidate.sendall(opening)
            while True:
                head = exact(candidate, 19)
                length, kind = struct.unpack('!HB', head[16:])
                body = exact(candidate, length-19)
                if kind == 3:
                    raise RuntimeError('BGP NOTIFICATION ' + body.hex())
                if kind == 4:
                    break
            candidate.settimeout(None)
            connection = candidate
            break
        except (OSError, EOFError, RuntimeError) as error:
            last_error = error
            if candidate is not None:
                candidate.close()
            time.sleep(0.5)
    if connection is None:
        raise RuntimeError('reflector handshake unavailable: ' + str(last_error))
    (root/f'generator-{peer}.ready').write_text('ready')
    def receive():
        try:
            while True:
                head = exact(connection, 19)
                length, kind = struct.unpack('!HB', head[16:])
                body = exact(connection, length-19)
                if kind == 3:
                    raise RuntimeError('BGP NOTIFICATION ' + body.hex())
        except BaseException as error:
            (root/f'generator-{peer}.error').write_text(str(error))
    threading.Thread(target=receive, daemon=True).start()
    previous, keepalive = '', 0
    while True:
        phase = (root/'phase').read_text().strip() if (root/'phase').exists() else ''
        if phase and phase != previous:
            started = time.monotonic()
            if phase == 'load':
                for family in (4, 6):
                    for number in range(1250):
                        connection.sendall(update(family, number))
            elif phase == 'churn':
                for family in (4, 6):
                    for number in range(100):
                        connection.sendall(update(family, number, withdrawn=True))
                    for number in range(1250, 1350):
                        connection.sendall(update(family, number))
                    for number in range(100, 200):
                        connection.sendall(update(family, number, origin=1))
            else:
                raise ValueError('unknown phase '+phase)
            connection.sendall(frame(2, b'\0\0\0\0'))
            connection.sendall(frame(2, bytes.fromhex('00000006800f03000201')))
            (root/f'generator-{peer}.{phase}').write_text(json.dumps({'seconds': time.monotonic()-started}))
            previous = phase
        if time.monotonic()-keepalive > 2:
            connection.sendall(frame(4))
            keepalive = time.monotonic()
        time.sleep(0.02)
except BaseException as error:
    (root/f'generator-{peer}.error').write_text(str(error))
    raise
