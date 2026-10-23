#!/usr/bin/env python3
"""Exercise refresh, ADD-PATH and per-family GR EOR with real BGP TCP sessions."""
import argparse
import contextlib
import ipaddress
import json
from pathlib import Path
import socket
import struct
import threading
import time

from r03_rpki_vrf import GLOBAL, bgp_frame, connect_bgp, daemon, read_exact, wait_for


V4 = '203.0.113.0/24'
V6 = '2001:db8:10::/48'


def nlris(prefix, path_id=None):
    p = ipaddress.ip_network(prefix)
    return (b'' if path_id is None else struct.pack('!I', path_id)) + bytes([p.prefixlen]) + p.network_address.packed[:(p.prefixlen+7)//8]


def decode_nlris(data, family, addpath):
    paths = []
    while data:
        path_id = int.from_bytes(data[:4], 'big') if addpath else 0
        data = data[4:] if addpath else data
        bits = data[0]
        size = (bits+7)//8
        address = ipaddress.ip_address(data[1:1+size].ljust(4 if family == 1 else 16, b'\0'))
        paths.append((family, str(ipaddress.ip_network((address, bits))), path_id))
        data = data[1+size:]
    return paths


class Peer:
    def __init__(self, address, asn, receive_addpath=False, send_addpath=False, enhanced=True, gr=False, restarting=False, restart_time=60, gr_families=None, llgr=None):
        self.address, self.asn = address, asn
        self.receive_addpath, self.send_addpath = receive_addpath, send_addpath
        self.routes, self.messages = set(), []
        self.lock = threading.Lock()
        self.error, self.stopping = None, False
        self.ready = threading.Event()
        caps = bytes.fromhex('0200010400010001010400020001')  # refresh and IPv4/IPv6 MP
        if enhanced:
            caps += bytes([70, 0])
        if receive_addpath or send_addpath:
            mode = int(receive_addpath) | (int(send_addpath) << 1)
            caps += bytes([69, 8]) + struct.pack('!HBBHBB', 1, 1, mode, 2, 1, mode)
        if gr:
            families = {1: True, 2: True} if gr_families is None else gr_families
            value = struct.pack('!H', restart_time | (0x8000 if restarting else 0))
            value += b''.join(struct.pack('!HBB', family, 1, 128 if preserved else 0) for family, preserved in families.items())
            caps += bytes([64, len(value)]) + value
        if llgr is not None:
            value = b''.join(struct.pack('!HBB', family, 1, 128 if preserved else 0) + seconds.to_bytes(3, 'big') for family, (preserved, seconds) in llgr.items())
            caps += bytes([71, len(value)]) + value
        options = bytes([2, len(caps)]) + caps
        body = struct.pack('!BHH4sB', 4, asn, 90, socket.inet_aton(address), len(options)) + options
        self.opening = bgp_frame(1, body) + bgp_frame(4)
        wait_for(self.connect, 'BGP handshake unavailable')
        self.conn.settimeout(None)
        self.ready.set()
        self.worker = threading.Thread(target=self.receive, daemon=True)
        self.worker.start()
        wait_for(self.ready.is_set, 'BGP handshake failed')

    def connect(self):
        conn = connect_bgp(self.address, self.opening)
        if conn is None:
            return False
        self.conn = conn
        return True

    def receive(self):
        try:
            while True:
                header = read_exact(self.conn, 19)
                length, kind = struct.unpack('!HB', header[16:])
                body = read_exact(self.conn, length-19)
                if kind == 3:
                    raise AssertionError('NOTIFICATION ' + body.hex())
                if kind == 4:
                    self.ready.set()
                with self.lock:
                    if kind == 5:
                        family, subtype, safi = struct.unpack('!HBB', body)
                        self.messages.append(('refresh', family, subtype, safi))
                    if kind != 2:
                        continue
                    n = int.from_bytes(body[:2], 'big')
                    removed = decode_nlris(body[2:2+n], 1, self.receive_addpath)
                    size = int.from_bytes(body[2+n:4+n], 'big')
                    added = decode_nlris(body[4+n+size:], 1, self.receive_addpath)
                    attrs = body[4+n:4+n+size]
                    while attrs:
                        flags, code = attrs[:2]
                        offset = 4 if flags & 16 else 3
                        length = int.from_bytes(attrs[2:offset], 'big')
                        value, attrs = attrs[offset:offset+length], attrs[offset+length:]
                        if code in (14, 15):
                            family, safi = struct.unpack('!HB', value[:3])
                            if code == 14:
                                offset = 5 + value[3]
                                added += decode_nlris(value[offset:], family, self.receive_addpath)
                            else:
                                removed += decode_nlris(value[3:], family, self.receive_addpath)
                    self.routes.difference_update(removed)
                    self.routes.update(added)
                    self.messages.append(('update', tuple(added), tuple(removed)))
        except (OSError, EOFError, AssertionError, ValueError) as error:
            if not self.stopping:
                self.error = str(error)

    def announce(self, prefix, path_id=0, withdraw=False, aspath=None, communities=()):
        family = 1 if ipaddress.ip_network(prefix).version == 4 else 2
        paths = nlris(prefix, path_id if self.send_addpath else None)
        aspath = aspath or [self.asn]
        attrs = bytes([64, 1, 1, 0, 64, 2, 2+2*len(aspath), 2, len(aspath)]) + b''.join(struct.pack('!H', n) for n in aspath)
        if communities:
            attrs += bytes([192, 8, 4*len(communities)]) + b''.join(struct.pack('!I', n) for n in communities)
        if family == 1:
            attrs += bytes([64, 3, 4]) + socket.inet_aton(self.address)
            body = struct.pack('!H', len(paths)) + paths + b'\0\0' if withdraw else b'\0\0' + struct.pack('!H', len(attrs)) + attrs + paths
        else:
            value = struct.pack('!HB', 2, 1)
            if not withdraw:
                value += bytes([16]) + socket.inet_pton(socket.AF_INET6, '2001:db8::1') + b'\0'
            value += paths
            mp = bytes([128, 15 if withdraw else 14, len(value)]) + value
            attrs = mp if withdraw else attrs + mp
            body = b'\0\0' + struct.pack('!H', len(attrs)) + attrs
        self.conn.sendall(bgp_frame(2, body))

    def refresh(self, family, subtype):
        self.conn.sendall(bgp_frame(5, struct.pack('!HBB', family, subtype, 1)))

    def eor(self, family):
        self.conn.sendall(bgp_frame(2, b'\0\0\0\0' if family == 1 else bytes.fromhex('00000006800f03000201')))

    def count(self, prefix):
        with self.lock:
            return sum(p[1] == prefix for p in self.routes)

    def paths(self, prefix):
        with self.lock:
            return {p for p in self.routes if p[1] == prefix}

    def update_count(self, family):
        with self.lock:
            return sum(m[0] == 'update' and any(p[0] == family for p in m[1] + m[2]) for m in self.messages)

    def refresh_ends(self):
        with self.lock:
            return sum(m[0] == 'refresh' and m[2] == 2 for m in self.messages)

    def close(self):
        self.stopping = True
        with contextlib.suppress(OSError):
            self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()
        self.worker.join(timeout=3)


def config_peer(address, asn, send_max=0, receive=False, gr=False, llgr=None):
    text = f'[[neighbors]]\n[neighbors.config]\nneighbor-address = "{address}"\npeer-as = {asn}\n[neighbors.transport.config]\npassive-mode = true\n'
    if gr:
        text += '[neighbors.graceful-restart.config]\nenabled = true\nrestart-time = 60\n'
        if llgr is not None:
            text += 'long-lived-enabled = true\n'
    for family in ('ipv4-unicast', 'ipv6-unicast'):
        text += f'[[neighbors.afi-safis]]\n[neighbors.afi-safis.config]\nafi-safi-name = "{family}"\nenabled = true\n[neighbors.afi-safis.add-paths.config]\nreceive = {str(receive).lower()}\nsend-max = {send_max}\n'
        if gr:
            text += '[neighbors.afi-safis.mp-graceful-restart.config]\nenabled = true\n'
        if llgr is not None:
            text += f'[neighbors.afi-safis.long-lived-graceful-restart.config]\nenabled = true\nrestart-time = {llgr}\n'
    return text


def addpath_case(binary, limit, enhanced=True):
    peers = []
    config = GLOBAL + config_peer('127.0.0.2', 65010) + config_peer('127.0.0.4', 65011) + config_peer('127.0.0.3', 65020, send_max=limit)
    try:
        with daemon(binary, config):
            a = Peer('127.0.0.2', 65010); peers.append(a)
            b = Peer('127.0.0.4', 65011); peers.append(b)
            out = Peer('127.0.0.3', 65020, receive_addpath=True, enhanced=enhanced); peers.append(out)
            for prefix in (V4, V6):
                a.announce(prefix)
                wait_for(lambda: out.count(prefix) == 1, 'initial preferred path missing')
                b.announce(prefix, aspath=[65011, 65012])
                if limit == 2:
                    wait_for(lambda: out.count(prefix) == 2, 'second ADD-PATH path missing')
            b.announce('198.51.100.0/24')
            wait_for(lambda: out.count('198.51.100.0/24') == 1, 'fallback input barrier missing')
            for family, prefix in ((1, V4), (2, V6)):
                for _ in range(2):
                    ends, updates, paths = out.refresh_ends(), out.update_count(family), out.paths(prefix)
                    out.refresh(family, 0)
                    wait_for(lambda: out.update_count(family) > updates, 'refresh replay missing')
                    assert out.count(prefix) == limit, 'refresh exceeded ADD-PATH send-max'
                    assert out.paths(prefix) == paths, 'refresh changed advertised Path-IDs'
                    if enhanced:
                        wait_for(lambda: out.refresh_ends() > ends, 'enhanced refresh response lacks EoRR')
                    else:
                        assert out.refresh_ends() == 0, 'legacy peer received enhanced markers'
            for prefix in (V4, V6):
                b.announce(prefix, withdraw=True)
                wait_for(lambda: out.count(prefix) == 1, 'fallback withdrawal left stale Path-ID')
                a.announce(prefix, withdraw=True)
                wait_for(lambda: out.count(prefix) == 0, 'last withdrawal left stale Path-ID')
            assert all(p.error is None for p in peers), [p.error for p in peers]
    finally:
        for p in peers:
            p.close()
    return {'case': 'refresh-addpath', 'send-max': limit, 'enhanced': enhanced, 'result': 'PASS'}


def enhanced_case(binary):
    peers = []
    config = GLOBAL + config_peer('127.0.0.2', 65010, receive=True) + config_peer('127.0.0.3', 65020, send_max=4)
    try:
        with daemon(binary, config):
            source = Peer('127.0.0.2', 65010, send_addpath=True); peers.append(source)
            out = Peer('127.0.0.3', 65020, receive_addpath=True); peers.append(out)
            for path_id in (11, 22):
                source.announce(V4, path_id)
            source.announce(V6, 33)
            wait_for(lambda: out.count(V4) == 2 and out.count(V6) == 1, 'initial paths missing')
            source.refresh(1, 1)
            source.announce(V4, 11)
            source.eor(1)
            time.sleep(0.2)
            assert out.count(V4) == 2, 'EOR incorrectly finished enhanced refresh'
            source.refresh(1, 2)
            wait_for(lambda: out.count(V4) == 1, 'EoRR retained an omitted Path-ID')
            assert out.count(V6) == 1, 'refresh crossed address families'
            source.refresh(2, 2)
            source.refresh(1, 99)
            time.sleep(0.2)
            assert out.count(V6) == 1 and out.count(V4) == 1, 'unpaired or unknown refresh removed a route'
            source.refresh(1, 1)
            source.refresh(1, 2)
            wait_for(lambda: out.count(V4) == 0, 'empty refresh did not remove all omitted paths')
            assert out.count(V6) == 1
            ends = out.refresh_ends()
            out.refresh(1, 0)
            wait_for(lambda: out.refresh_ends() > ends, 'empty refresh response missing markers')
            assert out.count(V4) == 0
            assert all(p.error is None for p in peers), [p.error for p in peers]
    finally:
        for p in peers:
            p.close()
    return {'case': 'enhanced-refresh', 'result': 'PASS'}


def gr_case(binary):
    peers = []
    config = GLOBAL + config_peer('127.0.0.2', 65010, gr=True) + config_peer('127.0.0.3', 65020)
    try:
        with daemon(binary, config):
            source = Peer('127.0.0.2', 65010, gr=True); peers.append(source)
            out = Peer('127.0.0.3', 65020); peers.append(out)
            source.announce(V4); source.announce(V6)
            wait_for(lambda: out.count(V4) == 1 and out.count(V6) == 1, 'initial GR paths missing')
            source.eor(1); source.eor(2)
            source.refresh(2, 1)
            time.sleep(0.1)
            source.close(); peers.remove(source)
            time.sleep(1)
            assert out.count(V4) == 1 and out.count(V6) == 1, 'GR did not retain stale routes'
            source = Peer('127.0.0.2', 65010, gr=True, restarting=True); peers.append(source)
            source.refresh(1, 1); source.refresh(1, 2); source.refresh(2, 2)
            time.sleep(0.2)
            assert out.count(V4) == 1, 'BoRR before initial EOR purged GR routes'
            source.eor(1)
            wait_for(lambda: out.count(V4) == 0, 'IPv4 EOR waited for another family')
            assert out.count(V6) == 1, 'IPv4 EOR removed IPv6'
            source.eor(2)
            wait_for(lambda: out.count(V6) == 0, 'IPv6 EOR did not purge stale routes')
            assert all(p.error is None for p in peers), [p.error for p in peers]
    finally:
        for p in peers:
            p.close()
    return {'case': 'gr-eor-family', 'result': 'PASS'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gobgpd', required=True)
    parser.add_argument('--case', choices=['all', 'addpath', 'enhanced', 'eor'], default='all')
    args = parser.parse_args()
    binary = str(Path(args.gobgpd).resolve())
    if args.case in ('all', 'addpath'):
        for enhanced in (True, False):
            for limit in (1, 2):
                print(json.dumps(addpath_case(binary, limit, enhanced)), flush=True)
    if args.case in ('all', 'enhanced'):
        print(json.dumps(enhanced_case(binary)), flush=True)
    if args.case in ('all', 'eor'):
        print(json.dumps(gr_case(binary)), flush=True)


if __name__ == '__main__':
    main()
