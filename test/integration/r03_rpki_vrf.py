#!/usr/bin/env python3
"""Run real daemon/BGP TCP sessions with deterministic RTR and FRR 8.1 ZAPI fixtures.

No privileges, host networking, kernel modules, or third-party Python packages.
Usage: python3 test/integration/r03_rpki_vrf.py --gobgpd /path/to/gobgpd
"""
import argparse
import contextlib
import ipaddress
import json
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import threading
import time


def wait_for(check, label, timeout=10):
    limit = time.monotonic() + timeout
    while time.monotonic() < limit:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError(label)


def read_exact(conn, size):
    result = b''
    while len(result) < size:
        data = conn.recv(size - len(result))
        if not data:
            raise EOFError('connection closed')
        result += data
    return result


def bgp_frame(kind, body=b''):
    return b'\xff' * 16 + struct.pack('!HB', 19 + len(body), kind) + body


def nlris(prefixes):
    result = b''
    for prefix in prefixes:
        network = ipaddress.ip_network(prefix)
        result += bytes([network.prefixlen]) + network.network_address.packed[:(network.prefixlen + 7) // 8]
    return result


def decode_nlris(data):
    result = []
    while data:
        bits = data[0]
        size = (bits + 7) // 8
        result.append(str(ipaddress.IPv4Network((int.from_bytes(data[1:1+size].ljust(4, b'\0'), 'big'), bits))))
        data = data[1+size:]
    return result


class Peer:
    def __init__(self, address, asn):
        self.address = address
        wait_for(self.connect, 'BGP listener did not start')
        self.conn.settimeout(None)
        self.routes = set()
        self.error = None
        self.ready = threading.Event()
        self.stopping = False
        self.lock = threading.Lock()
        # Four-octet AS capability deliberately omitted: send two-octet AS_PATH.
        caps = bytes.fromhex('0206010400010001')
        body = struct.pack('!BHH4sB', 4, asn, 90, socket.inet_aton(address), len(caps)) + caps
        self.conn.sendall(bgp_frame(1, body) + bgp_frame(4))
        self.worker = threading.Thread(target=self.receive, daemon=True)
        self.worker.start()
        wait_for(self.ready.is_set, 'BGP handshake did not complete')

    def connect(self):
        conn = socket.socket()
        conn.bind((self.address, 0))
        conn.settimeout(2)
        try:
            conn.connect(('127.0.0.1', 1179))
            self.conn = conn
            return True
        except (ConnectionRefusedError, ConnectionAbortedError, TimeoutError):
            conn.close()
            return False

    def receive(self):
        try:
            while True:
                header = read_exact(self.conn, 19)
                length, kind = struct.unpack('!HB', header[16:])
                body = read_exact(self.conn, length - 19)
                if kind == 4:
                    self.ready.set()
                elif kind == 3:
                    raise AssertionError('BGP NOTIFICATION ' + body.hex())
                elif kind == 2:
                    withdrawn = int.from_bytes(body[:2], 'big')
                    attrs = int.from_bytes(body[2+withdrawn:4+withdrawn], 'big')
                    with self.lock:
                        self.routes.difference_update(decode_nlris(body[2:2+withdrawn]))
                        self.routes.update(decode_nlris(body[4+withdrawn+attrs:]))
        except (EOFError, OSError, AssertionError) as error:
            if not self.stopping:
                self.error = str(error)

    def announce(self, prefixes):
        attrs = b'\x40\x01\x01\x00' + b'\x40\x02\x04\x02\x01' + struct.pack('!H', 65010)
        attrs += b'\x40\x03\x04' + socket.inet_aton('192.0.2.1')
        self.conn.sendall(bgp_frame(2, b'\x00\x00' + struct.pack('!H', len(attrs)) + attrs + nlris(prefixes)))

    def has(self, prefix):
        with self.lock:
            return prefix in self.routes

    def close(self):
        self.stopping = True
        with contextlib.suppress(OSError):
            self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()
        self.worker.join(timeout=3)


def rtr_frame(kind, session=1, payload=b''):
    return struct.pack('!BBHI', 0, kind, session, 8 + len(payload)) + payload


class RTR:
    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.port = self.listener.getsockname()[1]
        self.connected = threading.Event()
        self.serial = 1
        self.updates = b''
        self.worker = threading.Thread(target=self.serve, daemon=True)
        self.worker.start()

    def serve(self):
        try:
            self.conn, _ = self.listener.accept()
            while True:
                header = read_exact(self.conn, 8)
                _, kind, _, length = struct.unpack('!BBHI', header)
                read_exact(self.conn, length-8)
                if kind in (1, 2):
                    self.conn.sendall(rtr_frame(3) + self.updates + rtr_frame(7, payload=struct.pack('!I', self.serial)))
                    self.connected.set()
        except (EOFError, OSError):
            pass

    def change(self, flags):
        self.connected.wait(timeout=10)
        self.serial += 1
        payload = struct.pack('!BBBB4sI', flags, 24, 24, 0, socket.inet_aton('203.0.113.0'), 65099)
        self.updates = rtr_frame(4, session=0, payload=payload)
        self.conn.sendall(rtr_frame(0, payload=struct.pack('!I', self.serial)))

    def close(self):
        if hasattr(self, 'conn'):
            with contextlib.suppress(OSError):
                self.conn.shutdown(socket.SHUT_RDWR)
            self.conn.close()
        self.listener.close()
        self.worker.join(timeout=3)


class Zebra:
    def __init__(self, path):
        self.path = path
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(path))
        self.listener.listen()
        self.connected = threading.Event()
        self.registrations = set()
        self.worker = threading.Thread(target=self.serve, daemon=True)
        self.worker.start()

    def serve(self):
        try:
            self.conn, _ = self.listener.accept()
            self.connected.set()
            self.nht(0, False)  # initial empty update, ignored before a reachable state
            while True:
                header = read_exact(self.conn, 10)
                length, _, _, vrf, command = struct.unpack('!HBBIH', header)
                read_exact(self.conn, length-10)
                if command == 20:
                    self.registrations.add(vrf)
        except (EOFError, OSError):
            pass

    def nht(self, vrf, reachable, metric=10):
        self.connected.wait(timeout=10)
        # FRR 8.1, ZAPI v6: flags, family, prefix, type, instance, distance,
        # metric, nexthop count; reachable records carry VRF/type/flags/IP/ifindex.
        body = struct.pack('!IHB4sBHBI', 0, socket.AF_INET, 32, socket.inet_aton('192.0.2.1'), 2, 0, 0, metric)
        body += bytes([int(reachable)])
        if reachable:
            body += struct.pack('!IBB4sI', vrf, 2, 0, socket.inet_aton('192.0.2.1'), 1)
        self.conn.sendall(struct.pack('!HBBIH', 10+len(body), 255, 6, vrf, 22) + body)

    def close(self):
        RTR.close(self)


def neighbor(address, vrf='', policy=''):
    return f'''[[neighbors]]
[neighbors.config]
neighbor-address = "{address}"
peer-as = {65010 if int(address.split('.')[-1]) % 2 == 0 else 65020}
vrf = "{vrf}"
[neighbors.transport.config]
passive-mode = true
{policy}
'''


@contextlib.contextmanager
def daemon(binary, config):
    with tempfile.TemporaryDirectory(prefix='gobgp-r03-') as directory:
        root = Path(directory)
        (root/'config.toml').write_text(config)
        with (root/'daemon.log').open('w+') as log:
            process = subprocess.Popen([binary, '-f', str(root/'config.toml'), '--api-hosts=127.0.0.1:50051', '-l', 'warn'], stdout=log, stderr=log)
            try:
                yield
            except BaseException:
                log.flush()
                log.seek(0)
                print(log.read()[-16000:], flush=True)
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                log.flush()
                log.seek(0)
                if 'WARNING: DATA RACE' in log.read():
                    raise AssertionError('daemon race detector reported a data race')


GLOBAL = '[global.config]\nas = 65000\nrouter-id = "192.0.2.254"\nport = 1179\nlocal-address-list = ["127.0.0.1"]\n'


def rpki_case(binary, direction):
    cache = RTR()
    peers = []
    try:
        assignment = f'[neighbors.apply-policy.config]\n{direction}-policy-list = ["drop-invalid"]\ndefault-{direction}-policy = "accept-route"'
        config = GLOBAL + neighbor('127.0.0.2', policy=assignment if direction == 'import' else '')
        config += neighbor('127.0.0.3', policy=assignment if direction == 'export' else '')
        config += f'''[[rpki-servers]]
[rpki-servers.config]
address = "127.0.0.1"
port = {cache.port}
[[policy-definitions]]
name = "drop-invalid"
[[policy-definitions.statements]]
name = "invalid"
[policy-definitions.statements.conditions.bgp-conditions]
rpki-validation-result = "invalid"
[policy-definitions.statements.actions]
route-disposition = "reject-route"
'''
        with daemon(binary, config):
            source = Peer('127.0.0.2', 65010); peers.append(source)
            observer = Peer('127.0.0.3', 65020); peers.append(observer)
            wait_for(cache.connected.is_set, 'RTR reset did not complete')
            source.announce(['203.0.113.0/24', '198.51.100.0/24'])
            wait_for(lambda: observer.has('203.0.113.0/24') and observer.has('198.51.100.0/24'), 'initial routes missing')
            cache.change(1)
            wait_for(lambda: not observer.has('203.0.113.0/24') and observer.has('198.51.100.0/24'), 'stale export after invalid VRP')
            cache.change(0)
            wait_for(lambda: observer.has('203.0.113.0/24') and observer.has('198.51.100.0/24'), 'RPKI recovery missing')
            assert all(p.error is None for p in peers), [p.error for p in peers]
    finally:
        for peer in peers:
            peer.close()
        cache.close()
    return {'case': 'rpki-' + direction, 'result': 'PASS'}


def vrf_case(binary):
    with tempfile.TemporaryDirectory(prefix='zapi-r03-') as directory:
        zebra = Zebra(Path(directory)/'zserv.api')
        peers = []
        try:
            config = GLOBAL + f'[zebra.config]\nenabled = true\nurl = "unix:{zebra.path}"\nversion = 6\nsoftware-name = "frr8.1"\n'
            for index, vrf in enumerate(('blue', 'red', ''), 1):
                if vrf:
                    config += f'[[vrfs]]\n[vrfs.config]\nname = "{vrf}"\nid = {index}\nrd = "65000:{index}"\nboth-rt-list = ["65000:{index}"]\n'
                config += neighbor(f'127.0.0.{index*2}', vrf) + neighbor(f'127.0.0.{index*2+1}', vrf)
            with daemon(binary, config):
                observers = []
                for index in range(1, 4):
                    source = Peer(f'127.0.0.{index*2}', 65010); peers.append(source)
                    observer = Peer(f'127.0.0.{index*2+1}', 65020); peers.append(observer); observers.append(observer)
                    source.announce(['203.0.113.0/24'])
                wait_for(lambda: all(p.has('203.0.113.0/24') for p in observers), 'initial VRF/global routes missing')
                for vrf in (0, 1, 2):
                    zebra.nht(vrf, True)
                wait_for(lambda: {0, 1, 2}.issubset(zebra.registrations), 'NHT registration missing lookup VRF')
                zebra.nht(1, False)
                wait_for(lambda: not observers[0].has('203.0.113.0/24'), 'blue VRF did not withdraw')
                # Allow queued cross-VRF withdrawals to reach the independent observers.
                time.sleep(0.2)
                assert all(p.has('203.0.113.0/24') for p in observers[1:]), 'cross-VRF/global withdrawal'
                peers[0].announce(['203.0.114.0/24'])
                peers[2].announce(['203.0.114.0/24'])
                wait_for(lambda: observers[1].has('203.0.114.0/24'), 'red new route missing')
                assert not observers[0].has('203.0.114.0/24'), 'blue new route escaped failed NHT cache'
                zebra.nht(1, True, metric=5)
                wait_for(lambda: all(p.has('203.0.113.0/24') for p in observers) and observers[0].has('203.0.114.0/24'), 'VRF recovery missing')
                assert all(p.error is None for p in peers), [p.error for p in peers]
        finally:
            for peer in peers:
                peer.close()
            zebra.close()
    return {'case': 'vrf-nht', 'result': 'PASS'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gobgpd', required=True)
    parser.add_argument('--case', choices=['all', 'rpki-import', 'rpki-export', 'vrf'], default='all')
    args = parser.parse_args()
    binary = str(Path(args.gobgpd).resolve())
    for direction in ('import', 'export'):
        if args.case in ('all', 'rpki-' + direction):
            print(json.dumps(rpki_case(binary, direction)), flush=True)
    if args.case in ('all', 'vrf'):
        print(json.dumps(vrf_case(binary)), flush=True)


if __name__ == '__main__':
    main()
