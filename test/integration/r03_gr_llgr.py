#!/usr/bin/env python3
"""Regress GR/LLGR recovery with real TCP sessions and a race-enabled daemon."""
import argparse
import json
from pathlib import Path
import time

from r03_refresh_addpath import Peer, V4, V6, config_peer
from r03_rpki_vrf import GLOBAL, daemon, wait_for


def gr_reconnect(binary, mode):
    peers = []
    config = GLOBAL + config_peer('127.0.0.2', 65010, gr=True) + config_peer('127.0.0.3', 65020)
    try:
        with daemon(binary, config):
            old = Peer('127.0.0.2', 65010, gr=True); peers.append(old)
            out = Peer('127.0.0.3', 65020); peers.append(out)
            old.announce(V4); old.announce(V6)
            old.eor(1); old.eor(2)
            wait_for(lambda: out.count(V4) == out.count(V6) == 1, 'initial GR routes missing')
            # Leave the old TCP session alive: replacement must not wait 90s.
            started = time.monotonic()
            source = Peer('127.0.0.2', 65010, gr=mode != 'withdraw-capability', restarting=True,
                          gr_families={1: False, 2: True} if mode == 'clear-forwarding' else None)
            peers.append(source)
            assert time.monotonic() - started < 3, 'GR replacement waited for old session timeout'
            if mode == 'preserve':
                assert out.count(V4) == out.count(V6) == 1, 'GR replacement lost retained routes'
                source.eor(1)
                wait_for(lambda: out.count(V4) == 0, 'IPv4 EOR did not remove retained routes')
                assert out.count(V6) == 1, 'IPv4 EOR removed IPv6'
                source.eor(2)
            elif mode == 'clear-forwarding':
                wait_for(lambda: out.count(V4) == 0, 'cleared F bit retained IPv4')
                assert out.count(V6) == 1, 'cleared IPv4 F bit removed IPv6'
                source.eor(2)
            wait_for(lambda: out.count(V4) == out.count(V6) == 0, 'new OPEN or EOR retained obsolete routes')
            assert source.error is None and out.error is None, (source.error, out.error)
    finally:
        for p in peers:
            p.close()
    return {'case': 'gr-reconnect-' + mode, 'result': 'PASS'}


def llgr_case(binary, mode):
    peers = []
    stale = {1: (True, 3), 2: (True, 6)}
    config = GLOBAL + config_peer('127.0.0.2', 65010, gr=True, llgr=6)
    config += config_peer('127.0.0.3', 65020, gr=True, llgr=6)
    config += config_peer('127.0.0.4', 65030)
    omitted, no_llgr = '203.0.114.0/24', '203.0.115.0/24'
    try:
        with daemon(binary, config):
            source = Peer('127.0.0.2', 65010, gr=True, restart_time=10 if mode == 'llgr-only' else 1,
                          gr_families={} if mode == 'llgr-only' else None, llgr=stale); peers.append(source)
            out = Peer('127.0.0.3', 65020, gr=True, llgr=stale); peers.append(out)
            plain = Peer('127.0.0.4', 65030); peers.append(plain)
            source.announce(V4); source.announce(omitted); source.announce(V6)
            source.announce(no_llgr, communities=(0xffff0007,))
            source.eor(1); source.eor(2)
            wait_for(lambda: out.count(V4) == out.count(omitted) == out.count(V6) == out.count(no_llgr) == 1, 'initial LLGR routes missing')
            wait_for(lambda: plain.count(V4) == plain.count(V6) == 1, 'initial plain peer routes missing')
            source.close(); peers.remove(source)
            wait_for(lambda: out.count(no_llgr) == 0 and plain.count(V4) == plain.count(V6) == 0, 'GR expiry did not apply NO_LLGR and export restrictions', timeout=3)
            assert out.count(V4) == out.count(omitted) == out.count(V6) == 1, 'LLGR-capable peer lost stale routes'
            # Changing LLST in a new OPEN must not extend the old deadline.
            new_stale = {1: (True, 60), 2: (True, 60)}
            if mode == 'clear-forwarding':
                new_stale[1] = (False, 60)
            source = Peer('127.0.0.2', 65010, gr=True, restarting=True, restart_time=1,
                          gr_families={} if mode == 'llgr-only' else {1: False, 2: False}, llgr=new_stale)
            peers.append(source)
            if mode == 'clear-forwarding':
                wait_for(lambda: out.count(V4) == out.count(omitted) == 0, 'cleared LLGR F bit retained routes')
                assert out.count(V6) == 1, 'LLGR F bit crossed families'
            else:
                source.announce(V4)
                wait_for(lambda: plain.count(V4) == 1, 'refreshed route still treated as LLGR stale')
                if mode in ('eor', 'llgr-only'):
                    source.eor(1)
                if mode == 'consecutive':
                    source.close(); peers.remove(source)
                    wait_for(lambda: plain.count(V4) == 0, 'reset during LLGR leaked route to plain peer')
                    assert out.count(V4) == 1, 'consecutive reset prematurely purged LLGR route'
                wait_for(lambda: out.count(omitted) == 0, 'LLST expiry or EOR retained omitted route', timeout=5)
                time.sleep(3.2 if mode in ('eor', 'llgr-only') else 0.2)
                assert out.count(V4) == (0 if mode == 'consecutive' else 1), 'family timer mishandled refreshed route'
                assert out.count(V6) == 1, 'IPv4 recovery removed IPv6'
                if mode == 'expiry':
                    source.close(); peers.remove(source)
                    wait_for(lambda: plain.count(V4) == 0, 'reset leaked LLGR route to plain peer')
                    wait_for(lambda: out.count(V4) == 0, 'reset extended expired LLST')
            if mode not in ('expiry', 'consecutive'):
                source.eor(2)
            wait_for(lambda: out.count(V6) == 0, 'IPv6 cleanup did not complete', timeout=8)
            assert out.error is None and plain.error is None, (out.error, plain.error)
    finally:
        for p in peers:
            p.close()
    return {'case': 'llgr-' + mode, 'result': 'PASS'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gobgpd', required=True)
    args = parser.parse_args()
    binary = str(Path(args.gobgpd).resolve())
    for mode in ('preserve', 'clear-forwarding', 'withdraw-capability'):
        print(json.dumps(gr_reconnect(binary, mode)), flush=True)
    for mode in ('eor', 'expiry', 'clear-forwarding', 'consecutive', 'llgr-only'):
        print(json.dumps(llgr_case(binary, mode)), flush=True)


if __name__ == '__main__':
    main()
