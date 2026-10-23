#!/usr/bin/env python3
"""Bound incomplete Enhanced Route Refresh with real BGP TCP sessions."""
import argparse
import json
from pathlib import Path
import subprocess
import threading
import time

from r03_refresh_addpath import GLOBAL, Peer, V4, V6, config_peer, daemon, wait_for


def incomplete_case(binary, repeated=False, disabled=False):
    peers = []
    config = (GLOBAL + config_peer('127.0.0.2', 65010, receive=True, gr=True)
              + config_peer('127.0.0.4', 65011)
              + config_peer('127.0.0.3', 65020, send_max=4))
    stop, worker = threading.Event(), None
    try:
        with daemon(binary, config, ['--route-refresh-stale-time=' + ('0' if disabled else '1')]):
            source = Peer('127.0.0.2', 65010, send_addpath=True, gr=True); peers.append(source)
            other = Peer('127.0.0.4', 65011); peers.append(other)
            out = Peer('127.0.0.3', 65020, receive_addpath=True); peers.append(out)
            source.eor(1); source.eor(2)
            for path_id in (11, 22):
                source.announce(V4, path_id)
            source.announce(V6, 33)
            other.announce('198.51.100.0/24')
            wait_for(lambda: out.count(V4) == 2 and out.count(V6) == 1
                     and out.count('198.51.100.0/24') == 1, 'initial input missing')
            source.refresh(1, 1)
            source.announce(V4, 11)
            source.eor(1)
            if repeated:
                # All BoRRs mark all current input stale, but must not keep
                # moving the deadline. No routes are reannounced in this loop.
                def markers():
                    while not stop.wait(0.05):
                        source.refresh(1, 1)
                worker = threading.Thread(target=markers, daemon=True)
                worker.start()
                wait_for(lambda: out.count(V4) == 0, 'repeated BoRR extended stale retention', timeout=4)
                stop.set(); worker.join(timeout=2)
            elif disabled:
                time.sleep(1.3)
                assert out.count(V4) == 2, 'disabled deadline removed an omitted route'
                source.refresh(1, 2)
                wait_for(lambda: out.count(V4) == 1, 'explicit EoRR failed with the limit disabled')
            else:
                wait_for(lambda: out.count(V4) == 1, 'missing EoRR kept an omitted Path-ID', timeout=4)
                # The late EoRR has no paired cycle and must not remove new input.
                source.announce(V4, 22)
                wait_for(lambda: out.count(V4) == 2, 'new input after expiry missing')
                source.refresh(1, 2)
                time.sleep(0.15)
                assert out.count(V4) == 2, 'late EoRR purged input from after expiration'
            assert out.count(V6) == 1, 'refresh expiration crossed address families'
            assert out.count('198.51.100.0/24') == 1, 'refresh expiration crossed peers'
            assert all(p.error is None for p in peers), [p.error for p in peers]
    finally:
        stop.set()
        if worker is not None:
            worker.join(timeout=2)
        for p in peers:
            p.close()
    return {'case': 'refresh-timeout', 'repeated-borr': repeated, 'disabled': disabled, 'result': 'PASS'}


def disconnect_case(binary, llgr=False):
    peers = []
    stale = {1: (True, 30), 2: (True, 30)} if llgr else None
    config = GLOBAL + config_peer('127.0.0.2', 65010, gr=True, llgr=30 if llgr else None)
    config += config_peer('127.0.0.3', 65020, gr=llgr, llgr=30 if llgr else None)
    if llgr:
        config += config_peer('127.0.0.4', 65030)
    try:
        with daemon(binary, config, ['--route-refresh-stale-time=1']):
            source = Peer('127.0.0.2', 65010, gr=True, restart_time=0 if llgr else 60, llgr=stale); peers.append(source)
            out = Peer('127.0.0.3', 65020, gr=llgr, llgr=stale); peers.append(out)
            if llgr:
                plain = Peer('127.0.0.4', 65030); peers.append(plain)
            source.eor(1); source.eor(2)
            source.announce(V4); source.announce(V6)
            wait_for(lambda: out.count(V4) == 1 and out.count(V6) == 1, 'initial GR input missing')
            if llgr:
                wait_for(lambda: plain.count(V4) == 1, 'initial plain observer input missing')
            source.refresh(1, 1)
            # An UPDATE after BoRR confirms the input reader processed the marker.
            source.announce('198.51.100.0/24')
            wait_for(lambda: out.count('198.51.100.0/24') == 1, 'refresh input barrier missing')
            source.close()
            if llgr:
                wait_for(lambda: plain.count(V4) == 0, 'disconnect did not enter LLGR')
            time.sleep(1.3)
            assert out.count(V4) == 1 and out.count(V6) == 1, 'old refresh timeout shortened GR retention'
            source = Peer('127.0.0.2', 65010, gr=True, restarting=True, llgr=stale); peers.append(source)
            source.eor(1); source.eor(2)
            wait_for(lambda: out.count(V4) == 0 and out.count(V6) == 0, 'recovery EOR did not clear GR stale input')
            assert out.error is None and source.error is None, (out.error, source.error)
    finally:
        for p in peers:
            p.close()
    return {'case': 'refresh-timeout-disconnect', 'llgr': llgr, 'result': 'PASS'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gobgpd', required=True)
    args = parser.parse_args()
    binary = str(Path(args.gobgpd).resolve())
    invalid = subprocess.run([binary, '--route-refresh-stale-time=-1'], capture_output=True, text=True, timeout=5)
    assert invalid.returncode != 0 and 'route-refresh-stale-time' in invalid.stdout + invalid.stderr, 'negative retention was accepted'
    for repeated, disabled in ((False, False), (True, False), (False, True)):
        print(json.dumps(incomplete_case(binary, repeated, disabled)), flush=True)
    for llgr in (False, True):
        print(json.dumps(disconnect_case(binary, llgr)), flush=True)


if __name__ == '__main__':
    main()
