# Copyright (C) 2015 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import unittest
import sys
import time

import collections
collections.Callable = collections.abc.Callable


from lib.noseplugin import parser_option

from lib import base
from lib.base import (
    BGP_FSM_ACTIVE,
    BGP_FSM_ESTABLISHED,
    local,
    wait_for,
)
from lib.gobgp import GoBGPContainer
from lib.exabgp import ExaBGPContainer


class GoBGPTestBase(unittest.TestCase):

    wait_per_retry = 5
    retry_limit = 10

    @classmethod
    def setUpClass(cls):
        gobgp_ctn_image_name = parser_option.gobgp_image
        base.TEST_PREFIX = parser_option.test_prefix

        g1 = GoBGPContainer(name='g1', asn=65000, router_id='192.168.0.1',
                            ctn_image_name=gobgp_ctn_image_name,
                            log_level=parser_option.gobgp_log_level)

        rs_clients = [
            ExaBGPContainer(name='q{0}'.format(i + 1), asn=(65001 + i),
                            router_id='192.168.0.{0}'.format(i + 2))
            for i in range(4)]
        ctns = [g1] + rs_clients

        initial_wait_time = max(ctn.run() for ctn in ctns)
        time.sleep(initial_wait_time)

        for i, rs_client in enumerate(rs_clients):
            g1.add_peer(rs_client, is_rs_client=True)
            as2 = False
            if i > 1:
                as2 = True
            rs_client.add_peer(g1, as2=as2)

        # advertise a route from route-server-clients
        for idx, rs_client in enumerate(rs_clients):
            route = '10.0.{0}.0/24'.format(idx + 1)
            rs_client.add_route(route)
            if idx < 2:
                route = '10.0.10.0/24'
            rs_client.add_route(route)

        cls.gobgp = g1
        cls.quaggas = {x.name: x for x in rs_clients}

    # test each neighbor state is turned establish
    def test_01_neighbor_established(self):
        for q in self.quaggas.values():
            self.gobgp.wait_for(expected_state=BGP_FSM_ESTABLISHED, peer=q)

    def test_02_check_gobgp_local_rib(self):
        for rs_client in self.quaggas.values():
            def _has_expected_routes():
                state = self.gobgp.get_neighbor_state(rs_client)
                self.assertEqual(state, BGP_FSM_ESTABLISHED)
                local_rib = self.gobgp.get_local_rib(rs_client)
                if len(local_rib) < (len(self.quaggas) - 1):
                    return False

                self.assertEqual(len(local_rib), 4)
                return True

            wait_for(
                _has_expected_routes,
                timeout=self.retry_limit * self.wait_per_retry,
                interval=self.wait_per_retry,
            )

    def test_03_stop_q2_and_check_neighbor_status(self):
        q2 = self.quaggas['q2']
        q2.remove()
        # The default Idle hold after transport loss lasts only one second.
        # Polling for it races with the normal retry transition to Active.
        self.gobgp.wait_for(expected_state=BGP_FSM_ACTIVE, peer=q2)

        expected_routes = {
            'q1': {'10.0.3.0/24', '10.0.4.0/24'},
            'q3': {'10.0.1.0/24', '10.0.4.0/24', '10.0.10.0/24'},
            'q4': {'10.0.1.0/24', '10.0.3.0/24', '10.0.10.0/24'},
        }

        def _routes_converged():
            for name, expected in expected_routes.items():
                client = self.quaggas[name]
                self.assertEqual(self.gobgp.get_neighbor_state(client), BGP_FSM_ESTABLISHED)
                actual = {route['prefix'] for route in self.gobgp.get_local_rib(client)}
                if actual != expected:
                    return False
            return True

        # Require the removed client's routes to disappear while preserving
        # every surviving client's routes and established session.
        wait_for(
            _routes_converged,
            timeout=self.retry_limit * self.wait_per_retry,
            interval=self.wait_per_retry,
            timeout_message='AS2 routes did not converge after removing q2',
        )

