"""Tests for stager.py."""
import random
import socket
import threading
import time
import unittest

from webarchiver.server import StagerServer


def run_server(server):
    """Creates and runs a server as a thread.

    Args:
        server (:obj:`webarchiver.server.StagerServer`): The server to run.

    Returns:
        :obj:`threading.Thread`: The created thread for the server.
    """
    server_run = threading.Thread(target=server.run)
    server_run.daemon = True
    server_run.start()
    return server_run


class TestStagerServer(unittest.TestCase):
    """Tests for a stager server."""

    def test_simple_creation(self):
        s = StagerServer()
        self.assertEqual(s._address[0], socket.getfqdn())
        self.assertTrue(s._address[1] >= 3000 and s._address[1] < 6000)
        self.assertTupleEqual(s._socket.listener, s._address)

    def test_address_creation(self):
        address = ('127.0.0.1', 7000)
        s = StagerServer(host=address[0], port=address[1])
        self.assertTupleEqual(s._address, address)
        self.assertTupleEqual(s._socket.listener, address)

    def test_initial_connection(self):
        s1 = StagerServer()
        s2 = StagerServer(stager_host=s1._address[0],
                          stager_port=s1._address[1])
        self.assertEqual(len(s2._stager), 1)
        self.assertEqual(len(s2._listeners), 1)
        s1_on_s2_node = s2._listeners[s1._address]
        self.assertIn(s1_on_s2_node, s2._stager)
        self.assertIn(s1._address, s2._listeners)

        # Start servers so information about `s2` is send to `s1`.
        s1_run = run_server(s1)
        s2_run = run_server(s2)
        time.sleep(0.1) # Wait for communication
        self.assertEqual(len(s1._stager), 1)
        self.assertEqual(len(s1._listeners), 1)
        s2_on_s1_node = s1._listeners[s2._address]
        self.assertIn(s2_on_s1_node, s1._stager)
        self.assertIn(s2._address, s1._listeners)

    def test_non_existing_initial_connection(self):
        pass

    def test_multiple_connections(self):
        servers = [StagerServer()]
        for i in range(9):
            s = random.choice(servers)
            servers.append(StagerServer(stager_host=s._address[0],
                                        stager_port=s._address[1]))
        servers_run = [run_server(servers[0])]
        for s in servers[1:]:
            time.sleep(0.1) # Wait for communication
            servers_run.append(run_server(s))
        time.sleep(0.1) # Wait for communication
        for s in servers:
            self.assertEqual(len(s._stager), len(servers)-1)
            self.assertEqual(len(s._listeners), len(servers)-1)

    def test_free_space(self):
        s1 = StagerServer()
        free_space = s1.free_space
        diff = int(0.5 * free_space)
        s1.free_space -= diff
        self.assertEqual(s1.free_space, free_space-diff)
        s1.free_space -= free_space * 2
        self.assertEqual(s1.free_space, 0)

    # Tests for commands
    def ggg(self):
        pass

