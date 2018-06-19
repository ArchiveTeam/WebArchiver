"""The base of the servers."""
import logging
import pickle
import random
import select
import socket
import struct

from webarchiver.config import *

logger = logging.getLogger(__name__)


class Node:
    """Contains the listener and socket of a node in the network.

    The node has all calls the socket has.

    Args:
        socket: The socket of the node.
        listener: The listener of the node as tuple of (host, port).
    """
    #TODO add if this is a stager of crawler server?
    def __init__(self, s, listener=None):
        """Inits the Node with at least a socket.

        Args:
            s (:obj:`socket.socket`): The socket to create the node for.
            listener (tuple, optional): A tuple (host, port) of the server of
                the node.
        """
        self.socket = s
        self.listener = listener

    def __getattr__(self, attr):
        """If the attribute is not found, find it in the socket.

        Args:
            attr (str): The called attribute.

        Returns:
            Data from the socket attribute.
        """
        return getattr(self.socket, attr)

    def __repr__(self):
        return '<{} at 0x{:x} listener={}>'.format(__name__, id(self),
                                                   self.listener)


class BaseServer:
    """The base for the server for a crawler or stager."""

    def __init__(self, host=None, port=None):
        """Creates the base server with an address.

        The base server will create a node for the server and bind it at
        ``0.0.0.0`` with a certain port. A given host is not used to bind to,
        only to use for the listener address.

        A tuple (host, port) of the assigned listener is printed.

        Args:
            host (str, optional): The host to use for the listener address for
                the server. If no host is given, the FQDN will be used as host
                for the server.
            port (int, optional): The port to use for the listener address for
                the server. If no port is given, a random port number between
                3000 and 6000 is chosen.
        """
        self._address = (host or socket.getfqdn(),
                         port or random.randrange(3000, 6000))
        self._read_list = []
        self._write_list = []
        self._error_list = []
        self._write_queue = {}
        self._last_stager_request = 0
        self._last_ping = 0

        logger.info('Creating server with listener %s.', self._address)
        self._socket = Node(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                            self._address)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(('0.0.0.0', self._address[1]))
        self._socket.listen(LISTEN_QUEUE)
        self._read_list.append(self._socket)

    def run(self):
        """Runs a loop to get the new readable and writable sockets."""
        while True:
            self._run_round()

    def _run_round(self):
        """Initiates the reading from and writing to sockets."""
        read_ready, write_ready, error_ready = select.select(
            self._read_list, self._write_list, self._error_list, 1)
        for s in read_ready:
            self._read_socket(s)
        for s in write_ready:
            self._write_socket(s)
        #self._handle_error(error_ready) TODO

    def _connect_socket(self, address):
        """Connects to an address.

        Create a :class:`Node` with the given address as listener and created
        socket.

        Args:
            address (tuple): A tuple (host, port) of the server to connect to.

        Returns:
            :obj:`Node`: With the address and the connected socket.
        """
        logger.debug('Connecting to address %s.', address)
        s = Node(socket.socket(socket.AF_INET, socket.SOCK_STREAM), address)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(tuple(address))
        return s

    def _read_socket(self, s):
        """Reads a message from a :class:`Node`.

        Reads a message from a socket, send by a crawler or stager. The first
        8 bytes contain the length for the whole message. This length is used
        to received the full messages in parts. The messages is loaded with
        :mod:`pickle` and send to :func:`self._process_message` to be
        processed.

        The received message is printed to stdout as::

            received <message> from <peername>

        Args:
            s (:obj:`Node`): The node to read from.
        """
        message_length = s.recv(8)
        if len(message_length) == 0:
            return
        message_length = struct.unpack('L', message_length)[0]
        message = b''
        while len(message) < message_length:
            message += s.recv(message_length - len(message))
        message = pickle.loads(message)
        logger.debug('Received message %s from %s.', message, s)
        self._process_message(s, message)

    def _write_socket(self, s):
        """Writes all waiting messages for a :class:`Node`.

        Each message for a certain :class:`Node` is dumped with :mod:`pickle`,
        after which the length of the messages is prepended to the message. The
        message is then send.

        The :class:`Node` is put back in the read list and removed from the
        write list.

        Args:
            s (:obj:`Node`): The :class:`Node` to write the message to.
        """
        while len(self._write_queue[s]) > 0:
            message = self._write_queue[s].pop(0)
            logger.debug('Sending message %s to %s.', message, s)
            message = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
            s.sendall(struct.pack('L', len(message)) + message)
        self._read_list.append(s)
        self._write_list.remove(s)

    def _write_socket_message(self, s, *message):
        """Prepares writing a message to a :class:`Node`.

        A single :class:`Node` can be given or a list, set, or dict. The
        message it copied and added to the write queue of every given
        :class:`Node`. Each :class:`Node` is removed from the read list and
        added to the write list.

        Args:
            s (:obj:`Node` or list or set or dict): One or more :class:`Node`s
                to send the message to.
            *message: Data to be send to the :class:`Node`s.
        """
        if isinstance(s, socket.socket):
            s_original = s
            s = Node(s)
            logger.debug('Connection %s is not a node, created %s.',
                         s_original, s)
        if type(s) in [list, set, dict]:
            logger.debug('Sending message %s to %s servers.', message, len(s))
            for s_ in s:
                self._write_socket_message(s_, *message)
            return None
        self._write_queue[s].append(message)
        if s in self._read_list:
            self._read_list.remove(s)
        if s not in self._write_list:
            self._write_list.append(s)

    def _write_socket_file(self, s, filename, *message):
        """Writes a file to a :class:`Node` object.""" #TODO
        with open(filename, 'rb') as f:
            self._write_socket_message(s, message[0], filename, f.read(),
                                       *message[1:])

    def _process_message(self, s, message):
        """Processes a received message.

        The first item in a received message is the command of the messsage.
        This is changed to the attribute belonging with the command. This is
        done by making the command lower text and prepended ``_command_``. The
        Attribute is then called with the argument :obj:`Node` and message.

        Args:
            s (:obj:`Node`): The :class:`Node` the message was received from.
            message (list): The received message.
        """
        getattr(self, '_command_{}'.format(message[0].lower()))(s, message)

    def _command_ping(self, s, message):
        """Write a message ``PONG`` when a message ``PING`` is received.

        Args:
            s (:obj:`Node`): The :class:`Node` the message was received from.
            message (list): The received message.
        """
        self._write_socket_message(s, 'PONG')

