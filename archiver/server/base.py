import pickle
import select
import socket
import struct

from archiver.config import *


class Node:
    def __init__(self, s, listener=None):
        self.socket = s
        self.listener = listener

    def __getattr__(self, attr):
        return getattr(self.socket, attr)


class BaseServer:
    def __init__(self, ip=None, port=None):
        self._address = (ip or socket.getfqdn(), port or DATA_PORT)
        self._read_list = []
        self._write_list = []
        self._error_list = []
        self._write_queue = {}
        self._last_stager_request = 0
        self._last_ping = 0

        print(self._address)
        self._socket = Node(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                            self._address)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(('0.0.0.0', self._address[1]))
        self._socket.listen(LISTEN_QUEUE)
        self._read_list.append(self._socket)

    def run(self):
        while True:
            self._run_round()

    def _run_round(self):
        read_ready, write_ready, error_ready = select.select(
            self._read_list, self._write_list, self._error_list, 1)
        for s in read_ready:
            self._read_socket(s)
        for s in write_ready:
            self._write_socket(s)
        #self._handle_error(error_ready)

    def _connect_socket(self, address):
        s = Node(socket.socket(socket.AF_INET, socket.SOCK_STREAM), address)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(tuple(address))
        return s

    def _read_socket(self, s):
        message_length = s.recv(8)
        if len(message_length) == 0:
            return
        message_length = struct.unpack('L', message_length)[0]
        message = b''
        while len(message) < message_length:
            message += s.recv(message_length - len(message))
        message = pickle.loads(message)
        print('received', [m[:30] if type(m) is bytes else m for m in message],
              'from', s.getpeername())
        self._process_message(s, message)

    def _write_socket(self, s):
        while len(self._write_queue[s]) > 0:
            message = self._write_queue[s].pop(0)
            print('sending', [m[:30] if type(m) is bytes else m for m in message],
                  'to', s.getpeername())
            message = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
            s.sendall(struct.pack('L', len(message)) + message)
        self._read_list.append(s)
        self._write_list.remove(s)

    def _write_socket_message(self, s, *message):
        if isinstance(s, socket.socket):
            s = Node(s)
        if type(s) in [list, set, dict]:
            for s_ in s:
                self._write_socket_message(s_, *message)
            return None
        self._write_queue[s].append(message)
        if s in self._read_list:
            self._read_list.remove(s)
        if s not in self._write_list:
            self._write_list.append(s)

    def _write_socket_file(self, s, filename, *message):
        with open(filename, 'rb') as f:
            self._write_socket_message(s, message[0], filename, 'FILE',
                                       f.read(), *message[1:])

    def _process_message(self, s, message):
        getattr(self, '_command_{}'.format(message[0].lower()))(s, message)

    def _command_ping(self, s, message):
        self._write_socket_message(s, 'PONG')

