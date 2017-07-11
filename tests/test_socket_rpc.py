import multiprocessing
import unittest
import select
import socket
import uuid
import os

import selectable

class ClientProc(multiprocessing.Process):
    def __init__(self, procname, sock_path):
        self.procname = procname
        self.sock_path = sock_path
        self.client = None
        super(ClientProc, self).__init__()

    def call_me_back(self):
        got = self.client.sync.callback(self.procname)
        self.client.async.got(self.procname, got)
        return 'all good'

    def run(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.sock_path)
        self.client = selectable.RPCSocket(
            sock,
            handlers={'call_me_back': self.call_me_back})
        self.client.async.start(self.procname)
        self.client.execute() # wait for call_me_back
        sock.close()

class Tracker(object):
    def __init__(self):
        self.state = {
            'client1': {
                'started': False,
                'callback': []
            },
            'client2': {
                'started': False,
                'callback': []
            }
        }

    def start(self, client):
        self.state[client]['started'] = True

    def is_started(self, client):
        return self.state[client]['started']

    def callback(self, client):
        self.state[client]['callback'].append('callback')
        return 'my message'

    def got(self, client, msg):
        self.state[client]['callback'].append(('got', msg))

class TestSocketRPC(unittest.TestCase):
    def setUp(self):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket_path = '/tmp/test_socket_rpc_%s' % uuid.uuid4()
        self.socket.bind(self.socket_path)
        self.socket.listen(5)

    def test_basic(self):
        tracker = Tracker()
        proc1 = ClientProc('client1', self.socket_path)
        proc1.daemon = True
        proc1.start()
        conn1, _ = self.socket.accept()
        client1 = selectable.RPCSocket(
            conn1,
            handlers={
                'start': tracker.start,
                'callback': tracker.callback,
                'got': tracker.got
            }
        )
        proc2 = ClientProc('client2', self.socket_path)
        proc2.daemon = True
        proc2.start()
        conn2, _ = self.socket.accept()
        client2 = selectable.RPCSocket(
            conn2,
            handlers={
                'start': tracker.start,
                'callback': tracker.callback,
                'got': tracker.got
            }
        )

        self.assertFalse(tracker.is_started('client1'))
        self.assertFalse(tracker.is_started('client2'))

        client1.execute()
        client2.execute()

        self.assertTrue(tracker.state['client1']['started'])
        self.assertTrue(tracker.state['client2']['started'])

        self.assertEqual(client1.sync.call_me_back(), 'all good')
        self.assertEqual(
            tracker.state['client1']['callback'],
            ['callback', ('got', 'my message')])

        self.assertEqual(client2.sync.call_me_back(), 'all good')
        self.assertEqual(
            tracker.state['client2']['callback'],
            ['callback', ('got', 'my message')])

        proc1.join()
        proc2.join()

        conn1.close()
        conn2.close()

    def tearDown(self):
        try:
            self.socket.close()
        except:
            pass
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)
