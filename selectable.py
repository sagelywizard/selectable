import os
import pickle
import struct
import functools

class UniPipe(object):
    """A selectable unidirectional pipe, usable between processes.

    All data written/read is pickled/unpickled.
    """
    def __init__(self):
        read_pipe, write_pipe = os.pipe()
        self.read_pipe = os.fdopen(read_pipe, 'rb', 0)
        self.write_pipe = os.fdopen(write_pipe, 'wb', 0)
        self.__is_open = True

    def __del__(self):
        self.close()

    def is_open(self):
        return self.__is_open

    def writable(self):
        """Sets if the pipe should be readable or writable."""
        self.read_pipe.close()

    def readable(self):
        """Sets if the pipe should be readable or writable."""
        self.write_pipe.close()

    def read(self):
        try:
            return pickle.load(self.read_pipe)
        except EOFError:
            self.read_pipe.close()
            self.__is_open = False

    def write(self, data):
        pickle.dump(data, self.write_pipe)
        self.write_pipe.flush()

    def fileno(self):
        """Required for doing a select. See select.select documentation."""
        return self.read_pipe.fileno()

    def close(self):
        self.__is_open = False
        self.read_pipe.close()
        self.write_pipe.close()

class Pipe(object):
    """
    A selectable pipe, usable between processes.

    > import os, select, selectable
    > pipe1 = selectable.Pipe()
    > pipe2 = selectable.Pipe()
    > pid = os.fork()
    > if pid == 0:
    >     pipe1.use_left()
    >     for i in range(10):
    >         pipe1.write('pipe1: %s' % i)
    > else:
    >     pid = os.fork()
    >     if pid == 0:
    >         pipe2.use_left()
    >         for i in range(10):
    >             pipe2.write('pipe2: %s' % i)
    >     else:
    >         pipe1.use_right()
    >         pipe2.use_right()
    >         (readable, _, _) = select.select([pipe1, pipe2], [], [])
    >         for pipe in readable:
    >             print(pipe.read())
    """
    def __init__(self):
        self.left = UniPipe()
        self.right = UniPipe()
        self.write_pipe = None
        self.read_pipe = None

    def __del__(self):
        self.close()

    def use_right(self):
        """Claim the right channel. Must be called post-fork."""
        self.right.writable()
        self.left.readable()
        self.write_pipe = self.right
        self.read_pipe = self.left

    def use_left(self):
        """Claim the left channel. Must be called post-fork."""
        self.left.writable()
        self.right.readable()
        self.write_pipe = self.left
        self.read_pipe = self.right

    def write(self, data):
        self.write_pipe.write(data)

    def read(self):
        return self.read_pipe.read()

    def fileno(self):
        """Required for select. See select.select documentation."""
        return self.read_pipe.fileno()

    def close(self):
        self.left.close()
        self.right.close()

class RPCFuncHandler(object):
    def __init__(self, fun):
        self.fun = fun

    def __getattr__(self, func_name):
        return functools.partial(self.fun, func_name)

class RPCClient(object):
    def __init__(self, client_obj, handlers=None):
        self.client_obj = client_obj
        self.rpc = RPCHandler(client_obj)
        if handlers is None:
            self.handlers = {}
        else:
            self.handlers = handlers
        self.sync = RPCFuncHandler(self._sync)
        self.async = RPCFuncHandler(self._async)

    def add_handler(self, name, func):
        self.handlers[name] = func

    def execute(self):
        msg_type, func_name, args, kwargs = self.read()
        assert msg_type in ['syncrequest', 'asyncrequest']
        resp = self.handlers[func_name](*args, **kwargs)
        if msg_type == 'syncrequest':
            self.write(('response', resp))

    def _sync(self, func_name, *args, **kwargs):
        self.write(('syncrequest', func_name, args, kwargs))
        msg = self.read()
        while msg[0] in ['syncrequest', 'asyncrequest']:
            _, func_name, args, kwargs = msg
            resp = self.handlers[func_name](*args, **kwargs)
            if msg[0] == 'syncrequest':
                self.write(('response', resp))
            msg = self.read()

        if msg[0] == 'error':
            if msg[1] == 'UnknownRPCError':
                raise UnknownRPCError
            raise RPCError(msg[1])

        return msg[1]

    def _async(self, func_name, *args, **kwargs):
        self.write(('asyncrequest', func_name, args, kwargs))

    def fileno(self):
        return self.client_obj.fileno()

    def close(self):
        self.client_obj.close()

    def write(self, msg):
        raise NotImplementedError

    def read(self):
        raise NotImplementedError

class RPCPipe(RPCClient):
    def __init__(self, pipe):
        self.pipe = pipe
        super(RPCPipe, self).__init__(pipe)

    def write(self, msg):
        self.pipe.write(msg)

    def read(self):
        return self.pipe.read()

class RPCSocket(RPCClient):
    def __init__(self, sock, handlers=None):
        if handlers is None:
            handlers = {}
        self.sock = sock
        self.buff = b''
        super(RPCSocket, self).__init__(self.sock, handlers=handlers)

    def write(self, msg):
        binary = pickle.dumps(msg)
        header = struct.pack('!L', len(binary))
        self.sock.sendall(b'%b%b' % (header, binary))

    def read(self):
        while len(self.buff) < 4:
            read = self.sock.recv(1024)
            if read == b'':
                return None
            self.buff += read

        (length,) = struct.unpack('!L', self.buff[0:4])

        while len(self.buff) < (length+4):
            read = self.sock.recv(1024)
            if read == b'':
                return None
            self.buff += read

        pick = self.buff[4:length+4]
        self.buff = self.buff[length+4:]
        return pickle.loads(pick)

class UnknownRPCError(Exception):
    pass

class RPCError(Exception):
    pass

class RPCHandler(object):
    def __init__(self, client_obj):
        self.client_obj = client_obj

    def __getattr__(self, func):
        return RPCCaller(self.client_obj, func)

class RPCCaller(object):
    def __init__(self, client_obj, func_name):
        self.client_obj = client_obj
        self.func_name = func_name

    def __call__(self, *args, **kwargs):
        self.client_obj.write((self.func_name, args, kwargs))
        (msg_type, msg) = self.client_obj.read()
        assert msg_type in ['return', 'error']
        if msg_type == 'error':
            if msg == 'UnknownRPCError':
                raise UnknownRPCError
            raise RPCError(msg)

        return msg
