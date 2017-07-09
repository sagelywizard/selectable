import os
import pickle

class UniPipe(object):
    """A selectable unidirectional pipe, usable between processes.

    All data written/read is pickled/unpickled.
    """
    def __init__(self):
        read_pipe, write_pipe = os.pipe()
        self.read_pipe = os.fdopen(read_pipe, 'rb')
        self.write_pipe = os.fdopen(write_pipe, 'wb')
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
