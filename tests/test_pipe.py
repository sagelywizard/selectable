import unittest
import multiprocessing

import selectable

class CustomObject(object):
    pass

class Proc(multiprocessing.Process):
    def __init__(self, pipe):
        self.pipe = pipe
        super(Proc, self).__init__()

    def run(self):
        self.pipe.use_right()
        msg = None
        while msg != 'shutdown':
            msg = self.pipe.read()
            self.pipe.write(['okay', msg])

class TestPipe(unittest.TestCase):
    def test_multiprocess(self):
        pipe = selectable.Pipe()
        proc = Proc(pipe)
        proc.daemon = True
        proc.start()
        pipe.use_left()
        pipe.write('hello')
        pipe.write(5)
        pipe.write(CustomObject())
        self.assertEqual(pipe.read(), ['okay', 'hello'])
        self.assertEqual(pipe.read(), ['okay', 5])
        [okay, obj] = pipe.read()
        self.assertEqual(okay, 'okay')
        self.assertEqual(type(obj), type(CustomObject()))
        pipe.write('shutdown')
        proc.join()
        self.assertEqual(pipe.read(), ['okay', 'shutdown'])
        self.assertFalse(proc.is_alive())
        pipe.close()
