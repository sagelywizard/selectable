import unittest
import threading
import multiprocessing

import selectable

class CustomObject(object):
    pass

def make_concurrent_class(parent):
    class Concurrent(parent):
        def __init__(self, pipe):
            self.pipe = pipe
            super(Concurrent, self).__init__()

        def run(self):
            self.pipe.write('hello')
            self.pipe.write(5)
            obj = CustomObject()
            self.pipe.write(obj)

    return Concurrent

class TestUniPipe(unittest.TestCase):
    def test_single_threaded(self):
        pipe = selectable.UniPipe()
        self.assertTrue(pipe.is_open())
        pipe.write('hello')
        pipe.write(5)
        obj = CustomObject()
        pipe.write(obj)
        self.assertEqual(pipe.read(), 'hello')
        self.assertEqual(pipe.read(), 5)
        self.assertEqual(type(pipe.read()), type(obj))
        pipe.close()
        self.assertFalse(pipe.is_open())

    def test_multithreaded(self):
        self.concurrent_test(threading.Thread)


    def test_multiprocess(self):
        self.concurrent_test(multiprocessing.Process)

    def concurrent_test(self, parent):
        pipe = selectable.UniPipe()
        self.assertTrue(pipe.is_open())
        conc_class = make_concurrent_class(parent)
        conc = conc_class(pipe)
        conc.daemon = True
        conc.start()
        self.assertEqual(pipe.read(), 'hello')
        self.assertEqual(pipe.read(), 5)
        self.assertEqual(type(pipe.read()), type(CustomObject()))
        conc.join()
        self.assertFalse(conc.is_alive())
        pipe.close()
        self.assertFalse(pipe.is_open())

