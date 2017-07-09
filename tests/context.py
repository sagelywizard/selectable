"""
This module makes it possible for tests to import selectable without causing
problems for pylint.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import selectable # pylint: disable=import-error,wrong-import-position,unused-import
