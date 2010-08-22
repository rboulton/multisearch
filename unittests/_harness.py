# Copyright (c) 2010 Richard Boulton
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
r"""Support functions for the tests.

"""
__docformat__ = "restructuredtext en"

import multisearch
import os
import shutil
import tempfile
import unittest

# Add each new backend here.
all_backends = ('xapian', )

def with_backends(*args):
    """Decorator for calling a test with multiple different backends.

    """
    def call_for_backends(backends, fn, self):
        for backend in backends:
            try:
                fn(self, backend)
            except:
                print "Failure with backend %s" % backend
                # FIXME - report the backend more nicely - perhaps insert it
                # into the stack trace somehow.
                raise
    if len(args) == 1 and not isinstance(args[0], basestring):
        # Called as a decorator without arguments - just return the wrapped function.
        def do(self):
            call_for_backends(all_backends, args[0], self)
        return do

    backends = args
    if not backends:
        backends = all_backends
    def deco(fn):
        def do(self):
            call_for_backends(backends, fn, self)
        return do
    return deco

class MultiSearchTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="multisearchtest")

    def tearDown(self):
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)
