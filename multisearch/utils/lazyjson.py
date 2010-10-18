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
r"""A lazily packed and unpacked JSON object.

"""
__docformat__ = "restructuredtext en"

import copy
try:
    import simplejson as json
except ImportError:
    import json

class LazyJsonObject(object):
    """This behaves like a dict and lazily converts contents to and from JSON.

    """
    def __init__(self, json=None, data=None):
        if (json is None or json == '') and data is None:
            data = {}
        self._json = json
        self._data = data

    def _load(self):
        """Load the data from json format, if required"""
        if self._data is None:
            self._data = json.loads(self._json)

    def _dump(self):
        """Dump the data to json format, if required"""
        if self._json is None:
            self._json = json.dumps(self._data, separators=(',', ':'))

    def __getitem__(self, key):
        self._load()
        return self._data[key]

    def __setitem__(self, key, value):
        self._load()
        self._json = None
        self._data[key] = value

    def __delitem__(self, key):
        self._load()
        self._json = None
        del self._data[key]

    @property
    def json(self):
        """Get the data as a JSON string."""
        self._dump()
        return self._json

    def copy_data(self):
        """Get a (deep) copy of the data in the object."""
        self._load()
        return copy.deepcopy(self._data)

    def items(self):
        """Iterate through the items in the object.

        Note - to be really safe we'd apply copy.deepcopy() to each value
        before returning it (the keys are strings, so we don't need to worry
        about them).  However, that's probably a waste of time for almost all
        users, so instead users simply should't modify the values returned by
        this method.

        """
        self._load()
        for kv in self._data.iteritems():
            yield kv
