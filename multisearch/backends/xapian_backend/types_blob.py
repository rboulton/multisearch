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
r"""Blob field type.

"""
__docformat__ = "restructuredtext en"

from multisearch.backends.xapian_backend.xquery import XapianQuery
import xapian

class XapianBlobIndexer(object):
    """Indexer for a blob field.

    Accepts the following parameters:

     - store: boolean.  If True, store the field values in the document data.
     - slot: The slot number to use.
     - prefix: string.  The prefix to insert before terms.  Should follow
       Xapian conventions (ie, be composed of upper case ascii characters, and
       start with X if more than one character long).
     - weight: integer (>= 0).  The weight bias to use for this field.

    """
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        self.store = bool(params.get('store', True))
        slot = params.get('slot', None)
        if slot is not None:
            slot = int(slot)
            assert slot >= 0
        self.slot = slot
        self.prefix = str(params.get('prefix', ''))
        self.weight = int(params.get('weight', 1))
        assert self.weight >= 0

    def new_doc(self, xdoc):
        self.xdoc = xdoc

    def __call__(self, stored, values, route_params, state):
        if self.store:
            s = stored.get(self.fieldname, None)
            if s is None:
                stored[self.fieldname] = s = []
        else:
            s = None

        if isinstance(values, basestring):
            values = (values, )
        for value in values:
            if len(value) == 0:
                continue
            if self.slot is not None:
                self.xdoc.add_value(self.slot, value)
            if s is not None:
                s.append(value)
            if value[0].isupper():
                value = ':' + value
            self.xdoc.add_term(self.prefix + value, self.weight)

class XapianBlobQueryGenerator(object):
    def __init__(self, fieldname, params):
        self.prefix = str(params.get('prefix', ''))

    def __call__(self, client, value):
        if not value:
            return XapianQuery(xapian.Query())
        if value[0].isupper():
            value = ':' + value
        return XapianQuery(xapian.Query(self.prefix + value))
