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
r"""Floating point number field type.

"""
__docformat__ = "restructuredtext en"

from multisearch.backends.xapian_backend.xquery import XapianQuery
import multisearch.queries
import xapian

class XapianFloatIndexer(object):
    """Indexer for a float field.

    Accepts the following parameters:

     - store: boolean.  If True, store the field values in the document data.
     - slot: The slot number to use.

    """
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        self.store = bool(params.get('store', True))
        self.slot = int(params.get('slot', 0))

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
            self.xdoc.add_value(self.slot, xapian.sortable_serialise(float(value)))
            if s is not None:
                s.append(value)

class XapianFloatQueryGenerator(object):
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        self.slot = int(params.get('slot', 0))

    def __call__(self, client, start, end):
        start = xapian.sortable_serialise(start)
        end = xapian.sortable_serialise(end)
        return XapianQuery(xapian.Query.OP_RANGE(self.slot, start, end))
