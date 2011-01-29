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
r"""Wrapper around a xapian query to make it behave nicely.

"""
__docformat__ = "restructuredtext en"

import multisearch.queries

class XapianQuery(multisearch.queries.Query):
    def __init__(self, xapq):
        super(XapianQuery, self).__init__()
        self.xapq = xapq
        self.method = None
        self.args = ()
        self.kwargs = {}

    def _set_params(self, method, args, kwargs):
        self.method = method
        self.args = args
        self.kwargs = kwargs

    def __unicode__(self):
        if self.method is None:
            return u'XapianQuery(%s)' % (self.xapq,)
        else:
            return u'XapianQuery(%s, %r, %r, %r)' % (self.xapq, self.method,
                                                     self.args, self.kwargs)

    def __repr__(self):
        if self.method is None:
            return u'<XapianQuery(%s)>' % (self.xapq,)
        else:
            return u'<XapianQuery(%s, %r, %r, %r)>' % (self.xapq, self.method,
                                                       self.args, self.kwargs)

    def __str__(self):
        return u'<%s>' % (self.xapq, )
