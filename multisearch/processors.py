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
r"""Generic text and value processing code.

"""
__docformat__ = "restructuredtext en"

import queries

class BaseIndexer(object):
    """Base class of generic indexers.
    
    Subclasses should implement the __call__ method, taking a one parameter of
    a value in the field being indexed, and returning or yielding a sequence of
    terms to be indexed.

    """
    def __init__(self, fieldname, params):
        """Default initialiser - just stores the fieldname and parameters.

        """
        self.fieldname = fieldname
        self.params = params

class BaseQueryGenerator(object):
    """Base class of query generators.

    Subclasses should implement the __call__ method, taking one parameter of
    a value to search for in the field being indexed, and returning a Query
    subclass.

    """
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        self.params = params

class TextIndexer(BaseIndexer):
    def __call__(self, value):
        for term in value.split():
            yield self.fieldname + ':' + term

class TextQueryGenerator(BaseQueryGenerator):
    def __call__(self, value, default_op=None):
        terms = []
        for term in value.split():
            terms.append(self.fieldname + ':' + term)
        return queries.QueryTerms(terms, default_op=default_op)

class BlobIndexer(BaseIndexer):
    def __call__(self, value):
        yield self.fieldname + ':' + value

class BlobQueryGenerator(BaseQueryGenerator):
    def __call__(self, value):
        yield self.fieldname + ':' + value
    def __call__(self, value, *args, **kwargs):
        return queries.QueryTerms([self.fieldname + ':' + value],
                                  default_op=default_op)
