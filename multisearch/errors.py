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
r"""Exception classes.

"""
__docformat__ = "restructuredtext en"

class SearchClientError(Exception):
    """Base class of errors produced by SearchClient.

    """
    pass

class DocExistsError(SearchClientError):
    """A document already existed when it wasn't expected to.

    The document ID can be accessed as the `docid` property.

    """
    def __init__(self, docid):
        self.docid = docid
        self.message = "Document %r already existed" % docid

class DocNotFoundError(SearchClientError):
    """A document specified by an id wasn't found.

    The document ID can be accessed as the `docid` property.

    """
    def __init__(self, docid):
        self.docid = docid
        self.message = "Document %r not found" % docid

class DbClosedError(SearchClientError):
    """An operation was attempted on a closed database.

    """
    pass

class DbReadOnlyError(SearchClientError):
    """A write operation was attempted on a readonly database.

    """
    pass

class UnknownQueryTypeError(SearchClientError):
    """An unknown query type was passed to a backend.

    """
    pass

class BackendError(SearchClientError):
    """An error produced by a backend.

    This wraps an exception raised by a particular search backend.

    The original exception can be accessed as the "original" property.

    """
    def __init__(self, original):
        self.original = original
