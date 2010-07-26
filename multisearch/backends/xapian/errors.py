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
r"""Xapian specific errors.

"""
__docformat__ = "restructuredtext en"

def _rebase_xapian_exceptions():
    """Add new base classes for all the xapian exceptions.

    """
    from multisearch.errors import BackendError
    import xapian
    for name in (
                 'AssertionError',
                 'DatabaseCorruptError',
                 'DatabaseCreateError',
                 'DatabaseError',
                 'DatabaseLockError',
                 'DatabaseModifiedError',
                 'DatabaseOpeningError',
                 'DatabaseVersionError',
                 'DocNotFoundError',
                 # We skip 'Error' because it inherits directly from exception
                 # and this causes problems with method resolution order.
                 # However, we probably don't need it anyway, because it's
                 # just a base class, and shouldn't ever actually be raised.
                 # Users can catch multisearch.BackendError instead.
                 'FeatureUnavailableError',
                 'InternalError',
                 'InvalidArgumentError',
                 'InvalidOperationError',
                 'LogicError',
                 'NetworkError',
                 'NetworkTimeoutError',
                 'QueryParserError',
                 'RangeError',
                 'RuntimeError',
                 'UnimplementedError',
                 ):
        xapian_exception = getattr(xapian, name, None)
        if xapian_exception is not None:
            xapian_exception.__bases__ += (BackendError, )
            globals()['Xapian' + name] = xapian_exception
_rebase_xapian_exceptions()
del _rebase_xapian_exceptions
