# Copyright (c) 2011 Richard Boulton
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
r"""ElasticSearch specific errors.

"""
__docformat__ = "restructuredtext en"

def _rebase_pyes_exceptions():
    """Add new base classes for all the elasticsearch exceptions.

    Note - ideally, we would update the base classes for each of the exceptions
    in pyes.exceptions, but we can't do that because python doesn't appear to
    allow changing the base class of objects whose base class is a builtin (eg,
    object or Exception).

    Instead, we dynamically create a subclass of each exception which multiply
    inherits from Exception, and then monkeypatch this into pyes.  This means
    that any existing pyes exception objects won't have the correct base class,
    but this is (hopefully) unlikely to happen in practice.

    """
    import exceptions
    from multisearch.errors import BackendError
    import pyes.exceptions
    for name in pyes.exceptions.__all__:
        pyes_exception = getattr(pyes.exceptions, name, None)
        if pyes_exception is not None:
            newex = type(name, (pyes_exception, BackendError, object), {})
            setattr(pyes.exceptions, name, newex)
            globals()[name] = pyes_exception
_rebase_pyes_exceptions()
del _rebase_pyes_exceptions
