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
r"""Base class and factory for clients.

"""
__docformat__ = "restructuredtext en"

import utils

# Change to have a SearchClient, which is a factory method, and a
# SearchClientBase, which backends subclass.  Make all the methods
# which currently proxy into abstract base methods.  

# Also, add a type parameter to the update method, for elastic-search
# style control of the type of the document.  Backends can complain if
# this is non-None if they don't support it.

_factories = {}
def get_factory(type):
    """Get a backend factory.

    Raises KeyError or ImportError if the backend isn't known or has missing
    dependencies.

    """
    if not utils.is_safe_backend_name(type):
        raise KeyError("Backend type %r not known" % type)
    factory = _factories.get(type, None)
    if factory is None:
        module_name = "multisearch.backends.%s_backend" % type
        m = __import__(module_name, fromlist=['BackendFactory'], level=0)
        factory = m.SearchClient
        _factories[type] = factory
    return factory

def SearchClient(type, *args, **kwargs):
    return get_factory(type)(*args, **kwargs)
