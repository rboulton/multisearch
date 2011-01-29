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

import multisearch.utils
import multisearch.errors
import multisearch.queries

# Change to have a SearchClient, which is a factory method, and a
# SearchClientBase, which backends subclass.  Make all the methods
# which currently proxy into abstract base methods.  

# Also, add a type parameter to the update method, for elastic-search
# style control of the type of the document.  Backends can complain if
# this is non-None if they don't support it.

_factories = {}
def get_factory(type):
    """Get a backend factory.

    Raises ImportError if the backend isn't known or has missing dependencies.

    """
    if not multisearch.utils.is_safe_backend_name(type):
        raise ImportError("Backend type %r not known" % type)
    factory = _factories.get(type, None)
    if factory is None:
        module_name = "multisearch.backends.%s_backend" % type
        m = __import__(module_name, fromlist=['SearchClient'], level=0)
        factory = m.SearchClient
        _factories[type] = factory
    return factory

def SearchClient(type, *args, **kwargs):
    return get_factory(type)(*args, **kwargs)

class BaseSearchClient(object):
    """A base search client interface.

    All methods in this base class will normally be overridden by subclasses,
    but have default implementations to provide appropriate fallback or error
    handling behaviour if subclasses do not override them.  In particular, this
    is intended to allow for easy handling of features which are not available
    or not yet implemented for particular backends: for optional methods, this
    class will raise a FeatureNotAvailableError, whereas for required methods,
    this class will raise a NotImplementedError.

    """

    def close(self):
        """Close any open resources.

        """
        raise NotImplementedError

    @property
    def document_count(self):
        """Return the number of documents.

        """
        raise NotImplementedError

    def __len__(self):
        """Return the number of documents.

        """
        return self.document_count

    def iter_documents(self):
        """Iterate through all the documents.

        """
        raise NotImplementedError

    def __iter__(self):
        """Iterate through all the documents.

        """
        return self.iter_documents()

    def get_document(self, docid):
        """Get a document, given a document ID.

        Raise KeyError if the document does not exist.

        """
        raise NotImplementedError

    def document_exists(self, docid):
        """Return True if a document with the given id exists, False if not.

        """
        raise NotImplementedError

    def query(self, value, fieldname=None, *args, **kwargs):
        """Create a basic search for the value supplied.

        If fieldname is also supplied, search only for the value in that field.

        """
        raise NotImplementedError

    def query_all(self):
        """Create a search which returns all documents.

        """
        return multisearch.queries.QueryAll().connect(self)

    def query_none(self):
        """Create a search which returns no documents.

        """
        return multisearch.queries.QueryNone().connect(self)

    def search(self, search):
        """Perform a search.

        The search should be an instance of multisearch.queries.Search.

        """
        raise NotImplementedError

    def flush(self):
        """Empty any buffered changes; this minimises memory use, but does not
        force changes to be committed (ie, to become visible in searches).

        """
        raise multisearch.errors.FeatureNotAvailableError


    def commit(self):
        """Commit any changes which are currently in progress.

        """
        raise multisearch.errors.FeatureNotAvailableError

    def process(self, doc):
        """Process an incoming document into document suitable for this
        backend.

        """
        raise multisearch.errors.FeatureNotAvailableError

    def update(self, doc, docid=None, fail_if_exists=False, assume_new=False):
        raise multisearch.errors.FeatureNotAvailableError

    def delete(self, docid, fail_if_missing=False):
        raise multisearch.errors.FeatureNotAvailableError

    def destroy_database(self):
        raise multisearch.errors.FeatureNotAvailableError
