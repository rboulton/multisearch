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

import queries
import schema

_factories = {}

def SearchClient(type=None, *args, **kwargs):
    """Create a client of a given type.

    If the type of client requested is not available, raises KeyError.

    """
    factory = _factories.get(type, None)
    if factory is None:
        if type == 'redis':
            from backends.redis import RedisSearchClient
            factory = RedisSearchClient
        elif type == 'xapian':
            from backends.xapian import XapianSearchClientFactory
            factory = XapianSearchClientFactory
        else:
            raise KeyError("SearchClient type %r not known" % type)
        _factories[type] = factory
    return factory(*args, **kwargs)

class BaseSearchClient(object):
    def __init__(self):
        """Initialise a search client.

        """
        self.schema = schema.Schema()

    def close(self):
        """Close any open resources.

        """
        raise NotImplementedError

    def commit(self):
        """Perform any buffered changes.

        The exact semantics of this will vary between backends, but users
        should generally call this after a batch of operations, to ensure that 

        """
        raise NotImplementedError

    def update(self, doc, docid=None, fail_if_exists=False):
        """Add or update a document.

        `doc` is either:
         - a dictionary, keyed by field name.  The values are lists of field
           values.
         - or, a sequence of (field name, field value) pairs.

        Field values may be any type, depending on the schema and backend, but
        will usually be strings, integers, floats or datetime objects.

        The order of values within a field is often significant (for example,
        for phrase searches, phrases may be allowed to run from the end of one
        field to the start of another), but the relative order of fields is
        usually not significant.  Whether order is significant in either case
        depends on the processing assigned to the fields in question by the
        schema; if the relative order of fields is important, provide `doc` as
        a sequence.

        `docid` is a unique identifier used to identify the document.  If this
        is not specified, a new identifier will be allocated automatically.
        The identifier will be a byte string.

        If `fail_if_exists` is set to True, a DocExistsError will be raised if
        the document already exists.  Otherwise, any existing document with the
        same document ID will be replaced by this call.

        Returns the unique identifier used for the document.

        """
        raise NotImplementedError

    def delete(self, docid, fail_if_missing=False):
        """Delete a document, given its docid.

        If `fail_if_missing` is True, a DocNotFoundError will be raised if the
        document isn't found.  Otherwise, no action will be taken if the
        document isn't found.

        """
        raise NotImplementedError

    def full_reset(self):
        """Delete all documents, clear the schema, and reset all state.

        """
        raise NotImplementedError

    def document_count(self):
        """Return the number of documents.

        """
        raise NotImplementedError

    def iter_documents(self):
        """Iterate through all the document IDs.

        """
        raise NotImplementedError

    def query(self, fieldname, value, *args, **kwargs):
        """Build a basic query for the contents of a named field.

        """
        qg = self.schema.query_generator(fieldname)
        return qg(value, *args, **kwargs).connect(self)

    def search(self, search):
        """Perform a search.

        The search should be an instance of multisearch.queries.Search.  This
        method is usually called by the __call__ method of such an instance.

        """
        raise NotImplementedError
