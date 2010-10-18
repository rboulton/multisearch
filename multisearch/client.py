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

from backends.closed import ClosedBackend
import errors
import queries
import utils

_factories = {}
class SearchClient(object):
    """A client for a search engine.

    """

    def __init__(self, type=None, **kwargs):
        """Initialise a search client of a given type.

        If the type of client requested is not available, raises KeyError.

        """
        # type can be None to allow external code to create a search client
        # with a custom backend.
        if type is not None:
            self.backend = self.get_factory(type)(**kwargs)

    @staticmethod
    def get_factory(type):
        if not utils.is_safe_backend_name(type):
            raise KeyError("Backend type %r not known" % type)
        factory = _factories.get(type, None)
        if factory is None:
            try:
                m = __import__("multisearch.backends.%s_backend" % type,
                                fromlist=['BackendFactory'], level=0)
                factory = m.BackendFactory
            except ImportError, e:
                raise KeyError("Backend type %r not known, or missing dependencies: %s" % (type, e))
            _factories[type] = factory
        return factory

    @property
    def schema(self):
        """Get the schema in use by this client.

        """
        return self.backend.schema

    def close(self):
        """Close any open resources.

        """
        self.backend.close()
        self.backend = ClosedBackend()

    def destroy_database(self):
        """Delete all documents, clear the schema, and reset all state.

        For disk-based backends, this should delete the database entirely from
        the filesystem.

        """
        self.backend.destroy_database()
        self.backend = ClosedBackend()

    def cancel(self):
        """Cancel buffered but unapplied changes.

	The exact semantics of this will vary between backends, and it may not
        be supported at all on some backends, but it should cause document
        updates which have been made, but not yet committed to the index, to be
        discarded.

        """
        return self.backend.cancel()

    def commit(self):
        """Perform any buffered changes.

        The exact semantics of this will vary between backends, but users
        should generally call this after a batch of operations, to ensure that
        changes have been applied such that the modified items can be searched.

        """
        return self.backend.commit()

    def update(self, doc, docid=None, fail_if_exists=False, assume_new=False):
        """Add or update a document.

        `doc` is either:
         - a dictionary, keyed by field name.  The values are lists of field
           values, or single values.
         - or, a sequence of (field name, field value) pairs.

        Field values may be any type, depending on the schema and backend, but
        will usually be strings, integers, floats or datetime objects.

        The order of values within a field is often significant (for example,
        for phrase searches, phrases may be allowed to run from the end of one
        field to the start of another), but the relative order of fields is
        not significant.

        `docid` is a unique identifier used to identify the document.  If this
        is not specified, a new identifier will be allocated automatically.
        The identifier will be a byte string.

        If `fail_if_exists` is set to True, a DocExistsError will be raised if
        a document with the same ID already exists.  Otherwise, any existing
        document with the same document ID will be replaced by this call.

	If `assume_new` is set to True, the backend may choose not to check
	whether the document ID is already in use, and instead assume that it
	is not in use.  Skipping this check allows a small speed improvement.
	However, if this is used incorrectly (ie, when the document ID is
	already in use), this may result in multiple documents with the same
	ID entering the database, which may cause various problems.

        This usually returns the unique identifier used for the document.  With
        some backends, it may also return None if no identifier was supplied
        and an identifier has not yet been allocated at the time this method
        returns.

        """
        return self.backend.update(doc, docid, fail_if_exists, assume_new)

    def delete(self, docid, fail_if_missing=False):
        """Delete a document, given its docid.

        If `fail_if_missing` is True, a DocNotFoundError will be raised if the
        document isn't found.  Otherwise, no action will be taken if the
        document isn't found.

        """
        self.backend.delete(docid, fail_if_missing)

    @property
    def document_count(self):
        """Return the number of documents.

        """
        return self.backend.get_document_count()

    def __len__(self):
        """Return the number of documents.

        """
        return self.backend.get_document_count()

    def iter_documents(self):
        """Iterate through all the documents.

        """
        return self.backend.get_document_iter()

    def __iter__(self):
        """Iterate through all the documents.

        """
        return self.backend.get_document_iter()

    def get_document(self, docid):
        """Get a document, given a document ID.

        Raise KeyError if the document does not exist.

        """
        return self.backend.get_document(docid)

    def document_exists(self, docid):
        """Return True if a document exists, otherwise return False.

        """
        return self.backend.document_exists(docid)

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
        return self.backend.search(search)
