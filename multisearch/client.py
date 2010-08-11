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

from backends.closed_backend import ClosedBackend
import errors
import queries

class Term(object):
    """A term returned from a list of terms.

    This object can have various properties, although the exact set available
    will depend on the backend, and on the list of term which the object came
    from.  The `raw` property is the only property which must always be
    present.

     - `raw`: The raw value of the term (for most backends, this will be a
       string).  The raw value should suffice to identify a term in the
       database.
     - `value`: The value of the term.
     - `field`: The field that the term is associated with (this may not always
       be present - and, indeed, may not always be meaningful, since a term may
       be a composite piece of information from several fields).
     - `wdf`: The "within document frequency" of the term.  This is the
       number of times that the term occurred in the document.
     - `docfreq`: The "document frequency" of the term.  This is the total
       number of documents that the term occurred in across all the documents
       in the collection.
     - `collfreq`: The "collection frequency" of the term.  This is the total
       number of times that the term occurred across all the documents in the
       collection.

    """
    pass

class Document(object):
    """A document returned from the search engine.

    """
    @property
    def docid(self):
        """The document ID for the document.

        """
        return self.get_docid()

    @property
    def data(self):
        """The data stored in the document.
        
        This consists of all field values which were passed in which the schema
        caused to be stored.  The values are returned as a sequence of
        (fieldname, value) pairs, in the same order as indexed.

        """
        return self.get_data()

    @property
    def terms(self):
        """The terms stored in the document.

        This consists of an iterator over, or sequence of, term objects generated from
        field values by the actions in the schema.  items in the

        """
        return self.get_terms()

    def __str__(self):
        return "Document(docid=%r)" % self.docid

    def __repr__(self):
        return "<multisearch.Document(docid=%r)>" % self.docid

_factories = {}
class SearchClient(object):
    """A client for a search engine.

    """

    def __init__(self, type=None, *args, **kwargs):
        """Initialise a search client of a given type.

        If the type of client requested is not available, raises KeyError.

        """
        # type can be None to allow external code to create a search client
        # with a custom backend.
        if type is not None:
            self.backend = self._get_factory(type)(*args, **kwargs)

    @staticmethod
    def _get_factory(type):
        factory = _factories.get(type, None)
        if factory is None:
            if type == 'redis':
                from backends.redis_backend import RedisBackend
                factory = RedisBackend
            elif type == 'xapian':
                from backends.xapian_backend import XapianBackendFactory
                factory = XapianBackendFactory
            else:
                raise KeyError("Backend type %r not known" % type)
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

    def commit(self):
        """Perform any buffered changes.

        The exact semantics of this will vary between backends, but users
        should generally call this after a batch of operations, to ensure that
        changes have been applied such that the modified items can be searched.

        """
        return self.backend.commit()

    def update(self, doc, docid=None, fail_if_exists=False):
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
        usually not significant.  Whether order is significant in either case
        depends on the processing assigned to the fields in question by the
        schema; if the relative order of fields is important, provide `doc` as
        a sequence.

        `docid` is a unique identifier used to identify the document.  If this
        is not specified, a new identifier will be allocated automatically.
        The identifier will be a byte string.

        If `fail_if_exists` is set to True, a DocExistsError will be raised if
        a document with the same ID already exists.  Otherwise, any existing
        document with the same document ID will be replaced by this call.

        This usually returns the unique identifier used for the document.  With
        some backends, it may also return None if no identifier was supplied
        and an identifier has not yet been allocated at the time this method
        returns.

        """
        if isinstance(doc, dict):
            flatdoc = []
            for fieldname, values in doc.iteritems():
                if isinstance(values, basestring):
                    flatdoc.append((fieldname, values))
                elif hasattr(values, '__iter__'):
                    flatdoc.extend(((fieldname, value) for value in values))
                else:
                    flatdoc.append((fieldname, values))
            doc = flatdoc
        return self.backend.update(doc, docid, fail_if_exists)

    def delete(self, docid, fail_if_missing=False):
        """Delete a document, given its docid.

        If `fail_if_missing` is True, a DocNotFoundError will be raised if the
        document isn't found.  Otherwise, no action will be taken if the
        document isn't found.

        """
        self.backend.delete(docid, fail_if_missing)

    def destroy_database(self):
        """Delete all documents, clear the schema, and reset all state.

        For disk-based backends, this should delete the database entirely from
        the filesystem.

        """
        self.backend.destroy_database()
        self.backend = ClosedBackend()

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
