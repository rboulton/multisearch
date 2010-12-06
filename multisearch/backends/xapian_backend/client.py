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
r"""Xapian backend.

"""
__docformat__ = "restructuredtext en"

from multisearch.utils.closed import ClosedObject
import multisearch.backends.xapian_backend.errors
import multisearch.client
import multisearch.errors
import multisearch.queries
from multisearch.schema import Schema
from multisearch import utils
import xapian

def SearchClient(path=None, readonly=False, **kwargs):
    """Factory for XapianBackends.

    """
    if path is None:
        raise multisearch.errors.BackendError("Missing path argument")
    if readonly:
        return ReadonlySearchClient(path)
    else:
        return WritableSearchClient(path)

class XapianDocument(multisearch.Document):
    def __init__(self, raw):
        """Create a XapianDocument.

        `raw` is the Xapian Document object wrapped by this.

        """
        self.raw = raw
        self._data = None

    def get_docid(self):
        """Get the document's id.

        """
        tl = self.raw.termlist()
        try:
            term = tl.skip_to("!").term
            if len(term) == 0 or term[0] != '!':
                return None
        except StopIteration:
            return None
        return term[1:]

    def get_data(self):
        """Get the data stored in the document.

        """
        if self._data is None:
 	    self._data = utils.LazyJsonObject(json=self.raw.get_data())
        return self._data.copy_data()

    def append_to_field(self, fieldname, value):
        """Append a value to a field in the data stored in the document.

        """
        if self._data is None:
 	    self._data = utils.LazyJsonObject(json=self.raw.get_data())
        fdata = self._data.get(fieldname, [])
        fdata.append(value)
        self._data[fieldname] = fdata

    def get_terms(self):
        FIXME

class DocumentIter(object):
    """Iterate through a set of documents.

    """
    def __init__(self, db, iter):
        """Initialise the prefixed term iterator.

        - `db` is the database.
        - `iter` is the iterator to use, which should be at its start.

        """
        self.db = db
        self.iter = iter

    def __iter__(self):
        return self

    def next(self):
        """Get the next document.

        """
        posting = self.iter.next()
        return XapianDocument(self.db.get_document(posting.docid))

class Results(object):
    def __init__(self, backend, mset):
        self.backend = backend
        self.mset = mset

    def __iter__(self):
        return DocumentIter(self.backend.db, iter(self.mset))

    def __len__(self):
        return len(self.mset)

class BaseSearchClient(object):
    """Base SearchClient class for Xapian.

    """
    def __init__(self):
        serialised_schema = self.db.get_metadata("__ms:schema")
        self._schema = Schema.unserialise(serialised_schema)

    @property
    def schema(self):
        """Get the schema in use by this client.

        """
        return self._schema

    def close(self):
        """Close any open resources.

        """
        self.db.close()
        self.db = ClosedObject()

    @property
    def document_count(self):
        """Return the number of documents.

        """
        return self.db.get_doccount()

    def __len__(self):
        """Return the number of documents.

        """
        return self.db.get_doccount()

    def iter_documents(self):
        """Iterate through all the documents.

        """
        return DocumentIter(self.db, self.db.postlist(''))

    def __iter__(self):
        """Iterate through all the documents.

        """
        return DocumentIter(self.db, self.db.postlist(''))

    @staticmethod
    def get_docid_term(docid):
        """Get the term used to reference a given document ID.
        """
        return "!%s" % docid

    def get_document(self, docid):
        """Get a document, given a document ID.

        Raise KeyError if the document does not exist.

        """
        docidterm = self.get_docid_term(docid)
        while True:
            try:
                postlist = self.db.postlist(docidterm)
                try:
                    plitem = postlist.next()
                except StopIteration:
                    raise KeyError("Unique ID %r not found" % docid)
                return XapianDocument(self.db.get_document(plitem.docid))
            except xapian.DatabaseModifiedError, e:
                self.db.reopen()

    def document_exists(self, docid):
        return self.db.term_exists(self.get_docid_term(docid))

    def query(self, fieldname, value, *args, **kwargs):
        qg = self.schema.query_generator(fieldname)
        return qg(value, *args, **kwargs).connect(self)

    def compile(self, query):
        """Make a xapian Query from a query tree.

        """
        if isinstance(query, multisearch.queries.QueryCombination):
            subqs = [self.compile(subq) for subq in query.subqs]
            try:
                op = {
                    multisearch.queries.Query.OR: xapian.Query.OP_OR,
                    multisearch.queries.Query.AND: xapian.Query.OP_AND,
                    multisearch.queries.Query.XOR: xapian.Query.OP_XOR,
                    multisearch.queries.Query.NOT: xapian.Query.OP_AND_NOT,
                }[query.op]
            except KeyError:
                raise multisearch.errors.UnknownQueryTypeError(
                    "Query operator unknown (%s)" % query.op)
            return xapian.Query(op, subqs)
        elif isinstance(query, multisearch.queries.QueryMultWeight):
            return xapian.Query(xapian.Query.OP_MULT_WEIGHT,
                                self.compile(query.subq))
        elif isinstance(query, multisearch.queries.QueryAll):
            return xapian.Query("")
        elif isinstance(query, multisearch.queries.QueryNone):
            return xapian.Query()
        elif isinstance(query, multisearch.queries.QueryTerms):
            return xapian.Query(query.default_op,
                                [xapian.Query(term) for term in query.terms])
        elif isinstance(query, multisearch.queries.QuerySimilar):
            FIXME # implement
            return xapian
        else:
            raise multisearch.errors.UnknownQueryTypeError(
                "Query %s of unknown type" % query)

    def search(self, search):
        """Perform a search.

        The search should be an instance of multisearch.queries.Search.

        """
        xq = self.compile(search.query)
        enq = xapian.Enquire(self.db)
        enq.set_query(xq)
        mset = enq.get_mset(search.start_rank,
                            search.end_rank - search.start_rank)
        return Results(self, mset)

class ReadonlySearchClient(BaseSearchClient):
    """A readonly Xapian SearchClient.

    """
    def __init__(self, path):
        self.db = xapian.Database(path)
        self.path = path
        BaseSearchClient.__init__(self)
        self.schema.modifiable = False

class WritableSearchClient(BaseSearchClient):
    """A writable Xapian SearchClient.

    """
    def __init__(self, path):
        self.db = xapian.WritableDatabase(path, xapian.DB_CREATE_OR_OPEN)
        self.path = path
        BaseSearchClient.__init__(self)

    def commit(self):
        if self.schema.modified:
            self.db.set_metadata("__ms:schema", self.schema.serialise())
            self.schema.modified = False
        self.db.commit()

    def process(self, doc):
        """Process an incoming document into a Xapian document.

        """
        xdoc = XapianDocument(xapian.Document())

        stored = {}
        #FIXME - should be a standard way for indexers to add to the document

        for fieldname, value in utils.iter_doc_fields(doc):
            self.schema.guess(fieldname, value)
            idx = self.schema.indexer(fieldname)
            for term in idx(value):
                xdoc.raw.add_term(term)
            xdoc.append_to_field(fieldname, value)

        xdoc.raw.set_data(xdoc._data.json)

        return xdoc.raw

    def update(self, doc, docid=None, fail_if_exists=False, assume_new=False):
        if docid is None:
            while True:
                docid = utils.make_docid()
                docidterm = self.get_docid_term(docid)
                if not self.db.term_exists(docidterm):
                    break
        else:
            docid = str(docid)
            docidterm = self.get_docid_term(docid)
            if fail_if_exists and not self.db.term_exists(docidterm):
                raise multisearch.errors.DocExistsError(
                    "Document with ID %r already exists" % docid)

        if isinstance(doc, xapian.Document):
            xdoc = doc
        else:
            xdoc = self.process(doc)
        xdoc.add_term(docidterm)
        self.db.replace_document(docidterm, xdoc)
        return docid

    def delete(self, docid, fail_if_missing=False):
        docidterm = self.get_docid_term(docid)
        if fail_if_missing and not self.db.term_exists(docidterm):
            raise multisearch.errors.DocNotFoundError(
                "No document with ID %r found when deleting" % docid)
        self.db.delete_document(docidterm)

    def destroy_database(self):
        self.db.close()
        shutil.rmtree(self.path)
