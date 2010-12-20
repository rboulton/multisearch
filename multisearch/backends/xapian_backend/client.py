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
from multisearch.utils.jsonschema import JsonSchema
from multisearch import utils
import xapian

class Schema(JsonSchema):
    types = {}

    @classmethod
    def register_type(cls, name, doc, indexer, querygen):
        cls.types[name] = (doc, indexer, querygen)

    def prefix_from_fieldname(self, fieldname):
        return 'X' + ''.join(c.upper() for c in fieldname if c.isalnum())

    def guess(self, fieldname, value):
        """Guess the route, type and parameters for a field, given its value.

        If the field is not known already, this guesses what route, field type
        and parameters would be appropriate, and sets the schema accordingly.


        """
        if fieldname in self.routes or fieldname in self.types:
            return
        self.set_route(fieldname, ("", fieldname))
        self.set(fieldname, "TEXT", {
                 'prefix': self.prefix_from_fieldname(fieldname),
                 })
        self.set('', "TEXT", {'prefix': ''})

    def make_indexer(self, type, fieldname, params):
        """Get the indexer for a field.

        Raises KeyError if the field is not in the schema.

        """
        return self.types[type][1](fieldname, params)

    def make_query_generator(self, fieldname):
        """Get a query generator for a field.

        Raises KeyError if the field is not in the schema.

        """
        return self.types[type][2](fieldname, params)

class XapianTextIndexer(object):
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        self.tg = TermGenerator()

        self.prefix = str(params.get('prefix', ''))
        self.weight = int(params.get('weight', 1))
        if params.get('positions', False):
            self.idx_method = self.tg.index_text
        else:
            self.idx_method = self.tg.index_text_without_positions

    def new_doc(self, xdoc):
        self.tg.set_document(xdoc)
        self.tg.set_termpos(0)

    def __call__(self, stored, value, route_params, state):

        if isinstance(values, basestring):
            values = (values, )
        for value in values:
            self.idx_method(value, self.weight, self.prefix)

class XapianTextQueryGenerator(object):
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        

Schema.register_type("TEXT",
                     """Free text - words, to be parsed.""",
                     XapianTextIndexer,
                     XapianTextQueryGenerator)

Schema.register_type("BLOB",
                     """A literal string of bytes, to be matched exactly.""",
                     XapianBlobIndexer,
                     XapianBlobQueryGenerator)


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
    def __init__(self, raw, idprefix):
        """Create a XapianDocument.

        `raw` is the Xapian Document object wrapped by this.

        """
        self.raw = raw

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

    def set_docid(self, newid):
        oldid = self.get_docid()
        if oldid is not None:
            self.raw.remove_term('!%s' % oldid)

        if newid is not None:
            self.raw.add_term('!%s' % newid, 0)

    def get_data(self):
        """Get the data stored in the document.

        """
        return utils.json.loads(self.raw.get_data())

    def set_data(self, data):
        """Set the data stored in the document.

        """
        self.raw.set_data(utils.json.dumps(data, separators=(',', ':')))

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

class BaseSearchClient(multisearch.client.BaseSearchClient):
    """Base SearchClient class for Xapian.

    """
    def __init__(self):
        serialised_schema = self.db.get_metadata("__ms:schema")
        self._schema = Schema.unserialise(serialised_schema)
        super(BaseSearchClient, self).__init__()

    @property
    def schema(self):
        """Get the schema in use by this client.

        """
        return self._schema

    def close(self):
        """Close any open resources.

        """
        if hasattr(self.db, 'close'):
            self.db.close()
        self.db = ClosedObject()

    @property
    def document_count(self):
        """Return the number of documents.

        """
        return self.db.get_doccount()

    def iter_documents(self):
        """Iterate through all the documents.

        """
        return DocumentIter(self.db, self.db.postlist(''))

    @staticmethod
    def get_docid_term(docid):
        """Get the term used to reference a given document ID.

        """
        return '!' + str(docid)

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
        """Return True if a document with the given id exists, False if not.

        """
        return self.db.term_exists(self.get_docid_term(docid))

    def query(self, value, fieldname=None, *args, **kwargs):
        # FIXME - document
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

    def search(self, query, params):
        """Perform a search.

        The query should be an instance of multisearch.queries.Query, and the
        params should be a dict of parameters.

        """
        xq = self.compile(query)
        enq = xapian.Enquire(self.db)
        enq.set_query(xq)

        order_by = params.get('order_by')
        if order_by is not None:
            enq.set_sort_by_value() # FIXME

        mset = enq.get_mset(params['start_rank'],
                            params['end_rank'] - params['start_rank'])
        return Results(self, mset)

class ReadonlySearchClient(BaseSearchClient):
    """A readonly Xapian SearchClient.

    """
    def __init__(self, path):
        self.db = xapian.Database(path)
        self.path = path
        super(ReadonlySearchClient, self).__init__()
        self.schema.modifiable = False

class WritableSearchClient(BaseSearchClient):
    """A writable Xapian SearchClient.

    """
    def __init__(self, path):
        self.db = xapian.WritableDatabase(path, xapian.DB_CREATE_OR_OPEN)
        self.path = path
        super(WritableSearchClient, self).__init__()

    def commit(self):
        """Commit any changes which are currently in progress.

        """
        if self.schema.modified:
            self.db.set_metadata("__ms:schema", self.schema.serialise())
            self.schema.modified = False
        if hasattr(self.db, 'commit'):
            self.db.commit()
        else:
            # Backwards compatibility: in the 1.0 series, databases don't have
            # a commit method.
            self.db.flush()

    def process(self, doc):
        """Process an incoming document into a Xapian document.

        """
        xdoc = xapian.Document()
        s = self.schema

        stored = {}
        state = {}

        fields_seen = set()
        for fieldname, value in utils.iter_doc_fields(doc):
            s.guess(fieldname, value)
            for destfield, route_params in s.get_route(fieldname):
                idx = self.schema.indexer(destfield)
                if destfield not in fields_seen:
                    fields_seen.add(destfield)
                    idx.new_doc(xdoc)
                idx(stored, value, route_params, state)

        result = XapianDocument(xdoc)
        result.set_data(stored)
        return result

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
            xdoc = self.process(doc).raw
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
        if hasattr(self.db, 'close'):
            self.db.close()
        shutil.rmtree(self.path)
