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
from multisearch.backends.xapian_backend.types_blob import XapianBlobIndexer, XapianBlobQueryGenerator
from multisearch.backends.xapian_backend.types_text import XapianTextIndexer, XapianTextQueryGenerator, parse_with_qp
from multisearch.backends.xapian_backend.types_float import XapianFloatIndexer, XapianFloatQueryGenerator
from multisearch.backends.xapian_backend.xquery import XapianQuery
from multisearch.backends.xapian_backend.operators import _opmap
import multisearch.client
import multisearch.errors
import multisearch.queries
from multisearch.utils.jsonschema import JsonSchema
from multisearch.utils import json
from multisearch import utils
import xapian

class DefaultGuesser(object):
    def __init__(self, **kwargs):
        pass

    def serialise(self):
        return ('multisearch.backends.xapian_backend.client',
                'DefaultGuesser', {})

    def __call__(self, schema, fieldname, value):
        schema.set_route(fieldname, ("", fieldname))
        schema.set(fieldname, "TEXT", {
                       'prefix': schema.prefix_from_fieldname(fieldname),
                   })
        if '' not in schema.fieldtypes:
            schema.set('', "TEXT", {'prefix': '', 'store': False})
        return True

class Schema(JsonSchema):
    known_types = {}

    # Schema version that this class creates.
    SCHEMA_FORMAT_VERSION = 1

    @classmethod
    def register_type(cls, name, doc, indexer, querygen):
        cls.known_types[name] = (doc, indexer, querygen)

    def __init__(self):
        super(Schema, self).__init__()
        self.append_guesser(DefaultGuesser())
        self.next_slot = 0

    @classmethod
    def unserialise(cls, value):
        """Load the schema from json.

        """
        if value is None or value == '':
            return Schema()
        schema = json.loads(value)
        format_version = schema['format_version']
        if format_version != cls.SCHEMA_FORMAT_VERSION:
            raise multisearch.errors.SearchClientError(
                "Can't handle this version of the schema (got "
                "version %s - I understand version %s" %
                (format_version, cls.SCHEMA_FORMAT_VERSION))
        result = Schema()
        result.fieldtypes = schema['fieldtypes']
        result.routes = schema['routes']
        result.clear_guessers()
        for module_name, name, kwargs in schema['guessers']:
            m = __import__(module_name, fromlist=[name], level=0)
            result.append_guesser(getattr(m, name)(**kwargs))
        result.next_slot = schema['next_slot']
        result.modified = False
        return result

    def serialise(self):
        """Serialise the schema to json.

        """
        schema = dict(
            format_version=self.SCHEMA_FORMAT_VERSION,
            fieldtypes=self.fieldtypes,
            routes=self.routes,
            guessers=[g.serialise() for g in self.guessers],
            next_slot = self.next_slot,
        )
        return json.dumps(schema, sort_keys=True)

    def prefix_from_fieldname(self, fieldname):
        return 'X' + ''.join(c.upper() for c in fieldname if c.isalnum())

    def indexer(self, fieldname):
        """Get the indexer for a field.

        Raises KeyError if the field is not in the schema.

        """
        type, params = self.get(fieldname)
        return self.known_types[type][1](fieldname, params)

    def query_generator(self, fieldname):
        """Get a query generator for a field.

        Raises KeyError if the field is not in the schema.

        """
        type, params = self.get(fieldname)
        return self.known_types[type][2](fieldname, params)

    def alloc_slot(self):
        self.next_slot += 1
        return self.next_slot - 1

Schema.register_type("TEXT",
                     """Free text - words, to be parsed.""",
                     XapianTextIndexer,
                     XapianTextQueryGenerator)

Schema.register_type("BLOB",
                     """A literal string of bytes, to be matched exactly.""",
                     XapianBlobIndexer,
                     XapianBlobQueryGenerator)

Schema.register_type("FLOAT",
                     """A floating point number, supporting exact matching or range searches.""",
                     XapianFloatIndexer,
                     XapianFloatQueryGenerator)


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
    def __init__(self, raw, client):
        """Create a XapianDocument.

        `raw` is the Xapian Document object wrapped by this.

        """
        self.raw = raw
        self.client = client

    def get_docid(self):
        """Get the document's id.

        """
        tl = self.raw.termlist()
        try:
            term = tl.skip_to(self.client.idprefix).term
            if len(term) == 0 or term[0] != self.client.idprefix:
                return None
        except StopIteration:
            return None
        return term[1:]

    def set_docid(self, newid):
        oldid = self.get_docid()
        if oldid is not None:
            self.raw.remove_term(self.client.idprefix + str(oldid))

        if newid is not None:
            self.raw.add_term(self.client.idprefix + str(newid), 0)

    def get_data(self):
        """Get the data stored in the document.

        """
        return json.loads(self.raw.get_data())

    def set_data(self, data):
        """Set the data stored in the document.

        """
        self.raw.set_data(json.dumps(data, separators=(',', ':')))

class XapianResultDocument(XapianDocument):
    def __init__(self, raw, client, rank):
        super(XapianResultDocument, self).__init__(raw, client)
        self.rank = rank

class DocumentIter(object):
    def __init__(self, iter, factory):
        self.iter = iter
        self.factory = factory

    def __iter__(self):
        return self

    def next(self):
        """Get the next document.

        """
        posting = self.iter.next()
        return self.factory(posting)

class Results(object):
    def __init__(self, client, mset, start_rank):
        self.client = client
        self.mset = mset
        self.start_rank = start_rank
        self.end_rank = start_rank + len(mset)

    def __iter__(self):
        client = self.client
        db = client.db
        def factory(posting):
            rawdoc = posting.document
            doc = XapianResultDocument(rawdoc, client, posting.rank)
            return doc

        return DocumentIter(iter(self.mset), factory)

    def at_rank(self, rank):
        if self.start_rank > rank or self.end_rank <= rank:
            raise IndexError("result requested at rank %d, which is outside the calculated range of %d-%d" % (rank, self.start_rank, self.end_rank - 1))
        rawdoc = self.mset[rank - self.start_rank].document
        return XapianResultDocument(rawdoc, self.client, rank)

    def __len__(self):
        return len(self.mset)

    @property
    def matches_lower_bound(self):
        return self.mset.get_matches_lower_bound()

    @property
    def matches_estimated(self):
        return self.mset.get_matches_estimated()

    @property
    def matches_upper_bound(self):
        return self.mset.get_matches_upper_bound()

class BaseSearchClient(multisearch.client.BaseSearchClient):
    """Base SearchClient class for Xapian.

    """
    idprefix = 'Q'
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
        if not isinstance(self.db, ClosedObject):
            self.commit()
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
        def factory(posting):
            rawdoc = self.db.get_document(posting.docid)
            return XapianDocument(rawdoc, self)

        return DocumentIter(self.db.postlist(''), factory)

    def get_docid_term(self, docid):
        """Get the term used to reference a given document ID.

        """
        return self.idprefix + str(docid)

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
                return XapianDocument(self.db.get_document(plitem.docid), self)
            except xapian.DatabaseModifiedError, e:
                self.db.reopen()

    def document_exists(self, docid):
        """Return True if a document with the given id exists, False if not.

        """
        return self.db.term_exists(self.get_docid_term(docid))

    def query(self, value, allow=None, deny=None,
              default_op=multisearch.queries.Query.AND,
              allow_wildcards=False):
        qp = xapian.QueryParser()
        qp.set_database(self.db)

        def totuple(val):
            if val is None:
                return ()
            if isinstance(val, basestring):
                return (val, )
            return tuple(val)

        allow = totuple(allow)
        deny = totuple(deny)

        if allow and deny:
            raise multisearch.errors.SearchClientError(
                "At most one of allow and deny may be specified")

        if not allow:
            allow = tuple(fieldname
                          for fieldname in self.schema.fields_of_type('TEXT')
                          if fieldname != '')
        if not deny:
            allow = tuple(fieldname
                          for fieldname in allow
                          if fieldname not in deny)

        for fieldname in allow:
            type, params = self.schema.get(fieldname)
            if type == 'TEXT':
                qp.add_prefix(fieldname, params.get('prefix', ''))
            else:
                raise multisearch.errors.SearchClientError(
                    "Can't handle field %r of type %r in query parser" %
                    (fieldname, type))

        try:
            type, params = self.schema.get('')
            lang = params.get('lang', '')
            if lang:
                qp.set_stemmer(xapian.Stem(lang))
                qp.set_stemming_strategy(qp.STEM_SOME)
        except KeyError:
            pass


        baseflags = (xapian.QueryParser.FLAG_LOVEHATE |
                     xapian.QueryParser.FLAG_PHRASE |
                     xapian.QueryParser.FLAG_AUTO_SYNONYMS |
                     xapian.QueryParser.FLAG_AUTO_MULTIWORD_SYNONYMS)
        query = XapianQuery(parse_with_qp(qp, value, baseflags,
                                          allow_wildcards))
        query.connect(self)
        query._set_params('query',
                          (value, ),
                          dict(allow=allow, deny=deny,
                               default_op=default_op,
                               allow_wildcards=allow_wildcards
                              ))
        return query

    def query_field(self, fieldname, *args, **kwargs):
        # FIXME - document
        qg = self.schema.query_generator(fieldname)
        query = qg(self, *args, **kwargs)
        query.connect(self)
        query._set_params('query_field',
                          tuple((fieldname, ) + args),
                          kwargs)
        return query

    def compile(self, query):
        """Make a xapian Query from a query tree.

        """
        if isinstance(query, multisearch.queries.QueryCombination):
            subqs = [self.compile(subq) for subq in query.subqs]
            try:
                op = _opmap[query.op]
            except KeyError:
                raise multisearch.errors.UnknownQueryTypeError(
                    "Query operator unknown (%s)" % query.op)
            return xapian.Query(op, subqs)
        elif isinstance(query, multisearch.queries.QueryMultWeight):
            return xapian.Query(xapian.Query.OP_SCALE_WEIGHT,
                                self.compile(query.subq), query.mult)
        elif isinstance(query, multisearch.queries.QueryAll):
            return xapian.Query("")
        elif isinstance(query, multisearch.queries.QueryNone):
            return xapian.Query()
        elif isinstance(query, multisearch.queries.QueryTerms):
            return xapian.Query(query.default_op,
                                [xapian.Query(term) for term in query.terms])
        elif isinstance(query, XapianQuery):
            return query.xapq
        elif isinstance(query, multisearch.queries.QuerySimilar):
            raise multisearch.errors.FeatureNotAvailableError("Similarity queries not yet implemented for Xapian backend")
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
        if order_by:
            if not isinstance(order_by, basestring):
                # FIXME - support multiple sort orders.
                raise multisearch.errors.FeatureNotAvailableError("Xapian backend currently only supports ordering by a single field.")
            ascending = True
            if order_by[0] == '+':
                order_by = order_by[1:]
            elif order_by[0] == '-':
                ascending = False
                order_by = order_by[1:]
            if order_by:
                order_type, order_params = self.schema.get(order_by)
                try:
                    slot = order_params['slot']
                except KeyError:
                    raise multisearch.errors.FeatureNotAvailableError("Cannot sort by this field type - no associated slot")
                enq.set_sort_by_value(slot, not ascending)

        check_at_least = params.get('check_at_least', 0)
        if check_at_least == -1:
            check_at_least = self.db.get_doccount()
        extra_args = list(params.get('search_args', []))

        start_rank = params['start_rank']
        end_rank = params['end_rank']
        mset = enq.get_mset(start_rank, end_rank - start_rank,
                            check_at_least, *extra_args)
        return Results(self, mset, start_rank)

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

        idxs = {}
        for fieldname, value in utils.iter_doc_fields(doc):
            s.guess(fieldname, value)
            for destfield, route_params in s.get_route(fieldname):
                idx = idxs.get(destfield, None)
                if idx is None:
                    idxs[destfield] = idx = self.schema.indexer(destfield)
                    idx.new_doc(xdoc)
                idx(stored, value, route_params, state)

        result = XapianDocument(xdoc, self)
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
