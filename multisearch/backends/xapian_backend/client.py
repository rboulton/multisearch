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
from multisearch.utils import json
from multisearch import utils
import xapian

class XapianQuery(multisearch.queries.Query):
    def __init__(self, xapq):
        super(XapianQuery, self).__init__()
        self.xapq = xapq
        self.method = None
        self.args = ()
        self.kwargs = {}

    def _set_params(self, method, args, kwargs):
        self.method = method
        self.args = args
        self.kwargs = kwargs

    def __unicode__(self):
        if self.method is None:
            return u'XapianQuery(%s)' % (self.xapq,)
        else:
            return u'XapianQuery(%s, %r, %r, %r)' % (self.xapq, self.method,
                                                     self.args, self.kwargs)

    def __repr__(self):
        if self.method is None:
            return u'<XapianQuery(%s)>' % (self.xapq,)
        else:
            return u'<XapianQuery(%s, %r, %r, %r)>' % (self.xapq, self.method,
                                                       self.args, self.kwargs)

    def __str__(self):
        return u'<%s>' % (self.xapq, )

_opmap = {
    multisearch.queries.Query.OR: xapian.Query.OP_OR,
    multisearch.queries.Query.AND: xapian.Query.OP_AND,
    multisearch.queries.Query.XOR: xapian.Query.OP_XOR,
    multisearch.queries.Query.NOT: xapian.Query.OP_AND_NOT,
}

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

class XapianTextIndexer(object):
    """Indexer for a text field.

    Accepts the following parameters:

     - store: boolean.  If True, store the field values in the document data.
     - prefix: string.  The prefix to insert before terms.  Should follow
       Xapian conventions (ie, be composed of upper case ascii characters, and
       start with X if more than one character long).
     - weight: integer (>= 0).  The weight bias to use for this field.
     - positions: boolean.  If True, store position information.
     - position_gap: integer (>= 0). The position gap to add between instances
       of the field.
     - lang: string.  The language to process the field contents as.  Should
       be one of the languages supported by the version of Xapian in use.
       Leave as an empty string to do no particular language specific
       processing (in which case, words will be split on spaces and
       punctuation).

    """
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        self.tg = xapian.TermGenerator()

        self.store = bool(params.get('store', True))
        self.prefix = str(params.get('prefix', ''))
        self.weight = int(params.get('weight', 1))
        assert self.weight >= 0
        if bool(params.get('positions', False)):
            self.idx_method = self.tg.index_text
        else:
            self.idx_method = self.tg.index_text_without_positions
        self.position_gap = int(params.get('position_gap', 1))
        self.lang = str(params.get('lang', ''))

        if self.lang:
            self.tg.set_stemmer(xapian.Stem(self.lang))

    def new_doc(self, xdoc):
        self.tg.set_document(xdoc)
        self.tg.set_termpos(0)

    def __call__(self, stored, values, route_params, state):
        if self.store:
            s = stored.get(self.fieldname, None)
            if s is None:
                stored[self.fieldname] = s = []
        else:
            s = None
        if isinstance(values, basestring):
            values = (values, )
        for value in values:
            self.idx_method(value, self.weight, self.prefix)
            self.tg.increase_termpos(self.position_gap)
            if s is not None:
                s.append(value)

def parse_with_qp(qp, query, baseflags, allow_wildcards):
    extraflags = 0
    if allow_wildcards:
        extraflags |= xapian.QueryParser.FLAG_WILDCARD

    try:
        return qp.parse_query(query,
                              baseflags | extraflags |
                              xapian.QueryParser.FLAG_BOOLEAN)
    except xapian.QueryParserError:
        return qp.parse_query(query, baseflags | extraflags)

class XapianTextQueryGenerator(object):
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        self.qp = xapian.QueryParser()
        self.prefix = str(params.get('prefix', ''))
        self.qp.add_prefix('', self.prefix)

        self.lang = str(params.get('lang', ''))
        if self.lang:
            self.qp.set_stemmer(xapian.Stem(lang))
            self.qp.set_stemming_strategy(qp.STEM_SOME)

        self.baseflags = (xapian.QueryParser.FLAG_LOVEHATE |
                          xapian.QueryParser.FLAG_PHRASE |
                          xapian.QueryParser.FLAG_AUTO_SYNONYMS |
                          xapian.QueryParser.FLAG_AUTO_MULTIWORD_SYNONYMS)

    def __call__(self, client, value,
                 default_op=multisearch.queries.Query.AND,
                 allow_wildcards=False):
        self.qp.set_database(client.db)
        self.qp.set_default_op(_opmap[default_op])

        return XapianQuery(parse_with_qp(self.qp, value, self.baseflags,
                                         allow_wildcards))

Schema.register_type("TEXT",
                     """Free text - words, to be parsed.""",
                     XapianTextIndexer,
                     XapianTextQueryGenerator)

#Schema.register_type("BLOB",
#                     """A literal string of bytes, to be matched exactly.""",
#                     XapianBlobIndexer,
#                     XapianBlobQueryGenerator)


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

class DocumentIter(object):
    """Iterate through a set of documents.

    """
    def __init__(self, client, iter):
        """Initialise the prefixed term iterator.

        - `client` is the client.
        - `iter` is the iterator to use, which should be at its start.

        """
        self.client = client
        self.iter = iter

    def __iter__(self):
        return self

    def next(self):
        """Get the next document.

        """
        posting = self.iter.next()
        return XapianDocument(self.client.db.get_document(posting.docid), self.client)

class Results(object):
    def __init__(self, client, mset):
        self.client = client
        self.mset = mset

    def __iter__(self):
        return DocumentIter(self.client, iter(self.mset))

    def __len__(self):
        return len(self.mset)

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
        return DocumentIter(self, self.db.postlist(''))

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
            return xapian.Query(xapian.Query.OP_MULT_WEIGHT,
                                self.compile(query.subq))
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
