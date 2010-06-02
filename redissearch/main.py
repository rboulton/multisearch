try:
    import simplejson as json
except ImportError:
    import json

import hashlib
import queries
import re
import redis
import schema

# Safe characters
safechar_re = re.compile("^[a-zA-Z0-9',./]*$")

def sha(value):
    return hashlib.sha1(value).hexdigest()

class RedisWrapper(object):
    def __init__(self, dbprefix, client, docid=None):
        self.client = client
        self.pipe = client.pipeline(transaction=True)
        self.dbprefix = dbprefix
        self.docid = docid

    def alloc_docid(self):
        """Allocate a new document ID.

        The docid will be used for future indexing actions.

        """
        self.docid = hex(self.client.incr(self.dbprefix + 'nextid'))[2:]
        return self.docid

    def watch(self, key):
        self.client.execute_command("WATCH", self.dbprefix + key)

    def set(self, key, value):
        self.pipe.set(self.dbprefix + key, value)

    def delete(self, key):
        self.pipe.delete(self.dbprefix + key)

    def add_term(self, term):
        assert self.docid is not None
        self.pipe.sadd(self.dbprefix + 'docs:' + term, self.docid)
        self.pipe.sadd(self.dbprefix + 'terms:' + self.docid, term)

    def remove_term(self, term):
        assert self.docid is not None
        self.pipe.srem(self.dbprefix + 'docs:' + term, self.docid)
        self.pipe.srem(self.dbprefix + 'terms:' + self.docid, term)

    def remove_terms(self):
        for term in self.get_terms():
            self.pipe.srem(self.dbprefix + 'docs:' + term, self.docid)
        self.pipe.delete(self.dbprefix + 'terms:' + self.docid)

    def get_terms(self):
        return self.client.smembers(self.dbprefix + 'terms:' + self.docid)

    def set_data(self, data):
        self.set('data:' + self.docid, json.dumps(data, sort_keys=True))

    def remove_data(self):
        return self.delete('data:' + self.docid)

    def get_data(self):
        return self.client.get(self.dbprefix + 'data:' + self.docid)

    def set_docexists(self):
        self.pipe.sadd(self.dbprefix + 'docs', self.docid)

    def rem_docexists(self):
        self.pipe.srem(self.dbprefix + 'docs', self.docid)

    def get_docexists(self):
        return self.client.sismember(self.dbprefix + 'docs', self.docid)

    def execute(self):
        self.pipe.execute()

class RedisSearch(object):
    def __init__(self, dbname='', client=None, client_args={}):
        """Initialise a search client.

         - `dbname` is a name for the database to use.  This is used as a
           prefix for all keys stored in redis.
         - `client` is a redis client to use.  If None, a client is created.
         - `client_args` is the arguments to use to create a client.  These are
           used only if `client` is None.

        """
        assert safechar_re.match(dbname)
        if client is None:
            client = redis.Redis(**client_args)
        self.client = client
        self.schema = schema.Schema()
        self.dbprefix = dbname + ':'
        self.cache_timeout = 1000000 # Number of seconds cached items are kept

    def flush(self):
        if self.schema.modified:
            self.client.set(self.dbprefix + 'schema', self.schema.serialise())
            self.schema.modified = False

    def add(self, doc):
        """Add a document.

        Returns the document ID allocated for that document.

        """
        wrapper = RedisWrapper(self.dbprefix, self.client)
        wrapper.alloc_docid()
        self._store_doc(doc, wrapper)
        return wrapper.docid

    def update(self, docid, doc):
        """Update the document stored under the specified docid.

        """
        self.delete(docid)
        wrapper = RedisWrapper(self.dbprefix, self.client, docid)
        self._store_doc(doc, wrapper)

    def _store_doc(self, doc, wrapper):
        """Store a document.

        Used by both add() and update().

        """
        wrapper.set_data(doc)
        wrapper.set_docexists()

        # Add terms for the document.
        for fieldname, value in doc.iteritems():
            self.schema.guess(fieldname, value)
            indexer = self.schema.indexer(fieldname)
            for term in indexer(value):
                wrapper.add_term(term)

        # If we've seen new fields, the schema will have been modified with
        # guesses about their types.
        if self.schema.modified:
            wrapper.set('schema', self.schema.serialise())
            self.schema.modified = False

        # Perform all the commands.
        wrapper.execute()

    def delete(self, docid):
        """Delete a document.

        """
        wrapper = RedisWrapper(self.dbprefix, self.client, docid)
        wrapper.watch('terms:' + docid)
        wrapper.watch('data:' + docid)
        wrapper.remove_data()
        wrapper.rem_docexists()
        wrapper.remove_terms()
        wrapper.execute()

    def full_reset(self):
        """Delete all documents, clear the schema, and reset all state.

        """
        for docid in self.iter_docids():
            self.delete(docid)
        self.client.delete(self.dbprefix + 'schema')
        self.client.delete(self.dbprefix + 'docs')
        self.client.delete(self.dbprefix + 'nextid')

    def document_count(self):
        """Return the number of documents.

        """
        return self.client.scard(self.dbprefix + 'docs')

    def iter_docids(self):
        """Iterate through all the document IDs.

        """
        return iter(self.client.smembers(self.dbprefix + 'docs'))

    def query(self, fieldname, value, *args, **kwargs):
        """Build a basic query for the contents of a named field.

        """
        qg = self.schema.query_generator(fieldname)
        return qg(value, *args, **kwargs).connect(self)

    def search(self, search):
        """Perform a search.

        The search should be an instance of redissearch.queries.Search.  This
        method is usually called by the __call__ method of such an instance.

        """
        # walk through the query tree, returning each child before its parents,
        # building up a list of "primitive" queries to be performed, and a
        # sequence of operations to be done on those.
        def walk(query):
            """Walk a query tree.

            """
            stack = [[query, 0]]
            while len(stack) != 0:
                query, index = stack[-1]
                if isinstance(query, queries.QueryCombination):
                    if index < len(query.subqs):
                        stack[-1][1] = index + 1
                        stack.append([query.subqs[index], None])
                        continue
                yield len(stack) - 1, query
                del stack[-1]

        pieces = {} # pieces of the query being built up, keyed by depth
        cmds = []
        for depth, query in walk(search.query):
            if query.op == queries.Query.TERMS:
                keys = [(self.dbprefix + "docs:" + term, sha(term))
                        for term in query.terms]
                subpieces = pieces.setdefault(depth, [])
                if len(keys) == 0:
                    raise UnimplementedError("empty queries not yet implemented")
                elif len(keys) == 1:
                    termkey, cachekey = keys[0]
                    cmds.append(('score', cachekey, [termkey]))
                    subpieces.append(cachekey)
                else:
                    cachekeys = []
                    for termkey, cachekey in keys:
                        cmds.append(('score', cachekey, [termkey]))
                        cachekeys.append(cachekey)
                    cachekey = sha(''.join(cachekeys))
                    cmds.append((query.default_op, cachekey, cachekeys))
                    subpieces.append(cachekey)
            elif query.op in (queries.Query.OR, queries.Query.AND,
                              queries.Query.NOT):
                subpieces = pieces.setdefault(depth, [])
                cachekeys = pieces[depth + 1]
                cachekey = sha(str(query.op) + ':' + ''.join(cachekeys))
                cmds.append((query.op, cachekey, cachekeys))
                subpieces.append(cachekey)
                del pieces[depth + 1]
            else:
                raise UnimplementedError("Query operator %r not yet "
                                         "implemented" % query.opname(query.op))

        def cachekey(key):
            return self.dbprefix + "cache:" + key

        cleanup_keys = []
        pipe = self.client.pipeline(transaction=True)
        destkey = None
        resnum = 0
        for cmd, dest, inputs  in cmds:
            destkey = cachekey(dest)
            if cmd == 'score':
                # FIXME - actually, we want to calculate scores for the terms,
                # here.
                pipe.sinterstore(destkey, inputs)
                resnum += 1
            elif cmd == queries.Query.OR:
                pipe.sunionstore(destkey, [cachekey(input) for input in inputs])
                resnum += 1
            elif cmd == queries.Query.AND:
                pipe.sinterstore(destkey, [cachekey(input) for input in inputs])
                resnum += 1
            elif cmd == queries.Query.NOT:
                pipe.sdiffstore(destkey, [cachekey(input) for input in inputs])
                resnum += 1
            else:
                raise UnimplementedError("Unknown command: %r" % cmd)
            cleanup_keys.append(destkey)
        if destkey is None:
            return ()
        pipe.smembers(destkey)
        for key in cleanup_keys:
            pipe.delete(key)
        result = pipe.execute()
        return result[resnum]
