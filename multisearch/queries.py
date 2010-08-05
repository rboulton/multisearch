# Copyright (c) 2009 Lemur Consulting Ltd
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
r"""Abstract definitions of queries and searches.

"""
__docformat__ = "restructuredtext en"

class Query(object):
    """Base class of all queries.

    All queries have the "op" member, which is used to identify the type
    of the query.

    """
    op = None
    conn = None

    def __unicode__(self):
        return u"Query()"

    def __repr__(self):
        return u"Query()"

    # Constants used to represent the operators.
    OR = 0
    AND = 1
    XOR = 2
    NOT = 3
    MULTWEIGHT = 4
    ALL = 5
    NONE = 6
    TERMS = 7
    SIMILAR = 8

    # The names of the operators, in order.
    OP_NAMES = (u'OR', u'AND', u'XOR', u'NOT',
                u'MULTWEIGHT',
                u'ALL', u'NONE',
                u'TERMS', u'SIMILAR',
               )

    # Symbols representing the operators, in order.  None if no symbol.
    OP_SYMS = (u'|', u'&', u'^', u'-',
               None,
               None, None,
               None, None,)

    @staticmethod
    def opname(op):
        """Get the textual name of an operator from its code.

        """
        return Query.OP_NAMES[op]

    @staticmethod
    def opsym(op):
        """Get the symbol for an operator from its code.

        """
        return Query.OP_SYMS[op]

    @staticmethod
    def compose(op, queries):
        """Compose a set of queries with the specified operator.

        The allowed operators are:

         - 'OR': documents match if any queries match.
         - 'AND': documents only match if all queries match.
         - 'XOR': documents match if exactly one of the subqueries match.
         - 'NOT', (or the synonym 'ANDNOT'): documents match only if the
           first query matches, and none of the other queries match.

        The operator may be specified by using one of the exact strings above,
        or by using one of the corresponding constants on the Query class
        (Query.OR, Query.AND, Query.XOR, Query.NOT).

        """
        if len(queries) < 2:
            if len(queries) == 0:
                return Query()
            else:
                return queries[0]

        for query in queries:
            if not isinstance(query, Query):
                raise TypeError("Object supplied to Query.compose() "
                                "was not a Query.")

        return {
            Query.OR: QueryOr,
            Query.AND: QueryAnd,
            Query.XOR: QueryXor,
            Query.NOT: QueryNot,
            "OR": QueryOr,
            "AND": QueryAnd,
            "XOR": QueryXor,
            "NOT": QueryNot,
            "ANDNOT": QueryNot,
        }[op](queries)

    def __mul__(self, mult):
        """Return a query with the weight scaled by multiplier.

        """
        try:
            return QueryMultWeight(self, mult)
        except TypeError:
            return NotImplemented

    def __rmul__(self, lhs):
        """Return a query with the weight scaled by multiplier.

        """
        return self.__mul__(lhs)

    def __div__(self, rhs):
        """Return a query with the weight divided by a number.

        """
        try:
            return self.__mul__(1.0 / rhs)
        except TypeError:
            return NotImplemented

    def __truediv__(self, rhs):
        """Return a query with the weight divided by a number.

        """
        try:
            return self.__mul__(1.0 / rhs)
        except TypeError:
            return NotImplemented

    def __and__(self, other):
        """Return a query combined using AND with another query.

        """
        if not isinstance(other, Query):
            return NotImplemented
        return QueryAnd((self, other))

    def __or__(self, other):
        """Return a query combined using OR with another query.

        """
        if not isinstance(other, Query):
            return NotImplemented
        return QueryOr((self, other))

    def __xor__(self, other):
        """Return a query combined using XOR with another query.

        """
        if not isinstance(other, Query):
            return NotImplemented
        return QueryXor((self, other))

    def __sub__(self, other):
        """Return a query combined using NOT with another query.

        """
        if not isinstance(other, Query):
            return NotImplemented
        return QueryNot((self, other))

    def filter(self, other):
        """Return a query filtered by another query.

        """
        return QueryAnd((self, other * 0))

    def connect(self, conn):
        """Connect this query to a connection.

        This is normally handled automatically by a backend when leaf queries
        are created.

        """
        if self.conn is None:
            self.conn = conn
        elif self.conn is not conn:
            raise ValueError("Can't connect query %r to connection %r - "
                             "already attached to a different connection %r"
                             % self, conn, self.conn)
        return self

    def search(self, *args, **kwargs):
        """Make a search using this query.

        """
        return Search(self, *args, **kwargs)

class QueryCombination(Query):
    """A query which represents a combination of other queries.

    """
    def __init__(self, subqs):
        self.subqs = list(subqs)
        for query in self.subqs:
            if not isinstance(query, Query):
                raise TypeError("Object supplied to QueryCombination() "
                                "was not a Query.")
            if query.conn is not None:
                self.connect(query.conn)

    def __unicode__(self):
        joinsym = u' ' + Query.opsym(self.op) + u' '
        return u'(' + joinsym.join(unicode(q) for q in self.subqs) + u')'

    def __repr__(self):
        joinsym = ' ' + Query.opsym(self.op) + ' '
        return '(' + joinsym.join(repr(q) for q in self.subqs) + ')'

class QueryOr(QueryCombination):
    """A query which matches a document if any of its subqueries match.

    The weights for the returned query will be the sum of the weights of the
    subqueries which match.

    """
    op = Query.OR

class QueryAnd(QueryCombination):
    """A query which matches a document if all of its subqueries match.

    The weights for the returned query will be the sum of the weights of the
    subqueries.

    """
    op = Query.AND

class QueryXor(QueryCombination):
    """A query which matches a document if exactly one of its subqueries match.

    The weights for the returned query will be the sum of the weights of the
    subqueries which match.

    """
    op = Query.XOR

class QueryNot(QueryCombination):
    """A query which matches a document if its first subquery matches but none
    of its other subqueries do.

    The weights for the returned query will be the weights of the first
    subquery.

    """
    op = Query.NOT
    def __init__(self, subqs):
        if len(subqs) > 2:
            subqs = (subqs[0], QueryOr(subqs[1:]))
        QueryCombination.__init__(self, subqs)

class QueryMultWeight(Query):
    """A query which returns the same documents as a sub-query, but with the
    weights multiplied by the given number.

    """
    op = Query.MULTWEIGHT
    def __init__(self, subq, mult):
        self.subq = subq
        self.mult = float(mult)
    def __unicode__(self):
        return u"(%s * %.4g)" % (unicode(self.subq), self.mult)
    def __repr__(self):
        return "(%s * %.4g)" % (repr(self.subq), self.mult)

class QueryAll(Query):
    """A query which matches all documents.

    """
    op = Query.ALL

class QueryNone(Query):
    """A query which matches no documents.

    """
    op = Query.NONE

class QueryTerms(Query):
    """A query which returns the documents containing a set of terms.

    """
    op = Query.TERMS
    def __init__(self, terms, default_op=None, conn=None):
        """Create a QueryTerms.

        - `terms` is a sequence of (unicode) terms.  
        - `default_op` is the operator to use to combine terms: it may be either
          Query.AND or Query.OR.  If None, it defaults to to Query.AND.

        """
        assert not isinstance(terms, basestring)
        terms = list(terms)
        for term in terms:
            if not isinstance(term, unicode):
                raise TypeError("Term supplied was not unicode")
        if default_op is None:
            default_op = Query.AND
        if default_op not in (Query.AND, Query.OR,):
            raise TypeError("Operator must be either Query.AND or Query.OR")
        self.terms = terms
        self.default_op = default_op
        self.conn = conn
    def __unicode__(self):
        return (u"QueryTerms(%r, default_op=%s)" %
                (self.terms, Query.opname(self.default_op)))
    def __repr__(self):
        return ("QueryTerms(%r, default_op=%s)" %
                (self.terms, Query.opname(self.default_op)))

class QuerySimilar(Query):
    """A query which returns similar documents to a given set of documents.

    """
    op = Query.SIMILAR
    def __init__(self, ids, simterms=20, conn=None):
        """Create a similarity query.

        The similarity query returns documents which are similar to those
        supplied.

        - `ids` is an iterable holding a list of document ids to find similar
          items to.
        - `simterms` is a suggestion for the number of terms to use in the
          similarity calculation.  Backends may ignore this, or treat it only
          as a recommendation.

        """
        self.ids = list(ids)
        self.simterms = int(simterms)
        self.conn = conn

    def __unicode__(self):
        return u"QuerySimilar(%r)" % (self.ids, )
    def __repr__(self):
        return "QuerySimilar(%r)" % (self.ids, )

class Search(object):
    def __init__(self, query, start_rank, end_rank, percent_cutoff=None):
        self.query = query
        self.start_rank = start_rank
        self.end_rank = end_rank
        self.percent_cutoff = percent_cutoff
        self.results = None

    def __len__(self):
        if self.results is None:
            self.execute()
        return len(self.results)

    def __iter__(self):
        if self.results is None:
            self.execute()
        return iter(self.results)

    def execute(self):
        """Perform the search.

        """
        if self.query.conn is None:
            raise RuntimeError("Query was not connected to a database - can't "
                               "execute it.")
        self.results = self.query.conn.search(self)

    def __unicode__(self):
        r = u"%r, %d, %d" % (self.query, self.start_rank, self.end_rank)
        if self.percent_cutoff is not None and self.percent_cutoff > 0:
            r += u", %d" % self.percent_cutoff
        return u"Search(%s)" % r
    def __repr__(self):
        r = "%r, %d, %d" % (self.query, self.start_rank, self.end_rank)
        if self.percent_cutoff is not None and self.percent_cutoff > 0:
            r += ", %d" % self.percent_cutoff
        return "Search(%s)" % r


query_types = 'Query,QueryOr,QueryAnd,QueryXor,QueryNot,' \
              'QueryMultWeight,QueryAll,QueryNone,QueryTerms,' \
              'QuerySimilar'
query_types = [(q, globals()[q]) for q in query_types.split(',')]
del q
query_types.sort()
