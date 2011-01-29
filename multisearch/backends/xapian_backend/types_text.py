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
r"""Text field type.

"""
__docformat__ = "restructuredtext en"

from multisearch.backends.xapian_backend.operators import _opmap
from multisearch.backends.xapian_backend.xquery import XapianQuery
import multisearch.queries
import xapian

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
