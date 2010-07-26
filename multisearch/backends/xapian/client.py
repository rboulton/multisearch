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

from multisearch.client import BaseSearchClient
import xapian

def XapianSearchClientFactory(path, readonly):
    if readonly:
        return ReadonlySearchClient(path)
    else:
        return WritableSearchClient(path)

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

class SearchClient(BaseSearchClient):
    def close(self):
        self.db.close()

    def document_count(self):
        return self.db.get_doccount()

    def all(self):
        FIXME # return a lazily evaluated object which behaves like a sequence and returns documents

    def query(self, fieldname, value, *args, **kwargs):
        pass

    def search(self, search):
        pass

class ReadonlySearchClient(SearchClient):
    def __init__(self, path):
        SearchClient.__init__(self)
        self.db = xapian.Database(path)

class WritableSearchClient(SearchClient):
    def __init__(self, path):
        SearchClient.__init__(self)
        self.db = xapian.WritableDatabase(path, xapian.DB_CREATE_OR_OPEN)

    def commit(self):
        self.db.commit()

    def update(self, doc, docid=None, fail_if_exists=False):
        pass

    def delete(self, docid, fail_if_missing=False):
        pass

    def full_reset(self):
        pass
