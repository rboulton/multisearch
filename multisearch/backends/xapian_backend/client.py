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

import multisearch.backends.xapian_backend.errors
from multisearch.schema import Schema
import xapian

def XapianBackendFactory(path, readonly=False, *args, **kwargs):
    """Factory for XapianBackends.

    """
    if readonly:
        return ReadonlyBackend(path, *args, **kwargs)
    else:
        return WritableBackend(path, *args, **kwargs)

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

class Backend(object):
    """Base class for Xapian Backends.

    """
    def __init__(self):
        serialised_schema = self.db.get_metadata("__ms:schema")
        self.schema = Schema.unserialise(serialised_schema)

    def close(self):
        self.db.close()

    def get_document_count(self):
        return self.db.get_doccount()

    def document_iter(self):
        FIXME # return a lazily evaluated object which behaves like a sequence and returns documents

    def query(self, fieldname, value, *args, **kwargs):
        FIXME

    def search(self, search):
        FIXME

class ReadonlyBackend(Backend):
    """A readonly Xapian Backend.

    """
    def __init__(self, path, *args, **kwargs):
        self.db = xapian.Database(path)
        Backend.__init__(self)
        self.schema.modifiable = False

class WritableBackend(Backend):
    """A readonly Xapian Backend.

    """
    def __init__(self, path, *args, **kwargs):
        self.db = xapian.WritableDatabase(path, xapian.DB_CREATE_OR_OPEN)
        Backend.__init__(self)

    def commit(self):
        self.db.commit()

    def update(self, doc, docid=None, fail_if_exists=False):
        result = xapian.Document()
        for fieldname, value in doc:
            self.schema.guess(fieldname, value)
            idx = self.schema.indexer(fieldname)
            for term in idx(value):
                doc.add_term(term)
        if docid is not None:
            doc.add_term("I" + docid) # FIXME - ensure that a ":" in the docid
            # doesn't risk producing a valid prefix. How?  Escape them?
        print [item for item in result.termlist()]

    def delete(self, docid, fail_if_missing=False):
        FIXME

    def destroy_database(self):
        self.db.close()
