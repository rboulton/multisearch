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
r"""Documents, and associated objects.

"""
__docformat__ = "restructuredtext en"

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
        
        This consists of a dict of field values which were passed in which the
        schema caused to be stored.

        The data returned by this data shouldn't be modified (the effect of
        modifying it will vary depending on the backend, and is not the
        appropriate way to modify a document).

        """
        return self.get_data()

    @property
    def terms(self):
        """The terms stored in the document.

        This consists of an iterator over, or sequence of, term objects generated from
        field values by the actions in the schema.

        """
        return self.get_terms()

    def __str__(self):
        return "Document(docid=%r)" % self.docid

    def __repr__(self):
        return "<multisearch.Document(docid=%r)>" % self.docid
