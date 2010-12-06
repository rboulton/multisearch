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
r"""Document processing routines.

"""
__docformat__ = "restructuredtext en"

import uuid

def iter_doc_fields(doc):
    """Iterate through the fields in a document-like structure.

    `doc` is either:
     - a dictionary, keyed by field name.  The values are lists of field
       values, or single values.
     - or, a sequence of (field name, field value) pairs.

    If a dict, the iteration order of fields is undefined.  The
    iteration order of values will always be as specified.

    """
    if isinstance(doc, dict):
        it = doc.iteritems()
    else:
        it = doc

    for fieldname, values in it:
        if isinstance(values, basestring):
            yield (fieldname, values)
        elif hasattr(values, '__iter__'):
            for value in values:
                yield (fieldname, value)
        else:
            yield (fieldname, values)

def make_docid():
    docid = str(uuid.uuid4())
