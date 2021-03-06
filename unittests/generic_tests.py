#!/usr/bin/env python
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
r"""Generic behaviour tests.

"""
__docformat__ = "restructuredtext en"

from _harness import *

class GenericTest(MultiSearchTestCase):
    """Test generic search behaviours which should be the same across
    most or all backends.

    """
    @with_backends
    def test_basicops(self, backend):
        """Test basic index and search operations.

        """
        client = self.client(backend)

        self.assertEqual(client.document_count, 0)
        self.assertEqual(len(client), client.document_count)

        indoc = {
            'title': 'My first document',
            'text': "This is a very simple document that we'd like to index",
        }
        id1 = client.update(indoc, docid=1)
        self.assertEqual(id1, '1')
        self.assertEqual(client.document_count, 1)
        self.assertEqual(list(sorted(doc.docid for doc in client.iter_documents())), ['1'])
        self.assertEqual(list(sorted((doc.docid, doc.data) for doc in client.iter_documents())),
                         [('1', {
                           'title': ['My first document'],
                           'text': ["This is a very simple document that we'd like to index"],
                         })])
#        r = client.query(u'first', u'title').search(0, 10)
        r = client.query_field(u'title', u'first').search(0, 10)
        self.assertEqual(len(r), 1)
        self.assertEqual(list(doc.docid for doc in r), ['1'])
        r = (client.query(u'first', u'title') | client.query(u'very simple', u'text')).search(0, 10)
        self.assertEqual(len(r), 1)
        self.assertEqual(list(doc.docid for doc in r), ['1'])

        client.delete(id1)
        self.assertEqual(client.document_count, 0)
        self.assertEqual(list(sorted(client.iter_documents())), [])
        r = client.query(u'first', u'title').search(0, 10)
        self.assertEqual(len(r), 0)
        self.assertEqual(list(r), [])
        r = (client.query(u'first', u'title') | client.query(u'very simple', u'text')).search(0, 10)
        self.assertEqual(len(r), 0)
        self.assertEqual(list(r), [])

    @with_backends
    def test_close(self, backend):
        client = self.client(backend)
        indoc = {
            'title': 'lorem',
            'text': "ipsum",
        }
        id1 = client.update(indoc, docid=1)
        self.assertEqual(client.document_count, 1)
        oldschema = client.schema.serialise()
        self.assertNotEqual(oldschema, '')
        client.commit()
        client.close()
        client.close()
        self.assertRaises(multisearch.errors.DbClosedError, client.update, indoc, docid=2)
        client = self.client(backend)
        self.assertEqual(oldschema, client.schema.serialise())
        id1 = client.update(indoc, docid=2)
        self.assertEqual(client.document_count, 2)

    def test_invalid_backends(self):
        self.assertRaises(ImportError, self.client, 'unknown')
        self.assertRaises(ImportError, self.client, '!invalid')

if __name__ == '__main__':
    unittest.main()
