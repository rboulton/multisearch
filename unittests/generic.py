#!/usr/bin/env python

import multisearch
import os
import shutil
import tempfile
import unittest

class XapianTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="multisearchtest")

    def tearDown(self):
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def test_basicops(self):
        """Test basic index and search operations.

        """
        db1path = os.path.join(self.tmpdir, "db1")
        client = multisearch.SearchClient('xapian', db1path, readonly=False)

        self.assertEqual(client.document_count, 0)
        self.assertEqual(len(client), client.document_count)

        doc = {
            'title': 'My first document',
            'text': "This is a very simple document that we'd like to index",
        }
        id1 = client.update(doc, docid=1)
        self.assertEqual(client.document_count, 1)
        self.assertEqual(list(sorted(doc.docid for doc in client.iter_documents())), ['1'])
        r = client.query(u'title', u'first').search(0, 10)
        self.assertEqual(len(r), 1)
        self.assertEqual(list(doc.docid for doc in r), ['1'])
        r = (client.query(u'title', u'first') | client.query(u'text', u'very simple')).search(0, 10)
        self.assertEqual(len(r), 1)
        self.assertEqual(list(doc.docid for doc in r), ['1'])

        client.delete(id1)
        self.assertEqual(client.document_count, 0)
        self.assertEqual(list(sorted(client.iter_documents())), [])
        r = client.query(u'title', u'first').search(0, 10)
        self.assertEqual(len(r), 0)
        self.assertEqual(list(r), [])
        r = (client.query(u'title', u'first') | client.query(u'text', u'very simple')).search(0, 10)
        self.assertEqual(len(r), 0)
        self.assertEqual(list(r), [])

if __name__ == '__main__':
    unittest.main()
