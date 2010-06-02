#!/usr/bin/env python

# If there is one, import an "ext" module first, which should set up the path
# to include the redis-py client.
try:
    import ext
except ImportError:
    pass

import redissearch
import unittest

class RedisTest(unittest.TestCase):
    def setUp(self):
        self.s = redissearch.RedisSearch('testdb')
        self.s.full_reset() # Try and clean up old test runs.

    def tearDown(self):
        # Try and clean up after ourselves
        self.s.full_reset()

    def test_basicops(self):
        """Test basic index and search operations.

        """
        s = self.s
        self.assertEqual(s.document_count(), 0)
        doc = {
            'title': 'My first document',
            'text': "This is a very simple document that we'd like to index",
        }
        id1 = s.add(doc)
        self.assertEqual(s.document_count(), 1)
        self.assertEqual(list(sorted(s.iter_docids())), ['1'])
        r = s.query(u'title', u'first').search(0, 10)
        self.assertEqual(len(r), 1)
        self.assertEqual(list(r), ['1'])
        r = (s.query(u'title', u'first') | s.query(u'text', u'very simple')).search(0, 10)
        self.assertEqual(len(r), 1)
        self.assertEqual(list(r), ['1'])

        s.delete(id1)
        self.assertEqual(s.document_count(), 0)
        self.assertEqual(list(sorted(s.iter_docids())), [])
        r = s.query(u'title', u'first').search(0, 10)
        self.assertEqual(len(r), 0)
        r = (s.query(u'title', u'first') | s.query(u'text', u'very simple')).search(0, 10)
        self.assertEqual(len(r), 0)


if __name__ == '__main__':
    unittest.main()
