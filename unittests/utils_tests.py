#!/usr/bin/env python

from multisearch import utils
import unittest

class UtilsTest(unittest.TestCase):
    def test_safe_backend_name(self):
        """Test checks on safe backend names

        """
        self.assertTrue(utils.is_safe_backend_name('hello'))
        self.assertTrue(utils.is_safe_backend_name('h_ello'))
        self.assertFalse(utils.is_safe_backend_name('Hello'))
        self.assertFalse(utils.is_safe_backend_name('HELLO'))
        self.assertFalse(utils.is_safe_backend_name('hello.world'))
        self.assertFalse(utils.is_safe_backend_name('_hello'))

class LazyJsonObjectTest(unittest.TestCase):
    def test_access(self):
        """Test accessing and modifying a LazyJsonObject.

        """
        obj = utils.LazyJsonObject()
	self.assertRaises(KeyError, obj.__getitem__, 'hi')
        obj['hi'] = 2
        self.assertEqual(obj['hi'], 2)
        self.assertEqual(obj.json, '{"hi":2}')
        self.assertEqual(tuple(obj.items()), (("hi", 2),))
        obj['hello'] = 3
        self.assertEqual(tuple(sorted(obj.items())), (("hello", 3), ("hi", 2)))
        self.assertEqual(utils.json.loads(obj.json), {"hi": 2, "hello": 3})

if __name__ == '__main__':
    unittest.main()
