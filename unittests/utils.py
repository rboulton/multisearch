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

if __name__ == '__main__':
    unittest.main()
