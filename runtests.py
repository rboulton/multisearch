#!/usr/bin/env python

import os
import shutil
import subprocess

shutil.rmtree('testdb')

p = subprocess.Popen(("nosetests", "--with-coverage", "--cover-erase",
                      "--cover-tests", "--cover-inclusive", "--with-doctest",
                      "--doctest-extension=rst", "unittests/", "README.rst",
                      "docs"))

sts = os.waitpid(p.pid, 0)[1]
