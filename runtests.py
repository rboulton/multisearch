#!/usr/bin/env python

import os
import shutil
import subprocess

try:
    shutil.rmtree('testdb')
except OSError: pass

p = subprocess.Popen(("nosetests",
                      "--with-coverage",
                      "--cover-erase",
                      "--cover-tests",
                      "--cover-inclusive",
                      "--cover-package=multisearch",
                      "--cover-package=unittests",
                      "--with-doctest",
                      "--doctest-extension=rst",
                      "unittests",
                      "README.rst",
                      "docs"))

sts = os.waitpid(p.pid, 0)[1]
