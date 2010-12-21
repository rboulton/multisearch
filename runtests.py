#!/usr/bin/env python

import os
import shutil
import subprocess
import sys

outdir = 'cover_html'
if len(sys.argv) > 1:
    outdir = sys.argv[1]

try:
    shutil.rmtree('readme_testdb')
except OSError: pass

p = subprocess.Popen(("nosetests",
                      "--with-coverage",
                      "--cover-erase",
                      "--cover-inclusive",
                      "--cover-package=multisearch",
                      "--cover-html",
                      "--cover-html-dir=%s" % outdir,
                      "--cover-inclusive",
                      "--with-doctest",
                      "--doctest-extension=rst",
                      "unittests",
                      "README.rst",
                      "docs"))

sts = os.waitpid(p.pid, 0)[1]

try:
    shutil.rmtree('readme_testdb')
except OSError: pass

