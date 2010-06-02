import sys
import os

def addtopath(*args):
    thisdir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, os.path.join(thisdir, *args))

addtopath('redis-py')
