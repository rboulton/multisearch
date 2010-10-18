MultiSearch
===========

Warning: experimental code.

This is an attempt to build a generic search engine interface in Python, which
exposes a common subset of features for each engine, but also allows each
engine to be accessed directly to allow the full range of features for that
engine to be used.

Example
-------

>>> import multisearch
>>> s = multisearch.SearchClient('xapian', 'testdb', readonly=False)
>>> docid = s.update({
...                'title': 'My first document',
...                'text': "This is a very simple document that we'd like to index",
...                })
>>> print repr(docid)
'1'
>>> r = s.query(u'title', u'first').search(0, 10)
>>> print list(r)
['1']

Features
--------

Add, update, delete documents.

Combine queries with AND, OR, NOT.  Python syntax is using the &, | and -
operators:

>>> q1 = s.query(u'title', u'first')
>>> q2 = s.query(u'title', u'second')
>>> qand = q1 & q2
>>> print list(qand.search(0, 10))
[]

Infrastructure for automatically guessing field types, and storing a schema
(though it always guesses "free text" currently).

Todo
----

Lots and lots and lots.

Facet calculation (should be quite easy; use SORT with GET to pull in facets
for each matching document).

Ranked results.  Initially, calculate ranks at client side, but aim to
calculate ranks on server side somehow.

More query operators.

Decent text procesing (currently just uses split()).  Should normalise case,
handle punctuation, and use stemming at the least.

Handle numeric ranges.
