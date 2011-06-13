MultiSearch
===========

Warning: experimental code.

This was an attempt to build a generic search engine interface in Python, which
exposes a common subset of features for each engine, but also allows each
engine to be accessed directly to allow the full range of features for that
engine to be used.

For now, I'm declaring the attempt a failure - I don't think it's completely
impossible to implement this, but it's a very hard problem because of the
massive variation in the interfaces provided by each engine.  Cross engine
integration is probably better done at a higher level, for a particular access
pattern, rather than at a low level attempting all allow all types of search
engine usage.

Example
-------

Create a client, add a document, then perform a search for it.

>>> import multisearch
>>> s = multisearch.SearchClient('xapian', 'readme_testdb', readonly=False)
>>> docid = s.update({
...                'title': 'My first document',
...                'text': "This is a very simple document that we'd like to index",
...                })
>>> print docid                                                     # doctest: +SKIP
7feedfd6bbc44247be040436928eccf3

>>> search = s.query(u'first').search(0, 10)
>>> print [result.data for result in search]
[{'text': ["This is a very simple document that we'd like to index"], 'title': ['My first document']}]

>>> search = s.query_field(u'title', u'first').search(0, 10)
>>> print [result.data for result in search]
[{'text': ["This is a very simple document that we'd like to index"], 'title': ['My first document']}]

Features
--------

Add, update, delete documents.

Combine queries with AND, OR, NOT.  Python syntax is using the &, | and -
operators:

>>> q1 = s.query(u'first')
>>> q2 = s.query(u'second')
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
