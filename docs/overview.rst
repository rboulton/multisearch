Overview
========

There are currently many different search engine libraries, available which
allow developers to perform efficient free-text searches from Python.  However,
there is no unified interface to these libraries: indeed, some libraries have
several different python interfaces.  The features provided by the libraries
vary widely, but there are some features which are provided by most or all of
the libraries.  At the start of a project, it can be very difficult to know
which backend is going to turn out to be most appropriate for running a system.

The aim of multisearch is to be a single, simple interface for many different
search engine libraries, which provides access to the common features in a
standardised manner, without getting in the way of direct access to the
libraries to allow access to features which are unique to a particular engine.
This allows a developer to start developing search features for an application
without worrying too much about the implementation: unless and until special
features which are only provided by a single backend have been used, the
application can be switched to use a different backend simply by changing a few
configuration values.

If you really want to, you can think of multisearch as the equivalent of an ORM
for search engines (though there are also plenty of reasons that that's not a
perfect analogy).

For the 1.0 release of multisearch, I aim to provide support for at least the
following search engines:

 - Xapian: http://xapian.org/
 - Lucene: http://lucene.apache.org/
 - Solr: http://lucene.apache.org/solr/
 - Sphinx: http://www.sphinxsearch.com/
 - Elastic Search: http://www.elasticsearch.com/
 - Whoosh: http://bitbucket.org/mchaput/whoosh/wiki/Home

Possible future backends include Toyko Dystopia, Redis, PostgreSQL and MySQL.

Assumptions
===========

For it to make sense to implement support for a search engine library backend,
the library should support at least the following concepts:

 - Documents, consisting of pieces of text, and possibly other data.
 - A store of some kind, which documents can be added to (this process is
   referred to by multisearch as "indexing").
 - The ability to perform searches based on a textual query string, and get
   back an ordered list of results from the store.
 - The ability to combine queries together with boolean operators.

Core features
=============

The following features are provided for all backends.  If the backend doesn't
support the feature directly, it will be implemented by the multisearch client.
(Note: this list of features is deliberately conservative - it may be expanded
for future backends.)

Documents
---------

Documents are composed of either a list of fieldname-value pairs, or a
dictionary mapping from fieldnames to values.  In general, the fieldnames may
be arbitrary unicode strings, though some backends will impose restrictions on
the valid characters in the strings, so it may be wise to use only ASCII
alphanumeric characters and underscores in fieldnames.

All backends will allow values to be unicode strings, but some may also other
types, such as numbers, datetime objects, sequences

In some backends, documents may be more complex than this: for example Elastic
Search can reasonably be provided with a hierarchical object, and can be
configured to index such objects in quite sophisticated ways.

The order in which the fieldname-value pairs are provided may be significant
for some backends; but often only the order of values for a given fieldname
will be significant.

Schemas
-------

Multisearch provides the concept of a schema, which determines how the indexing
process handles incoming documents, and determines what kinds of searches can
then be performed on the documents.  The schema maps each fieldname to
configuration for that field.

For some backends (eg, Whoosh, Sphinx), the schema needs to be set before any
data is indexed, and cannot be modified after data has been supplied.

For other backends, new fields can be added to the schema at any time, but the
configuration for a field cannot be modified once it has been set.

For other backends, arbitrary modifications can be made to the schema at any
time (though there may be some latency before searches are possible with the
new schema).

If a field is provided in an incoming document which is not listed in the
schema, multisearch can do one of three things:

 - ignore the field (optionally producing a warning with the standard python
   warning module).
 - raise an exception, so that the document containing the field is not
   indexed.
 - guess how to handle the field, and add it to the schema.

Indexing
--------

Backends vary in how synchronous modifications to the store are.  When a
document is added to the store for some backends, the call will block until the
document has been added, processed, and written to permanent storage.  For
other backends, the call will return immediately after putting the document
into a queue to be added in the future.

From a users point of view, there are several important considerations:

 - At what point does the backend guarantee that the newly added document has
   been written to the store, and won't be dropped due to an error condition?
 - At what point will the backend make the document visible in the results of
   searches?
 - At what point does the backend provide a unique identifier for a document,
   which can be used in future accesses and modifications for the document?

The answer to each of these questions depends on the backend in use.

Sorting
-------

Results of a search should be returned in sorted order.  By default, this will
be a "relevance" order - the most relevant documents will be returned first.
The definition of relevance is left up to the backend, so different backends
will return documents in a different order for the same search.
