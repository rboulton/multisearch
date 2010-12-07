Making a client
===============

All operations on a search engine are performed via a class implementing the
SearchClient interface.  There are some methods which must be implemented by
all backends, and others which may be omitted if the backend doesn't support
the relevant features.

To create a SearchClient there is a convenient factory function,
{{{multisearch.SearchClient()}}}.  This takes one required positional argument,
"type": the name of the backend type to use.  Other arguments depend on the
backend, but the following are commonly used:

 - `path`: For disk-based backends: the path on disk to store data under.
 - `host`: For backends accessed over the network: the host to connect to for
   access to the backend.
 - `port`: For backends accessed over the network: the port to connect to for
   access to the backend.
 - `readonly`: If True, the the client should be readonly, and write
   operations will not be permitted.  For some backends, search operations
   will only be possible on clients opened with readonly=True, and for some
   backends, only one client may be opened at a time with readonly=False.

Adding a document
=================

Documents can be added and updated using the SearchClient.
