Making a client
===============

All operations on a search engine are performed via a SearchClient.  This
class provides some basic behaviours which are shared across all search
engines, but delegates all the hard work to a `backend`.

To create a search client, call {{{multisearch.SearchClient()}}}.  There is
one required positional argument: the name of the backend to use.  Other
arguments may be required or optional depending on the backend in use.

Arguments
---------

 - `type`: The type of backend to create.
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
