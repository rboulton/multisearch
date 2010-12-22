# Copyright (c) 2010 Richard Boulton
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
r"""Definition of schemas, and the associated code to implement actions on a
field.

"""
__docformat__ = "restructuredtext en"

from multisearch import errors

class JsonSchema(object):
    """A schema mapping fields to types, with a JSON representation.

    This schema is suitable for use by backends which don't have their own
    schmea definitions.

    """
    def __init__(self):
        """Initialise the schema for a given database.

        """
        # The routing from incoming document input to fields to be processed.
        # Contains a list of output field names for each incoming field name.
        # Any incoming field which is not listed is passed to the output
        # unchanged.
        self.routes = {}

        # The field types which are known for this schema.  Keys are field
        # names, values are 2-tuples (type, params), where type is the name of
        # the type, and params is a dictionary of parameters for the type.
        self.fieldtypes = {}

        # The field type guessers to use, in order.
        self.guessers = []

        # Flag to indicate whether the schema has been modified from that
        # stored in the DB. Callers should set this to False when the schema
        # has been saved.
        self.modified = False

        # Flag to indicate when the schema is modifiable.
        # Some backends will set this to False.
        self.modifiable = True

    def check_modifiable(self):
        """Check that the schema is modifiable.

        """
        if not self.modifiable:
            raise errors.DbReadOnlyError("Attempt to modify schema for a "
                                         "readonly backend")

    def get(self, fieldname):
        """Get the field type and parameters.

        Raises KeyError if the field is not known.

        """
        return self.fieldtypes[fieldname]

    def set(self, fieldname, type, params):
        """Set the field type and parameters.

	For this backend, the field type cannot be changed once it is set.

        """
        self.check_modifiable()
        params = dict(params)
        if fieldname in self.fieldtypes:
            if self.fieldtypes[fieldname] != (type, params):
                raise errors.SearchClientError(
                    "Cannot change field type (for field %s)" % (fieldname, ))
            # No change - just return
            return
        self.fieldtypes[fieldname] = (type, params)
        self.modified = True

    def get_route(self, incoming_field):
        """Get the route for an incoming field.

        """
        r = self.routes.get(incoming_field)
        if r is not None:
            return r
        return ((incoming_field, {}), )

    def set_route(self, incoming_field, dest_fields):
        """Set the routing for an incoming field.

        Replaces any previous routing for that field (but doesn't update any
        previously indexed documents to use the new routing).

        dest_fields should contain a sequence of fieldname, parameter pairs.

        As a special case, dest_fields may contain a string, or a sequence of
        strings, which is equivalent to suppling an empty set of parameters
        for each item in the sequence.

        """
        self.check_modifiable()
        if isinstance(dest_fields, basestring):
            self.routes[incoming_field] = ((dest_fields, {}), )
            return

        route = []
        for item in dest_fields:
            if isinstance(item, basestring):
                route.append((item, {}))
                continue
            dest_field, params = item
            route.append((dest_field, dict(params)))
        self.routes[incoming_field] = tuple(route)
        self.modified = True

    def guess(self, fieldname, value):
        """Guess the route, type and parameters for a field, given its value.

        If the field is not known already, this guesses what route, field type
        and parameters would be appropriate, and sets the schema accordingly.

        """
        if fieldname in self.routes or fieldname in self.fieldtypes:
            return
        for guesser in self.guessers:
            if guesser(self, fieldname, value):
                return
        raise errors.SearchClientError(
                    "Cannot guess field type (for field %s)" % (fieldname, ))

    def append_guesser(self, guesser):
        """Append a guesser to the schema.

        """
        self.check_modifiable()
        self.guessers.append(guesser)
        self.modified = True

    def clear_guessers(self):
        self.check_modifiable()
        self.guessers = []
        self.modified = True

    def fields_of_type(self, type):
        """Get a list of the fieldnames for all fields of the given type.

        """
        return tuple(sorted(fieldname
                            for (fieldname, (ftype, params))
                            in self.fieldtypes.iteritems()
                            if ftype == type))
