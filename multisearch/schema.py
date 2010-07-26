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

try:
    from simplejson import json
except ImportError:
    import json
import queries

# Schema version that this creates.
SCHEMA_VERSION = 1

class Schema(object):
    """A schema, mapping fields to types.

    """

    # The known field types
    TEXT = 0 # "Free text" - words, to be parsed.
    BLOB = 1 # A literal string of bytes - to be matched exactly.

    alltypes = (TEXT, BLOB, )

    def __init__(self):
        """Initialise the schema for a given database.

        """
        # The types which are known for this schema.
        # Keys are field names, values are 2-tuples (type, params), where type
        # is a member of alltypes, and params is a dictionary of arbitrary
        # parameters for the type.
        self.types = {}

        # Flag to indicate whether the schema has been modified.
        # Callers should set this to False when the schema has been saved.
        self.modified = False

        # A cache of indexers for this schema.
        self._indexer_cache = {}

        # A cache of query generators for this schema.
        self._generator_cache = {}

    def unserialise(self, value):
        """Load the schema from json.

        Overrides any existing configuration in the schema.

        """
        schema = json.loads(value)
        if schema['version'] != SCHEMA_VERSION:
            raise RuntimeError("Can't handle this version of the schema (got "
                               "version %s - I understand version %s" %
                               (schema['version'], SCHEMA_VERSION))
        self.types = dict(schema['fieldtypes'])
        self.modified = False

    def serialise(self):
        """Serialise the schema to json.

        """
        schema = (
            ('version', SCHEMA_VERSION),
            ('fieldtypes', self.types),
        )
        return json.dumps(schema, sort_keys=True)

    def get(self, fieldname):
        """Get the field type and parameters.

        Raises KeyError if the field is not known.

        """
        return self.types[fieldname]

    def set(self, fieldname, type, params):
        """Set the field type and parameters.

        The field type cannot be changed once it is set.

        """
        assert type in self.alltypes
        params = dict(params)
        if fieldname in self.types:
            if self.types[fieldname] != (type, params):
                raise RuntimeError("Cannot change field type "
                                   "(for field %s)" % (fieldname, ))
            # No change - just return
            return
        self.types[fieldname] = (type, params)
        self.modified = True

    def guess(self, fieldname, value):
        """Guess the type and parameters for a field, given its value.

        Returns the field type and parameters, and sets the schema accordingly.

        """
        type_params = self.types.get(fieldname, None)
        if type_params is not None:
            return type_params

        type_params = self.make_guess(fieldname, value)
        self.set(fieldname, *type_params)
        return type_params

    def make_guess(self, fieldname, value):
        """Guess the type and parameters for a fieldname, given a sample value.

        This may be subclassed to implement a different set of guessing rules.

        """
        # Always guess TEXT, with no parameters, for now.
        return (self.TEXT, {})

    def indexer(self, fieldname):
        """Get the indexer for a field.

        Raises KeyError if the field is not in the schema.

        """
        indexer = self._indexer_cache.get(fieldname)
        if indexer is None:
            type, params = self.get(fieldname)
            indexer = {
                self.TEXT: TextIndexer,
                self.BLOB: BlobIndexer,
            }[type](fieldname, params)
            self._indexer_cache[fieldname] = indexer
        return indexer

    def query_generator(self, fieldname):
        """Get a query generator for a field.

        Raises KeyError if the field is not in the schema.

        """
        generator = self._generator_cache.get(fieldname)
        if generator is None:
            type, params = self.get(fieldname)
            generator = {
                self.TEXT: TextQueryGenerator,
                self.BLOB: BlobQueryGenerator,
            }[type](fieldname, params)
            self._generator_cache[fieldname] = generator
        return generator

class BaseIndexer(object):
    """Base class of indexers.
    
    Subclasses should implement the __call__ method, taking a one parameter of
    a value in the field being indexed, and returning or yielding a sequence of
    terms to be indexed.

    """
    def __init__(self, fieldname, params):
        """Default initialiser - just stores the fieldname and parameters.

        """
        self.fieldname = fieldname
        self.params = params

class BaseQueryGenerator(object):
    """Base class of query generators.

    Subclasses should implement the __call__ method, taking one parameter of
    a value to search for in the field being indexed, and returning a Query
    subclass.

    """
    def __init__(self, fieldname, params):
        self.fieldname = fieldname
        self.params = params

class TextIndexer(BaseIndexer):
    def __call__(self, value):
        for term in value.split():
            yield self.fieldname + ':' + term

class TextQueryGenerator(BaseQueryGenerator):
    def __call__(self, value, default_op=None):
        terms = []
        for term in value.split():
            terms.append(self.fieldname + ':' + term)
        return queries.QueryTerms(terms, default_op=default_op)

class BlobIndexer(BaseIndexer):
    def __call__(self, value):
        yield self.fieldname + ':' + value

class BlobQueryGenerator(BaseQueryGenerator):
    def __call__(self, value):
        yield self.fieldname + ':' + value
    def __call__(self, value, *args, **kwargs):
        return queries.QueryTerms([self.fieldname + ':' + value],
                                  default_op=default_op)
