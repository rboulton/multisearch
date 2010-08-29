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
r"""Field type guessing.

"""
__docformat__ = "restructuredtext en"

from schema import Schema

class GuesserMetaClass(type):
    def __new__(meta, classname, bases, classDict):
        classDict = dict(classDict)
        classDict['_classname'] = classname
        return type.__new__(meta, classname, bases, classDict)

class Guesser(object):
    """Base class of field type guessers.

    """

    __metaclass__ = GuesserMetaClass

    def guess(self, fieldname, value):
        """Guess the field type and parameters for a field of a given name,
        and a sample value.

        Guessers should return a dictionary of parameters for handling the
        given field, or None if it has no guess to make (which allows later
        guessers to make a guess).

        """
        raise NotImplementedError

    def serialise(self):
        return self._classname + self.guess

class TextGuesser(Guesser):
    """A guesser for a field type, which always guesses TEXT.

    """
    def guess(self, fieldname, value):
        return dict(type=Schema.TEXT)

class ExtensionGuesser(object):
    """A guesser for a field type, which guesses based on the extension.

    """
    def guess(self, fieldname, value):
        ext = fieldname.rsplit('_', 2)
