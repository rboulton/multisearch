class ClosedBackend(object):
    """A perpetually closed search backend.
    
    This is a "dummy" backend class, used to replace the real backend when the
    client is closed, so that the real backend will never be accessed once the
    client has closed it.

    """
    def close(self):
        """Close an already closed backend - no error.

        """
        pass

    def __getattr__(self, name):
        """All other attributes are functions which raise an error.

        """
        def raiseerror():
            raise errors.DbClosedError("This database has already been closed")
        return raiseerror
