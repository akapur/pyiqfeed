# coding=utf-8

"""
Exceptions thrown by pyiqfeed that are specific to pyiqfeed


"""


class NoDataError(KeyError):
    """Raised when a DTN function returns No Data."""
    pass


class UnauthorizedError(RuntimeError):
    """Raised if you make a request for data you are not authorized for."""
    pass


class UnexpectedField(BaseException):
    """
    Raised when we get a field with a value we don't know how to process.

    Deliberately derived from BaseException because this exception should
    not be caught and should result in a crash instead.

    """
    pass


class UnexpectedMessage(BaseException):
    """
    Raised when we get a message we don't know how to process.

    Deliberately derived from BaseException because this exception should
    not be caught and should result in a crash instead.

    """
    pass


class UnexpectedProtocol(BaseException):
    """
    Raised when IQFeed's protocol does not match the library's protocol.

    Deliberately derived from BaseException because this exception should
    not be caught and should result in a crash instead.

    """
    pass
