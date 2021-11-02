class TargetError(Exception):
    """
    Base exception.
    """

    pass


class FileSystemException(TargetError):
    """
    Base class for generic file system exceptions.
    """

    pass


class FileAlreadyExists(FileSystemException):
    """
    Raised when a file system operation can't be performed because
    a directory exists but is required to not exist.
    """

    pass


class FailedToCopy(FileSystemException):
    """
    Raised when a copy operation failed
    """

    pass


class MissingParentDirectory(FileSystemException):
    """
    Raised when a parent directory doesn't exist.
    (Imagine mkdir without -p)
    """

    pass


class NotADirectory(FileSystemException):
    """
    Raised when a file system operation can't be performed because
    an expected directory is actually a file.
    """

    pass


class FileNotFoundException(FileSystemException):
    pass


class NotSupportedValue(TargetError):
    pass


class InvalidDeleteException(FileSystemException):
    pass
