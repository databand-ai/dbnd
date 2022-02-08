import os


def abs_join(*path):
    return os.path.abspath(os.path.normpath(os.path.join(*path)))


def relative_path(file_in_path, *path):
    """
    Returns absolute path of a local file/directory.

    Args:
        file_in_path: Local file.
        *path: The path you want to turn into an absolute path.

    Returns: Absolute path of *path.
    """
    current_dir = os.path.dirname(os.path.abspath(file_in_path))
    return abs_join(current_dir, *path)


def relative_path_directory(file_in_path, *path):
    current_dir = os.path.abspath(file_in_path)
    return abs_join(current_dir, *path)
