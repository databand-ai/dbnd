


def parse(version):
    """
    Parse the given version string
    If setuptools are available, it will return Version/LegacyVersion, otherwise LegacyVersion from distutils
    """
    try:
        from setuptools.extern.packaging.version import parse as setuptools_parse
    except ImportError:
        # setuptools is an optional package. although distutils is going to be deprecated in py3.12+
        from distutils import version
        return version.LooseVersion(version)

    return setuptools_parse(version)

