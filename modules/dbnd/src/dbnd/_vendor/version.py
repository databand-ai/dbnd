


def parse(version):
    """
    Parse the given version string
    If setuptools are available, it will return Version/LegacyVersion, otherwise LegacyVersion from distutils
    """
    try:
        from setuptools.extern.packaging.version import parse as setuptools_parse
        return setuptools_parse(version)
    except ImportError:
        try:
            # setuptools is an optional package. although distutils is going to be deprecated in py3.12+
            from distutils import version
            return version.LooseVersion(version)
        except ImportError:
            raise Exception("distutils and setuptools are not installed, "
                            "please install one of them, or de-install `dbnd` package")

