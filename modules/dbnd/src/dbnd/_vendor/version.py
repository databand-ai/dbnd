


def parse(version_to_parse):
    """
    Parse the given version string
    If setuptools are available, it will return Version/LegacyVersion, otherwise LegacyVersion from distutils
    """
    try:
        from packaging.version import parse as packaging_parse
        return packaging_parse(version_to_parse)
    except ImportError:
        try:
            # setuptools is an optional package. although distutils is going to be deprecated in py3.12+
            from distutils import version
            return version.LooseVersion(version_to_parse)
        except ImportError:
            raise Exception("distutils and setuptools are not installed, "
                            "please install one of them, or de-install `dbnd` package")

