import setuptools


setuptools.setup(
    version="0.1",
    name="dbnd-test-package",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=[],
    include_package_data=False,
)
