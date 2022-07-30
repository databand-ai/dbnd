# © Copyright Databand.ai, an IBM Company 2022

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import sys


root_doc = "reference/index"

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#

# sys.path.append(os.path.abspath("../modules/dbnd/src/dbnd/"))
sys.path.append("dbnd")


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc", "sphinx.ext.autosummary", "sphinx.ext.napoleon"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "build/*"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"  # Chooses the Alabaster theme
html_theme_options = {
    "show_powered_by": False
}  # Hides the "powered by ..." text at the bottom of the page.
html_show_sourcelink = False  # Hides the page source link, as it doesn't work on readme
html_sidebars = {"**": []}  # Makes Sphinx not generate the sidebars
html_permalinks = False
html_show_copyright = False  # Disables the copyright text.
html_show_sphinx = False

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]


def skip(app, what, name, obj, would_skip, options):
    if name == "__init__":
        return True


def setup(app):
    app.connect("autodoc-skip-member", skip)
