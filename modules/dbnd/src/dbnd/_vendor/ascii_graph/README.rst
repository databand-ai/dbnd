py-ascii-graph
==============

A simple python lib to print data as ascii histograms

.. image:: https://secure.travis-ci.org/kakwa/py-ascii-graph.png?branch=master
        :target: http://travis-ci.org/kakwa/py-ascii-graph
        :alt: Travis CI
    
.. image:: https://img.shields.io/pypi/v/ascii_graph.svg
    :target: https://pypi.python.org/pypi/ascii_graph
    :alt: PyPI version

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/kakwa/py-ascii-graph
   :target: https://gitter.im/kakwa/py-ascii-graph?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

.. image:: https://coveralls.io/repos/kakwa/py-ascii-graph/badge.svg?branch=master 
    :target: https://coveralls.io/r/kakwa/py-ascii-graph?branch=master

.. image:: https://readthedocs.org/projects/py-ascii-graph/badge/?version=latest
    :target: http://py-ascii-graph.readthedocs.org/en/latest/?badge=latest
    :alt: Documentation Status

.. .. image:: https://img.shields.io/pypi/pyversions/ascii_graph.svg
..    :target: https://pypi.python.org/pypi/ascii_graph
..    :alt: Supported Python Versions

----

:Git: `Github <https://github.com/kakwa/py-ascii-graph>`_
:PyPI: `Package <https://pypi.python.org/pypi/ascii_graph>`_
:Doc: `Documentation <http://py-ascii-graph.readthedocs.org>`_
:License: MIT
:Author: Pierre-Francois Carpentier - copyright 2014

----

License
=======

py-ascii-graph is released under the MIT License.

Description
===========

py-ascii-graph is a simple python library to build ascii histograms. 
Just give it a label and a list of tuples (description, value) 
and it will automaticaly creates a nice histogram, 
with all the stuff aligned and fitting in a fixed width line (if possible).

py-ascii-graph although comes with a command line utility.

Examples
========

Library
-------

Simple example:

.. sourcecode:: python

    from ascii_graph import Pyasciigraph

    test = [('long_label', 423), ('sl', 1234), ('line3', 531), 
        ('line4', 200), ('line5', 834)]

    graph = Pyasciigraph()
    for line in  graph.graph('test print', test):
        print(line)

Result:

.. sourcecode:: bash

    test print
    ###############################################################################
    ████████████████████                                            423  long_label
    █████████████████████████████████████████████████████████████  1234  sl        
    ██████████████████████████                                      531  line3     
    █████████                                                       200  line4     
    █████████████████████████████████████████                       834  line5

Complex examples (colors, different spacing, no label...):

.. sourcecode:: python

    from ascii_graph import Pyasciigraph
    from ascii_graph.colors import *
    from ascii_graph.colordata import vcolor
    from ascii_graph.colordata import hcolor
    
    test = [('long_label', 423), ('sl', 1234), ('line3', 531),
        ('line4', 200), ('line5', 834)]
    
    # One color per line
    print('Color example:')
    pattern = [Gre, Yel, Red]
    data = vcolor(test, pattern)
    
    graph = Pyasciigraph()
    for line in graph.graph('vcolor test', data):
        print(line)
    
    # Multicolor on one line
    print('\nMultiColor example:')
    
    # Color lines according to Thresholds
    thresholds = {
      51:  Gre, 100: Blu, 350: Yel, 500: Red,
    }
    data = hcolor(test, thresholds)
    
    # graph with colors, power of 1000, different graph symbol,
    # float formatting and a few tweaks
    graph = Pyasciigraph(
        line_length=120,
        min_graph_length=50,
        separator_length=4,
        multivalue=False,
        human_readable='si',
        graphsymbol='*',
        float_format='{0:,.2f}',
        force_max_value=2000,
        )
    
    for line in graph.graph(label=None, data=data):
        print(line)

Command Line Utility
--------------------

command line:

.. sourcecode:: bash

    $ asciigraph -h
    Usage: asciigraph [-l <label>] [-f file] [-s inc|dec] \
       [-c] [-t <first color threshold> [-T <second color threshold>] \
       [-w <number of char>] [-m <min len of char>] [-H] [-M cs|si]
    
    examples:
       printf 'label1:10\nlabel2:100\n' | asciigraph -l 'my graph'
       printf 'label1:1000\nlabel2:20000\n' | asciigraph -l 'my graph' -H -M 'si'
       printf 'l1:100\nl2:1200.42\n' > ./mf; asciigraph -l 'my graph' -f ./mf
       asciigraph -l 'my graph' -f mf -s inc
       asciigraph -l 'my graph' -f mf -s dec -w 60 -m 10
       asciigraph -l 'my graph' -f mf -c -F '{0:,.2f}'
       asciigraph -l 'my graph' -f mf -c -t 5 -T 50
    
    
    Options:
      -h, --help            show this help message and exit
      -f FILE, --file=FILE  import data from FILE (one data per line,
                            format: <label>:<value>)
      -s SORT, --sort=SORT  sort type: inc (increasing) or dec (decreasing)
      -l LAB, --label=LAB   label of the graph
      -w WIDTH, --width=WIDTH
                            width of the graph
      -m LEN, --min_graph=LEN
                            minimum length of the graph bar
      -c, --color           Color the graph
      -t TC1, --threshold-1=TC1
                            first color threshold, only make sense if --color is
                            passed
      -T TC2, --threshold-2=TC2
                            second color threshold, only make sense if --color is
                            passed
      -H, --human-readable  enable human readable mode (K, M, G, etc)
      -M HR_MODE, --human-readable-mode=HR_MODE
                            Human readable mode ('cs' -> power of 1024 or 'si' ->
                            power of 1000, default: cs)
      -F FORMAT, --float-format=FORMAT
                            float formatting, ex: {0:,.2f}


See the examples/ directory for more examples.

Installation
============

.. sourcecode:: bash 

    $ pip install ascii_graph

or

.. sourcecode:: bash

    $ easy_install ascii_graph


