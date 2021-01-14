"""
" Copyright:    Loggly
" Author:       Scott Griffin
"
"""
from __future__ import absolute_import
from setuptools import setup
from muskrat import __version__

setup(
    name='Muskrat',
    author='Scott Griffin',
    author_email='scott@loggly.com',
    version=__version__,
    packages=['muskrat', 'muskrat.tests'],
    long_description=open( 'README.md' ).read(),
    install_requires=open( 'requirements.txt' ).read().split()
    )
