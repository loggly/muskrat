"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 01/14/2013
"
"""
from setuptools import setup

setup(
    name='Muskrat',
    author='Scott Griffin',
    author_email='scott@loggly.com',
    version='0.1dev',
    packages=['muskrat', 'muskrat.tests'],
    long_description=open( 'README.md' ).read(),
    install_requires=open( 'requirements.txt' ).read().split()
    )
