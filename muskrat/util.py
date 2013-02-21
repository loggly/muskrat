"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 02/20/2013
"
" Common routines for muskrat.
"
"""
import imp
import os

def config_loader( filename ):
    """
    Loads the configuration from an external python file.  In order for the configuration to
    be loaded successfully the file needs to define a module leve variable 'CONFIG' that
    points to the configuration class.

    filename
        The name of the python configuration file.  If a terminal filename is given, such as
        'dev_config.py' then the default behavior is to look in the folder where this file
        exists for a match.
    """
    #Non-path filenames need to be resolved to point to the same directory as this file
    #as per our default config loading structure
    if os.path.basename( filename ) == filename:
        config = os.path.join( os.path.dirname( __file__ ), filename )

    #Load config as a new module and place in this module's global scope
    d = imp.new_module('config')
    d.__file__ = config

    try:
        execfile(config, d.__dict__)
    except IOError, e:
        e.strerror = 'Unable to load configuration file (%s)' % e.strerror
        raise
    
    return d.CONFIG
