"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 02/21/2013
"
" Common routines for muskrat.
"
"""
from __future__ import absolute_import
import imp
import os

def config_loader( config ):
    """
    Loads the configuration from an either and external python file, python object, or dict.

    config
        A config of 3 possible types; python filepath, python object, or dict
        If the config parameter is a python file and is not an absolute path then the
        the load is attempted from this file's directory.
    """
    #Non-path filenames need to be resolved to point to the same directory as this file
    #as per our default config loading structure

    if isinstance( config, str ) and config.endswith( '.py' ):
        if os.path.basename( config ) == config:
            config = os.path.join( os.path.dirname( __file__ ), config )

        #Load config as a new module and place in this module's global scope
        d = imp.new_module('config')
        d.__file__ = config

        try:
            exec(compile(open(config, "rb").read(), config, 'exec'), d.__dict__)
        except IOError as e:
            e.strerror = 'Unable to load configuration file (%s)' % e.strerror
            raise
        
        return d.CONFIG

    elif isinstance( config, dict ):
        config_obj = type( 'Config', (object,), config )
        return config_obj

    elif isinstance( config, object ):
        return config
    
    else:
        raise TypeError('Config type not recognized')


