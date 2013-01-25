"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 01/24/2013
"
" This class provides the ability to register a function as 
" a consumer to an S3 topic.  This class also handles tracking
" that consumer's state.
"
"""
import os
import time

import boto

from config import ENV
if 'DEV' == ENV:
    from config import DevConfig as CONFIG
else:
    from config import Config as CONFIG


class S3Cursor(object):
    def __init__(self, name, atype='file'):
        self.name = name
        self.current = None
        self.type = atype

        if self.type == 'file':
            self.filename = os.path.join( CONFIG.s3_cursor['location'], name )
            self._update_func = self._update_file_cursor
            self._get_func = self._get_file_cursor

    def _update_file_cursor( self, key ):
        #instead of opening and re-opening we could just seek and truncate
        try:
            file = open( self.filename, 'w+' )
        except IOError:
            os.makedirs( os.path.dirname( self.filename ) )
            file = open( self.filename, 'w+' )

        file.write( key )
        file.close()

    def _get_file_cursor( self ):
        try:
            file = open( self.filename, 'r' )
            cursor = file.readline()
            file.close()
        except IOError as e:
            return None
        return cursor

    def update( self, key ):
        self._update_func( key ) 

    def get( self ):
        return self._get_func()


class S3Consumer(object):

    def __init__(self, topic, func, name=None):
        self.s3conn = boto.connect_s3( CONFIG.s3_key, CONFIG.s3_secret )
        self.bucket = self.s3conn.get_bucket( CONFIG.s3_bucket )
        self.topic = topic.upper()
        self.callback = func
        self._cursor = S3Cursor( self._gen_name( func ), atype=CONFIG.s3_cursor['type'] )

        if not name:
            self.name = self._gen_name( self.callback )
        else:
            self.name = name
    
    def _gen_name(self, func):
        return func.__module__ + '.' + func.__name__

    def _gen_routing_key( self, topic ):
        return topic.replace( '.', '/' )

    def consume(self):
        #If marker is not matched to a key then the returned list is none.
        messages = self.bucket.get_all_keys( 
                            prefix=self._gen_routing_key( self.topic ), 
                            marker=self._cursor.get() 
                        )

        for msg in messages:
            self.callback( msg.get_contents_as_string() )
            self._cursor.update( msg.name )


    def consumption_loop( self, interval=2 ):
        """
        Consumes as many messages as there are available for this object key.  Busy polls the 
        server and checks for new messages every 2 seconds if there are no messages.
        """
        try:
            while True:
                time.sleep( interval )
                self.consume()
        except KeyboardInterrupt:
            pass
        except:
            raise

        
def Consumer( routing_key ):
    """
    Decorator function that will attach the decorated function to a RabbitMQ queue
    defined for the specified routing key.

        example:
        cons = Consumer()
        @cons.consume( 'Frontend.Customer.Test' )
        def printMessage(body):
            print '%s' % body

    routing_key
        The key defining the messages that the consumer will subscribe to.
    """
    def decorator(func):
        s3consumer = S3Consumer( routing_key, func )

        #Attach the consumer to this callback function
        func.consumer = s3consumer
        return func
    return decorator

#BETER decorator that supports both functions and methods!
#class Consumer(object):
#    def __init__(self, func):
#        self.func = func
#    def __get__(self, obj, type=None):
#        return self.__class__(self.func.__get__(obj, type))
#    def __call__(self, *args, **kw):
#        return self.func(*args, **kw)
