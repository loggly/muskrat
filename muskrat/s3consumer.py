"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 02/22/2013
"
" This class provides the ability to register a function as 
" a consumer to an S3 routing_key.  This class also handles tracking
" that consumer's state.
"
"""
import os
import time

import boto
from   muskrat.util import config_loader

class S3Cursor(object):
    def __init__(self, name, type, **kwargs ):
        """
        Creates a cursor object to track messages consumed. Defaults to a file based cursor
        existing in a cursors directory at the same path 
        
        name
            name of the cursor
        config
            a dictionary containing cursor type and type specific config items.

            example: {'type':'file', 'location': os.path.dirname( __file__ ) }
        """
        self.name = name
        self.current = None
        self.type = type
        
        #TODO - handle multiple cursor types such as row injection into a DB.
        if self.type == 'file':
            #Default to the same directory as this file
            path = kwargs.get( 'location', os.path.dirname( __file__ ) )
            self.filename = os.path.join( path, name )
            self._update_func = self._update_file_cursor
            self._get_func = self._get_file_cursor
        else:
            raise NotImplementedError('File cursor types currently the only types supported')


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

    def __init__(self, routing_key, func, name=None, config='config.py'):

        self.config = config_loader( config )
        self.s3conn = boto.connect_s3( self.config.s3_key, self.config.s3_secret )
        self.bucket = self.s3conn.get_bucket( self.config.s3_bucket )
        self.routing_key = routing_key.upper()
        self.callback = func

        if not name:
            self.name = self._gen_name( self.callback )
        else:
            self.name = name

        self._cursor = S3Cursor( 
                           self.name, 
                           type=self.config.s3_cursor['type'],
                           location=self.config.s3_cursor['location']
                       )
                                 
    
    def _gen_name(self, func):
        """ Generates a cursor name so that the cursor can be re-attached to """
        return func.__module__ + '.' + func.__name__

    def _gen_routing_key( self, routing_key ):
        return routing_key.replace( '.', '/' )

    def _get_msg_iterator(self):
        #If marker is not matched to a key then the returned list is none.
        msg_iterator = self.bucket.list( 
                            prefix=self._gen_routing_key( self.routing_key ) + '/', 
                            delimiter= '/',
                            marker=self._cursor.get() 
                        )

        return msg_iterator

    def consume(self):
        #TODO - If the bucket is created, but no keys exist... this
        #attempts to do something. We should probably explicitly check for this.
        #Update: actually... this doesn't seem to be a problem...
        msg_iterator = self._get_msg_iterator()

        for msg in msg_iterator:
            #Sub 'directories' are prefix objects, so ignore them
            if isinstance( msg, boto.s3.key.Key ):
                self.callback( msg.get_contents_as_string() )
                self._cursor.update( msg.name )


    def consumption_loop( self, interval=2 ):
        """
        Consumes as many messages as there are available for this object key.  Busy polls the 
        server and checks for new messages every 2 seconds if there are no messages.
        """
        try:
            while True:
                self.consume()
                time.sleep( interval )
        except KeyboardInterrupt:
            pass
        except:
            raise


class S3AggregateConsumer( S3Consumer ):
    """
    A consumer that does not consume messages in single msg order.  Rather,
    this class retrieves all messages present when consume is called and issues 
    the callback with a list of messages.
    """

    def consume( self ):
        msg_iterator = self._get_msg_iterator()

        cursor = None
        messages = []
        for msg in msg_iterator:
            if isinstance( msg, boto.s3.key.Key ):
                messages.append( msg.get_contents_as_string() )
                cursor = msg.name

        if messages:
            self.callback( messages )
            self._cursor.update( cursor )


        
def Consumer( routing_key, aggregate=False, **kwargs):
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
        if not aggregate:
            s3consumer = S3Consumer( routing_key, func, **kwargs )
        else:
            s3consumer = S3AggregateConsumer( routing_key, func, **kwargs )

        #Attach the consumer to this callback function
        func.consumer = s3consumer
        return func
    return decorator
