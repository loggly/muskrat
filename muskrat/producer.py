"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 02/20/2013
"
"""
try: import simplejson as json
except ImportError: import json

import Queue
import threading
from   datetime   import datetime

import pika
import boto
from   muskrat.util import config_loader

class BaseProducer(object):
    """
    Producer object that is meant to ease the sending of messages
    """
    def __init__( self, config='config.py', **kwargs ):
        """
        Create the producer.  General practive is to have a single routing_key for this producer,
        channel, connection.  This is for simplicity.  Multiple producers can be defined for the same
        routing key but are logically seperate in the code base.
        
        routing_key
            key for the data source to bind this producer to. Defaults to the config file.
        config
            Configuration file if not defined in muskrat.config.py.
        """
        self.config = config_loader( config )

        self.routing_key = kwargs.get( 'routing_key' )
        if self.routing_key:
            self.routing_key = self.routing_key.upper()

    def send( self, msg ):
        """ Class interface method.  Needs to be implemented by children """
        raise NotImplementedError

    def send_json( self, obj ):
        """ Dumps the object to json before sending the message.  """

        #Handle datetime objects
        self.send( json.dumps( obj, default=lambda item: item.strftime(self.config.timeformat) if isinstance( item, datetime ) else None ) )


class RabbitMQProducer( BaseProducer ):

    def __init__(self, **kwargs):
        """
        Create the producer.  General practive is to have a single routing_key for this producer,
        channel, connection.  This is for simplicity.  Multiple producers can be defined for the same
        routing key but are logically seperate in the code base.
        
        routing_key
            key for the data source to bind this producer to. Defaults to the config file.
        exchange
            name of the exchange to send messages to. Defaults to the config file.
        """
        self.parameters = pika.ConnectionParameters( host=self.config.host )
        self.conn = pika.BlockingConnection( self.parameters )
        self.channel = self.conn.channel()

        self.exchange = kwargs.get( 'exchange', self.config.exchange_name )
        super( RabbitMQProducer, self ).__init__( **kwargs )
    
    def send( self, msg, **kwargs ):
        """
        Sends the actual message to the broker.
        """
        key = kwargs.get( 'routing_key', self.routing_key )
        key = key.upper()
        self.channel.basic_publish( exchange=self.exchange, routing_key=key, body=msg )


class S3Producer( BaseProducer ):
    """
    Producer object that also mirrors all writes to S3 was well as the topic.
    """

    def __init__(self, **kwargs):
        super( S3Producer, self ).__init__(**kwargs)
        self.s3conn = boto.connect_s3( self.config.s3_key, self.config.s3_secret )
        self.bucket = self.s3conn.get_bucket( self.config.s3_bucket )
    
    def send( self, msg, **kwargs ):
        """
        Actually sends the message to our s3 bucket.
        """
        rkey = kwargs.get( 'routing_key', self.routing_key )
        rkey = rkey.upper()
        try:
            s3key_name = self._create_key_name( rkey )
            s3key = self.bucket.new_key( key_name=s3key_name )
            self._send( msg, s3key )
        except:
            raise 

    def _send( self, msg, s3key):
        """
        Actually writes the message. Meant to be overridden for extensibility.
        """
        s3key.set_contents_from_string( msg )

    def _create_key_prefix( self, routing_key ):
        return routing_key.replace( '.', '/' )

    def _create_key_name( self, routing_key ):
        """
        Creates a key based on the routing key and a timestamp of the actual item.
        """
        return '/'.join( [self._create_key_prefix( routing_key ), datetime.today().strftime( self.config.s3_timestamp_format )] )

    def _set_lifecycle_policy( self, policy ):
        """
        Generates a lifecycle policy on the s3 bucket.
        """
        raise NotImplementedError


class S3WriteThread( threading.Thread ):
    """
    Actual thread that can write to S3.
    """
    def __init__(self, queue, timeout=1):
        super(S3WriteThread, self).__init__()
        self.queue = queue
        self.timeout = timeout

    def run(self):
        try:
            #We want to continually process the queue if items are available
            while True:
                msg, s3key = self.queue.get( True, self.timeout )
                s3key.set_contents_from_string( msg )
                self.queue.task_done()
        except Queue.Empty:
            #queue.get() timeout will cause this exception and mean we are done
            #with our messages, so exit the thread.
            pass


class ThreadedS3Producer( S3Producer ):
    """
    Creates a thread pool that allows for concurrent issuing of S3 requests.
    This allows us to not have to block on network bound I/O.
    This will create a pool that is limited to the number of threads defined.
    Each thread has the potential to block for at least 1 second on cleanup.

    Defaults to a thread pool of 20 threads.
    """
    def __init__(self, *args, **kwargs):
        self.queue = Queue.Queue()
        self.num_threads = kwargs.pop( 'num_threads', 20 )
        self.threads = []
        super( ThreadedS3Producer, self ).__init__( **kwargs )


    def _start(self):
        """
        Starts the threads if they are not already running.
        """
        if not self.threads:
            for i in range( self.num_threads ):
                t = S3WriteThread( self.queue )
                self.threads.append( t )
                t.start()
        else:
            for i, t in enumerate( self.threads ):
                if not t.isAlive():
                    try:
                        #Threads can only be started once.  In the case that
                        #our thread *appears* to have finished before all of
                        #our data is proccessed (Queue was temporarily empty)
                        #Lets replace that thread with a new one and keep
                        #keep proccessing
                        replacement_thread = S3WriteThread( self.queue )
                        self.threads[ i ] = replacement_thread
                        replacement_thread.start()
                    except RuntimeError:
                        #Nothing we can do here.. keep chugging along with
                        #the threads that exist
                        pass

    def _send(self, msg, s3key):
        #Our _start should guarantee that this is always atleast
        #one thread in our pool that is active and ready to procces
        self.queue.put( (msg, s3key) ) 
        self._start()

    def join( self ):
        """ Blocks until all messages have been processed/sent """
        self.queue.join()



class Producer( BaseProducer ):
    def __init__( self, brokers=None, **kwargs ):
        """
        Creates a generic producer that can use multiple interfaces for sending messages

        brokers
            a list of Producer classes to create a broker for.  All supplied keyword 
            arguments are forwarded to the __init__ method of the class upon instantiation.
        """
        super(Producer, self).__init__(**kwargs)

        #Default to S3
        if not brokers:
            brokers = [S3Producer]

        self.brokers = []

        for broker in brokers:
            self.brokers.append( broker( **kwargs ) )

    def send( self, msg, **kwargs):
        """
        Send the message via all available producer brokers.
        """
        for broker in self.brokers:
            broker.send( msg, **kwargs )

