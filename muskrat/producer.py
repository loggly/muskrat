"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 01/28/2013
"
"""
from datetime import datetime, timedelta
try: import simplejson as json
except ImportError: import json

import pika
import boto

from config import ENV
if 'DEV' == ENV:
    from config import DevConfig as CONFIG
else:
    from config import Config as CONFIG

class BaseProducer(object):
    """
    Producer object that is meant to ease the sending of messages
    """
    def __init__( self, **kwargs ):
        """
        Create the producer.  General practive is to have a single routing_key for this producer,
        channel, connection.  This is for simplicity.  Multiple producers can be defined for the same
        routing key but are logically seperate in the code base.
        
        routing_key
            key for the data source to bind this producer to. Defaults to the config file.
        """
        self.routing_key = kwargs.get( 'routing_key' )
        if self.routing_key:
            self.routing_key = self.routing_key.upper()


    def send_json( self, obj ):
        """
        Dumps the object to json before sending the message.
        """
        
        #Handle datetime objects
        self.send( json.dumps( obj, default=lambda item: item.isoformat() if isinstance( item, datetime ) else None ) )


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
        self.parameters = pika.ConnectionParameters( host=CONFIG.host )
        self.conn = pika.BlockingConnection( self.parameters )
        self.channel = self.conn.channel()

        self.exchange = kwargs.get( 'exchange', CONFIG.exchange_name )
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
        self.s3conn = boto.connect_s3( CONFIG.s3_key, CONFIG.s3_secret )
        self.bucket = self.s3conn.get_bucket( CONFIG.s3_bucket )
    
    def send( self, msg, **kwargs ):
        """
        Sends the message with a producer and then attempt to write it to our s3 bucket.
        """
        rkey = kwargs.get( 'routing_key', self.routing_key )
        rkey = rkey.upper()
        try:
            s3key_name = self._create_key_name( rkey )
            s3key = self.bucket.new_key( key_name=s3key_name )
            s3key.set_contents_from_string( msg )
        except:
            raise 

    def _create_key_prefix( self, routing_key ):
        return routing_key.replace( '.', '/' )

    def _create_key_name( self, routing_key ):
        """
        Creates a key based on the routing key and a timestamp of the actual item.
        """
        return '/'.join( [self._create_key_prefix( routing_key ), datetime.today().strftime( CONFIG.s3_timestamp_format )] )

    def _set_lifecycle_policy( self, policy ):
        """
        Generates a lifecycle policy on the s3 bucket.
        """
        pass


class Producer( BaseProducer ):
    def __init__( self, s3=True, rabbitmq=False, **kwargs ):
        """
        Creates a generic producer that can use multiple interfaces for sending messages
        """
        self.brokers = []
        super( Producer, self ).__init__( **kwargs )

        if s3:
            self.brokers.append( S3Producer( **kwargs ) )
        elif rabbitmq:
            self.brokers.append( RabbitMQProducer( **kwargs ) )
        else:
            raise Exception( 'No defined middleman for the producer! Please select: s3 or rabbitmq' )

    def set_routing_key():
        pass

    def send( self, msg, **kwargs):
        """
        Send the message via all available producer brokers.
        """
        for broker in self.brokers:
            broker.send( msg, **kwargs )


# For Retreival use bucket.get_key( prefix='First/Second/Third' )
# Then filter items by splitting on the last '/' and date
