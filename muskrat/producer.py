"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 01/10/2013
"
"""
import json


import pika
from config import Config as CONFIG

class Producer(object):
    """
    Producer object that is meant to ease the sending of messages
    """

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

        self.routing_key = kwargs.get( 'routing_key' )
        self.exchange = kwargs.get( 'exchange', CONFIG.exchange_name )

    def send( self, msg, routing_key=None ):
        """
        Sends the actual message to the broker.
        """
        key = self.routing_key if not routing_key else routing_key
        self.channel.basic_publish( exchange=self.exchange, routing_key=key, body=msg )

    def send_json( self, obj ):
        """
        Dumps the object to json before sending the message.
        """
        self.send( json.dumps( obj ) )
