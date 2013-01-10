"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 01/10/2013
"
" This class defines a consumer singleton that can be used to provide the
" underlying RabbitMQ connection interface for receiving messages.
"
"""
from collections import defaultdict
from functools import wraps
import exceptions

import pika
from config import Config as CONFIG


class ConsumerNameError( Exception ):
    pass

class BlockingConsumer(object):
    """
    Consumer object the holds a connection to rabbitmq, and all channels that message
    consumers are subscriped to queues on.

    This class gets most of it's benefit by providing a decorator function that allows functions
    to register with this consumer object on specificed routing_keys.
    """
    conn_params = pika.ConnectionParameters( CONFIG.host )
    conn = pika.BlockingConnection( conn_params )
    exchange_name = CONFIG.exchange_name
    exchange_type = CONFIG.exchange_type


    def __init__(self):
        self.channels = defaultdict( list )


    def start_channels(self, name=None):
        """
        Starts channels for the specified name.  The name is mapped to the function name that 
        was registered

        Defaults to start consuming on all names if a name is not specified.
        """
        if not name:
            for name in self.channels:
                for channel in self.channels[name]:
                    channel.start_consuming()
        else:
            if not name in self.channels:
                raise ConsumerNameError( 'Consumer named %s does not exist' % name )
            else:
                for channel in self.channels[ name ]:
                    channel.start_consuming()


    def start_consumer_func(self, func):
        self.start_channels( func.__name__ )


    def consume(self, routing_key):
        """
        Decorator function that will attach the decorated function to a RabbitMQ queue
        defined for the specified routing key.

            example:
            cons = Consumer()
            @cons.consume( 'Frontend.Customer.Test' )
            def printMessage(channel, method, header, body):
                print '%s' % channel
                print '%s' % method
                print '%s' % header
                print '%s' % body

        routing_key
            The key defining the messages that the consumer will subscribe to.
        """
        def decorator(func):
            channel = Consumer.conn.channel()
            channel.exchange_declare( exchange=Consumer.exchange_name, type=Consumer.exchange_type )

            #Make a name for this queue that can be re-attached to at a later point in the case that the
            #managing consumer dies.
            queue = channel.queue_declare( queue= '.'.join( getattr( func, '__module__', '' ), func.__name__ ) )
            channel.queue_bind(exchange=Consumer.exchange_name, queue=queue.method.queue, routing_key=routing_key)
            #Setup our callback as this function
            channel.basic_consume( func, queue=queue.method.queue )
            self.channels[ func.__name__ ].append( channel )

            @wraps(func)
            def wrapper(*args, **kwargs):
                #Because we are going to register this as a callback, do not modify the funciton in any way.
                #This will allow us to test the functions with mock data.
                return func(*args, **kwargs)
            return wrapper
        return decorator
