"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 01/15/2013
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

class ChannelNotFound( Exception ):
    pass

class ConsumerPool(object):
    """
    Consumer object the holds a connection to rabbitmq, and all channels that message
    consumers are subscriped to queues on.

    This class gets most of it's benefit by providing a decorator function that allows functions
    to register with this consumer object on specificed routing_keys.
    """
    def __init__(self):
        self.channels = {}

        self._conn_params = pika.ConnectionParameters( CONFIG.host )

        self._exchange_name = CONFIG.exchange_name
        self._exchange_type = CONFIG.exchange_type
    

    def connect( self ):
        """
        Creates a connectino with rabbitmq.
        """
        self._connection = pika.BlockingConnection( parameters = self._conn_params, 
                                                    on_open_callback=self._on_connection_open )

        self._connection.add_on_close_callback( self._on_connection_closed )


    def _gen_channel_name(self, func):
        """
        Generates a channel name based off the function that was registered as a consumer.
        This is attempted to be as unique as possible, because in the current implementation there
        is only one channel and one queue per registered function.
        """
        return '.'.join( (getattr(func, '__module__', '' ), func.__name__) )
    

    def _on_connection_closed(self, method_frame):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        method_frame
            RabbitMQ method fram passed ot this callback
        """
#        LOGGER.warning('Server closed connection, reopening: (%s) %s',
#                       method_frame.method.reply_code,
#                       method_frame.method.reply_text)
        self._connection = self.connect()


    def _on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
#        LOGGER.info('Connection opened')
        self.reconnect_channels()


    def reconnect_channel( self, name ):
        """
        Reconnect a single channel with the supplied name.  This creates a new channel (the old one
        should have died) and binds it to the queue that matches that channel name.
        """
        #this needs to be able to get a channel from the connection and then
        #re-bind it to a specified queue
        self.register_consumer( self.channels[ name ][ 'callback' ], self.channels[ name ]['routing_key'] )


    def reconnect_channels( self ):
        """
        Reconnects all channels in the consumer pool.
        """
        for name in self.channels:
            self.reconnect_channel( name )

    def close_connection(self):
        """
        Gracefully closes the connection with the rabbitmq server.  This closes this connection
        as well as all the channels in the pool.
        """
        self._connection.close()


    def kill_consumer(self, kill_queue=False):
        """
        This method kills a consumer and optionally complete destroys it's queue on the rabbitmq server so 
        no messages can be pushed into it.
        """
        raise NotImplementedError


    def start_consumers(self, name=None):
        """
        Starts channels for the specified name.  The name is mapped to the function name that 
        was registered

        Defaults to start consuming on all names if a name is not specified.

        name
            Name of the generated consumer channel.
        """
        if not name:
            for name in self.channels:
                self.channels[name]['channel'].start_consuming()
        else:
            if not name in self.channels:
                raise ConsumerNameError( 'Consumer named %s does not exist' % name )
            else:
                self.channels[name]['channel'].start_consuming()


    def start_consumer_func(self, func):
        """
        Starts the supplied consumer function.  This consumer function must be registered with this object
        (through a decorator) prior to calling this function.

        func
            the function to start consuming on.
        """
        self.start_consumers( name=self._gen_channel_name( func ) )


    def channel_exists(self, name):
        """
        Returns true if an active channel exists (known and open), otherwise it returns false.
        """
        if name in self.channels:
            return self.channels[name]['channel'].is_open
        else:
            return False

    def get_consumer_channel_by_func(self, func):
        """
        Returns the consumer channel based on the registered function.

        func
            The function that the consumer was registered with.
        """
        return self.get_consumer_channel( self._gen_channel_name( func ) )

    def get_consumer_channel(self, name):
        """
        Retuns the consumer channel based on the registered name.

        name
            The name of the registered consumer.
        """
        if name in self.channels:
            return self.channels[name]['channel']
        else:
            raise ChannelNotFound( '%s is not a known channel name' % name )

    def register_consumer(self, func, routing_key ):
        """
        Sets up all items we need for a channel to start consuming based on the name of the func and the routing_key.
        """
        channel = self._connection.channel()
        channel.exchange_declare( exchange=self._exchange_name, type=self._exchange_type )

        #Make a name for this queue that can be re-attached to at a later point in the case that the
        #managing consumer dies.  Also, for our own management, attach this name to the channel we
        #will be using to communicate with
        channel_name = self._gen_channel_name( func )
        if self.channel_exists( channel_name ):
            #TODO - Close the channel and re-map everything
            raise NotImplementedError

        queue = channel.queue_declare( queue=channel_name )
        channel.queue_bind( exchange=self._exchange_name, 
                            queue=queue.method.queue, 
                            routing_key=routing_key)

        #Setup our callback as this function
        channel.basic_consume( func, queue=queue.method.queue )

        #Store this information so that we have it again in the case that the channel is closed unexpectedly and we
        #need to re-construct this interface
        self.channels[ channel_name ] = {'channel':channel, 'queue':queue, 'routing_key':routing_key, 'callback':func }


    def consumer(self, routing_key):
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
            self.register_consumer( func, routing_key )

            @wraps(func)
            def wrapper(*args, **kwargs):
                #Because we are going to register this as a callback, do not modify the funciton in any way.
                #This will allow us to test the functions with mock data.
                return func(*args, **kwargs)
            return wrapper
        return decorator
