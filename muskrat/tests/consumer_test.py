"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 01/15/2013
"
"""
import unittest
from os import path
import json

from .. import consumer
from .. import producer

import pika

class TestConsumerBase( unittest.TestCase ):

    def setUp(self):
        #Setup a consumer pool
        #Add multiple function definitions
        self.routing_key = 'Muskrat.Queue.Tests' 
        self.cp = consumer.ConsumerPool()
        self.cp.connect()
        self.module = self.__module__

        @self.cp.consumer( self.routing_key )
        def testconsumer(channel, method, header, body ): pass

        self.test_consumer = testconsumer

class TestConsumerPool( TestConsumerBase ):
    
    def test_consumer_pool_exists( self ):
        """ ConsumerPool object was constructed """
        self.assertIsNotNone( self.cp, 'Consumer pool does not exist.  Was not created' )

    def test_connection(self):
        """ Connection with RabbitMQ successfully made """
        self.assertEqual( self.cp._connection.is_open, True, 'RabbitMQ connection not established' )


class TestChannelCreation( TestConsumerBase ):

    def test_channels_exist(self):
        """ Channels exist in the consumer pool """
        self.assertIsNotNone( self.cp.channels, 'Consumer pool channels is empty')

    def test_channel_names(self):
        """ Generating channel names for lookup based on consumer function """
        def simplefunc(): pass
        self.assertEqual( self.cp._gen_channel_name( simplefunc ), 
                          '%s.simplefunc' % self.module, 
                          'Channel name was generated incorrectly' )

    def test_registration_decorator(self):
        """ Channel registered with decorator """
        @self.cp.consumer( self.routing_key )
        def test_decorator( channel, method, header, body ): pass
        self.assertIn( '%s.testconsumer' % self.module, self.cp.channels, 'Consumer name not registered with pool' )

    def test_registration_function(self):
        """ Register a channel manually """
        #Manual consumer
        def manual_consumer( channel, method, header, body ): pass
        func = manual_consumer
        routing_key = 'Manual.Consumer.Route'
        self.cp.register_consumer( func, routing_key )
        self.assertIn( '%s.manual_consumer' % self.module, self.cp.channels, 'Could not manually register consumer' )
        self.assertEqual( self.cp.channels['%s.manual_consumer' % self.module]['routing_key'], routing_key )


class TestChannelRetrieval( TestConsumerBase ):

    def test_channel_retrieval_by_name(self):
        """ Retrieve channel by name """
        chan = self.cp.get_consumer_channel( 'muskrat.tests.consumer_test.testconsumer' )
        self.assertIsNotNone( chan, 'Could not retrieve channel by consumer function' ) 
        self.assertIsInstance( chan, pika.adapters.blocking_connection.BlockingChannel )

    def test_channel_retrieval_by_func(self):
        """ Retrieve channel by defined function """
        chan = self.cp.get_consumer_channel_by_func( self.test_consumer )
        self.assertIsNotNone( chan, 'Could not retrieve channel by consumer function' )
        self.assertIsInstance( chan, pika.adapters.blocking_connection.BlockingChannel )
    


class TestChannelProperties( TestConsumerBase ):

    def setUp(self):
        super( TestChannelProperties, self ).setUp()
        self.channel_name = 'testconsumer'
        self.channel_key = self.module + '.' + self.channel_name

    def test_channel_registered(self):
        """ Known channel registered with consumer pool """
        self.assertIn( self.channel_key, self.cp.channels, 'Consumer name not registered with pool' )

    def test_channel_keys(self):
        """ All channels have mapped properties for management """
        channel = self.cp.channels[self.channel_key]
        self.assertIn( 'channel', channel ) 
        self.assertIn( 'queue', channel ) 
        self.assertIn( 'routing_key', channel ) 
        self.assertIn( 'callback', channel ) 
        self.assertNotIn( 'fish', channel ) 

    def test_channel_members(self):
        """ All channel properties are defined """
        channel = self.cp.channels[self.channel_key]
        self.assertIsNotNone( channel[ 'channel' ] )
        self.assertIsNotNone( channel[ 'queue' ] )
        self.assertIsNotNone( channel[ 'routing_key' ] )
        self.assertIsNotNone( channel[ 'callback' ] )

    def test_channel_member_types(self):
        """ Channel propperties values are of the right type """
        channel = self.cp.channels[self.channel_key]
        self.assertIsInstance( channel[ 'callback' ], type( lambda x: x ) )
        self.assertIsInstance( channel[ 'queue' ], pika.frame.Method )
        self.assertIsInstance( channel[ 'channel' ], pika.adapters.blocking_connection.BlockingChannel )
        self.assertIsInstance( channel[ 'routing_key' ], str )

    def test_channel_queue_name(self):
        """ Make Sure that the queue name in RabbitMQ matches what the pool manages so that we can re-attach to it """
        channel = self.cp.channels[self.channel_key]
        queue = channel['queue']
        self.assertEqual( queue.method.queue, '%s.%s' % (self.module, self.channel_name), 'Queue name incorrect!  Will not be able to re-bind' )



class TestProducerConsumerCommunication( TestConsumerBase ):

    def setUp(self):
        super( TestProducerConsumerCommunication, self ).setUp()
        self.tp = producer.Producer( routing_key = self.routing_key )
        self.body = 'This is a simple message'
        self.received_message = {}

#    @unittest.skip('because as blocking connections do not have timeouts' )
    def test_get_message(self):
        """ test that we can communicate with the consumer """

        @self.cp.consumer( self.routing_key )
        def test_message( channel, method, header, body ):
            self.received_message[ 'channel' ] = channel
            self.received_message[ 'method' ] = method
            self.received_message[ 'header' ] = header
            self.received_message[ 'body' ] = body

            channel.basic_ack( method.delivery_tag )
            channel.stop_consuming()

        def timeout():
            self.cp._gen_channel_name( test_message ).stop_consuming()

        self.cp._connection.add_timeout( 15, timeout )

        #Send the message
        self.tp.send( self.body )
        
        #Will block until message is received or timedout
        self.cp.start_consumer_func( test_message )

        self.assertEqual( self.body, self.received_message['body'], 'Message received as not the same as was sent!' )


if '__main__' == __name__:
    unittest.main()
