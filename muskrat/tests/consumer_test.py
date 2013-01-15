"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 01/14/2013
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
        self.routing_key = 'Source.Object.Method' 
        self.cp = consumer.ConsumerPool()
        self.cp.connect()
        self.module = self.__module__

        @self.cp.consumer( self.routing_key )
        def testconsumer(): pass

class TestConsumerPool( TestConsumerBase ):
    
    def testConsumerPoolExists( self ):
        """ ConsumerPool object was constructed """
        self.assertIsNotNone( self.cp, 'Consumer pool does not exist.  Was not created' )

    def testConnection(self):
        """ Connection with RabbitMQ successfully made """
        self.assertEqual( self.cp._connection.is_open, True, 'RabbitMQ connection not established' )


class TestChannelCreation( TestConsumerBase ):

    def testChannelsExist(self):
        """ Channels exist in the consumer pool """
        self.assertIsNotNone( self.cp.channels, 'Consumer pool channels is empty')

    def testChannelNames(self):
        """ Generating channel names for lookup based on consumer function """
        def simplefunc(): pass
        self.assertEqual( self.cp._gen_channel_name( simplefunc ), 
                          '%s.simplefunc' % self.module, 
                          'Channel name was generated incorrectly' )

    def testRegistrationDecorator(self):
        """ Channel registered with decorator """
        @self.cp.consumer( self.routing_key )
        def test_decorator(): pass
        self.assertIn( '%s.testconsumer' % self.module, self.cp.channels, 'Consumer name not registered with pool' )

    def testRegistrationFunction(self):
        """ Register a channel manually """
        #Manual consumer
        def manual_consumer(): pass
        func = manual_consumer
        routing_key = 'Manual.Consumer.Route'
        self.cp.register_consumer( func, routing_key )
        self.assertIn( '%s.manual_consumer' % self.module, self.cp.channels, 'Could not manually register consumer' )
        self.assertEqual( self.cp.channels['%s.manual_consumer' % self.module]['routing_key'], routing_key )
    

class TestChannelProperties( TestConsumerBase ):

    def setUp(self):
        super( TestChannelProperties, self ).setUp()
        self.channel_name = 'testconsumer'
        self.channel_key = self.module + '.' + self.channel_name

    def testChannelRegistered(self):
        """ Known channel registered with consumer pool """
        self.assertIn( self.channel_key, self.cp.channels, 'Consumer name not registered with pool' )

    def testChannelKeys(self):
        """ All channels have mapped properties for management """
        channel = self.cp.channels[self.channel_key]
        self.assertIn( 'channel', channel ) 
        self.assertIn( 'queue', channel ) 
        self.assertIn( 'routing_key', channel ) 
        self.assertIn( 'callback', channel ) 
        self.assertNotIn( 'fish', channel ) 

    def testChannelMembers(self):
        """ All channel properties are defined """
        channel = self.cp.channels[self.channel_key]
        self.assertIsNotNone( channel[ 'channel' ] )
        self.assertIsNotNone( channel[ 'queue' ] )
        self.assertIsNotNone( channel[ 'routing_key' ] )
        self.assertIsNotNone( channel[ 'callback' ] )

    def testChannelMemberTypes(self):
        """ Channel propperties values are of the right type """
        channel = self.cp.channels[self.channel_key]
        self.assertIsInstance( channel[ 'callback' ], type( lambda x: x ) )
        self.assertIsInstance( channel[ 'queue' ], pika.frame.Method )
        self.assertIsInstance( channel[ 'channel' ], pika.adapters.blocking_connection.BlockingChannel )
        self.assertIsInstance( channel[ 'routing_key' ], str )

    def testChannelQueueName(self):
        """ Make Sure that the queue name in RabbitMQ matches what the pool manages so that we can re-attach to it """
        channel = self.cp.channels[self.channel_key]
        queue = channel['queue']
        self.assertEqual( queue.method.queue, '%s.%s' % (self.module, self.channel_name), 'Queue name incorrect!  Will not be able to re-bind' )


if '__main__' == __name__:
    unittest.main()
