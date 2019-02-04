"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 02/13/2013
"
" Unit tests for muskrat producers.  Currently only test S3 producers and
" default Producer with S3.
"
" TODO - Implement RabbitMQ producer and Kafka
"
"""
import unittest
import re
import os
from datetime import datetime

import boto

os.environ['MUSKRAT'] = 'TEST'
from ..       import producer
from ..config import TestConfig as config


TEST_KEY_PREFIX = 'Muskrat.Producer'

boto.config.add_section('s3')
boto.config.set('s3', 'use-sigv4', 'True')

class TestProducerBase( unittest.TestCase ):

    def setUp(self):
        self.key = TEST_KEY_PREFIX
        self.msg = 'This is a simple test'
        self.json_msg = {'Testing':'yes', 'format':'json' }


class TestProducer( TestProducerBase ):

    def setUp(self):
        super( TestProducer, self ).setUp()
        self.key += '.Test.Producer'
    
    def test_s3defaultInstantiation(self):
        """ Test producer creation """
#        with self.assertRaises( Exception, 'No routing key defined lacks exception' ):
#            producer.Producer( self.key )
        
#        with self.assertRaises( Exception, 'No broker defined lacks exception' ):
#            producer.Producer( routing_key=self.key, s3=False, rabbitmq=False )
        
        p = producer.Producer( routing_key = self.key )

        self.assertIsNotNone( p, 'Could no instantiate object' )
        self.assertIsNotNone( p.brokers, 'No broker producers registered for producer' )

        self.assertEqual( len( p.brokers ), 1, 'Default instantiation has multiple brokers' )

        p_broker = p.brokers[0] 
        self.assertIsInstance( p_broker, producer.S3Producer, 'Default broker is not S3' )
        self.assertEqual( p_broker.routing_key, self.key.upper(), 'Broker\'s key inconsistent' )

    def test_send(self):
        """ Test send of the brokers """
        p = producer.Producer( routing_key = self.key )
        p.send( self.msg )
        p.send_json( self.json_msg )
        p.send_json( {'Testing':'yes', 'date':datetime.today()} )

        

class TestS3Producer( TestProducerBase ):

    def setUp(self):
        super( TestS3Producer, self ).setUp()
        self.conn = boto.connect_s3( config.s3_key, config.s3_secret, host=config.s3_host )
        self.bucket = self.conn.get_bucket( config.s3_bucket )
        self.key += '.Test.S3Producer'
    
    def delete_key(self, routing_key ):
        """ Deletes keys from the S3 bucket that were used in the tests """
        for key in self.bucket.list( prefix=routing_key.replace( '.', '/' ) ):
            self.bucket.delete_key( key )

    def test_instantiation( self ):
        """ S3 producer creation """
        p = producer.S3Producer( routing_key = self.key )
        self.assertEqual( p.routing_key, self.key.upper(), 'Routing key inconsistent' )
        self.assertIsNotNone( p.s3conn, 'S3 Connection not made' )
        self.assertIsNotNone( p.bucket, 'S3 bucket not retrieved' )

        self.assertEqual( p.bucket.name, config.s3_bucket, 'S3 bucket is not same as defined in config' )

    def test_key_generation(self):
        """ Generating key prefix """
        p = producer.S3Producer( routing_key = self.key )
        key_prefix = 'Muskrat/Producer/Test/S3Producer'
        self.assertEqual( p._create_key_prefix( self.key ), key_prefix, 'Key prefix not generated correctly' )
        self.assertRegexpMatches( 
                p._create_key_name( self.key ), 
                re.compile( r'%s/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}' % self.key ),
                'Key does not match expected format' )

    def test_send(self):
        """ Sending a message """
        routing_key = self.key + '.send'
        known_key = routing_key.upper().replace( '.', '/' )
        p = producer.S3Producer( routing_key = routing_key )

        p.send( self.msg )
        keys = self.bucket.get_all_keys( prefix=known_key)

        self.assertIsNotNone( keys, 'string message not written to s3' )
        akey = keys[-1]
        self.assertEqual( 
                akey.get_contents_as_string(), 
                self.msg,
                'Received message not the same as sent' )

        self.delete_key( routing_key )

    def test_send_json(self):
        """ Sending a message as json """
        routing_key = self.key + '.send_json'
        known_key = routing_key.upper().replace( '.', '/' )
        p = producer.S3Producer( routing_key = routing_key )

        import simplejson as json
        p.send_json( self.json_msg )

        keys = self.bucket.get_all_keys( prefix=known_key )
        self.assertIsNotNone( keys, 'Json message not written to s3' )
        akey = keys[-1]
        self.assertEqual( 
                json.loads( akey.get_contents_as_string() ), 
                self.json_msg,
                'Received json message not that same as sent' )
       
        #Wipe the key
        self.delete_key( routing_key )

    def test_lifecycle_policy(self):
        pass

    def tearDown(self):
        self.delete_key( 'MUSKRAT' )
        

if '__main__' == __name__:
    unittest.main()
