"""
" Copyright:    Loggly
" Author:       Scott Griffin
" Last Updated: 02/21/2013
"
" These tests assume that config.py exists withing 
"
"""
import unittest
import os

import boto

os.environ['MUSKRAT'] = 'TEST'
from ..producer   import S3Producer
from ..s3consumer import S3Consumer, Consumer
from ..util      import config_loader

config_path = 'config.py'
TEST_KEY_PREFIX = 'Muskrat.Consumer'

class TestS3ConsumerBase( unittest.TestCase ):

    def setUp(self):
        #Setup a consumer pool
        #Add multiple function definitions
        self.routing_key = TEST_KEY_PREFIX
        self.config = config_loader( config_path )
        self.conn = boto.connect_s3( self.config.s3_key, self.config.s3_secret )
        self.bucket = self.conn.get_bucket( self.config.s3_bucket )

        def testconsumer(msg): pass

        self.consumer_func = testconsumer

    def delete_key(self, routing_key ):
        """ Deletes keys from the S3 bucket that were used in the tests """
        for key in self.bucket.list( prefix=routing_key.replace( '.', '/' ) ):
            self.bucket.delete_key( key )

    def tearDown(self):
        self.delete_key( 'MUSKRAT' )


class TestS3Consumer( TestS3ConsumerBase ):

    def setUp(self):
        super( TestS3Consumer, self ).setUp()
        self.routing_key += '.Test.Consumer'
    
    def test_instantiation( self ):
        """ Test S3Consumer creation """
        c = S3Consumer( self.routing_key, self.consumer_func )

        self.assertIsNotNone( c, 'S3 consumer could not be created.' )
        self.assertIsNotNone( c.s3conn, 'consumer could not connect with s3' )
        self.assertEqual( c.bucket.name, self.config.s3_bucket, 'consumer s3 bucket and config bucket are not equal' )
        self.assertEqual( c.callback, self.consumer_func, 'Consumer callback not set properly' )

        self.assertIsNotNone( c._cursor, 'Cursor failed to create' )
        self.assertEqual( c._cursor.type, 'file',  'Cursor default type is not file' )
        self.assertEqual( os.path.dirname( c._cursor.filename ), 
                          os.path.join( self.config.s3_cursor['location'] ),
                          'Cursor directory not created as expected' )
        self.assertEqual( os.path.basename( c._cursor.filename ), 
                          '__main__.testconsumer',
                          'Cursor file name not created as expected' )


    def test_bad_config( self ):
        """ Test S3Consumer with bad config """
        with self.assertRaises( IOError ):
            S3Consumer( self.routing_key, self.consumer_func,  config='borked_config.py' )

    def test_consume( self ):
        """ Test consumption of messges from s3 """
        p = S3Producer( routing_key=self.routing_key )
        msgs = ['Test1', 'Test2', 'Test3']

        def test_consumer_func( msg ):
            self.assertIn( msg, msgs, 'Retrieved unexpected message' )

        for msg in msgs:
            p.send( msg )

        c = S3Consumer( self.routing_key, test_consumer_func )
        c.consume()
       

    def test_cursor_update( self ):
        """ Test cursor update after successful consumption """
        p = S3Producer( routing_key=self.routing_key )
        next_msg = 'Was the cursor updated?'
        p.send( next_msg )

        def test_consumer_func( msg ):
            self.assertEqual( next_msg, msg, 'Cursor did not move to next message' )

        c = S3Consumer( self.routing_key, test_consumer_func )
        c.consume()

        
    def test_decorator(self):
        """ Test S3Consumer function decorator """
        p = S3Producer( routing_key=self.routing_key )
        message = 'This is a test of the consumer'
        p.send( message )

        @Consumer( self.routing_key )
        def decorated_consumer( msg ):
            self.assertEqual( message, msg, 'Consumer did not retrieve the correct message' )

        self.assertIsInstance( decorated_consumer.consumer, S3Consumer, 'Decorator did not correctly attach S3Consumer' )
        
        decorated_consumer.consumer.consume()
        

if '__main__' == __name__:
    unittest.main()
