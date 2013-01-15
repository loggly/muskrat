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

class TestProducer( unittest.TestCase ):

    def setUp(self):
        self.tp = producer.Producer( self.routing_key )
