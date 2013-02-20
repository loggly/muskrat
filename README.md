Muskrat
======

A python producer/consumer library that provides a persistent multicasting message queue with a simple interface. Originally built ontop of S3 for persistent message queueing that does not require the setup of a broker tool.  Also, has experimental support of multiple brokers.

Currently supports the following brokers: 

    s3
    RabbitMQ

Muskrat also allows Producers to 'tee' messages to multiple brokers.  As an example, a message can be tee'd to write to both RabbitMQ for high-throughput and s3 for message persistence/replay.

###Message Structure

####Routing Keys

Routing keys allow the producer to specify where the message is to be stored in the broker.  These routing_keys can then be subscribed to by multiple consumers and messages can be proccessed independently by the consumer endpoints.

Routing keys should fall under the following patterns:

    Example routing keys:

    ```Frontend.Customer.Signup 
    Chatserver.General
    Job.Queue.Command
    ```

####Message Bodies

Message bodies are transmitted as strings in a general structure that should be easily decoded by the consumer.  For this it is suggested that JSON is used.  The exact format of the message is left up to whomever is implementing the producer and consumer.

    Routing Key 

        Frontend.Customer.Signup

    Message

        ```json'{
            "id":48239,
            "email":"test@loggly.com",
            "phone":"(555)555-5555",
            "subdomain":"logglytest",
            "company":"loggly",
            "username":"awesome_logger"
            "subscription": {
                "volume":"200"
                "retention":"7"
                "rate":"0.0"
            }
        }'
        ```

###Producers

Producers write messages to brokers to be distributed.

####S3 Producers

S3Producer: Simple blocking S3 producer. Writes a message to S3.

Example:

```python
p = S3Producer( routing_key = 'Frontend.Customer.Signup' )
p.send( 'I am producing a message to s3!' )
p.send_json( {'email':'test@loggly.co', 'company':'loggly' } )
```

ThreadedS3Producer: An asynchronous interface to S3.  Allows the creation of a spcified thread pool to multiple asynchronous HTTP write requests to S3.  Used to speedup multiple contiguous writes as the producer will not block on the outbound network IO.

Example:

```python
p = ThreadedS3Producer( routing_key = 'ThreadTest.Messages', num_threads=100  )
for x in range( 1000 ):
    p.send_json( {'message':x} )
```


####RabbitMQ Producers

Experimental.  Utilizes RabbitMQ as a message queueing/brorker service.  Does not guarantee indefinite message persistence or lifecycle polcies.

####General Producers

General Producers can write to multiple brokers at one time.  A producer default to only use S3Producer if no other Producer objects are supplied.

```python
from muskrat.producer import Producer
p = Producer( routing_key='Chatserver.General' )
p.send( 'Welcome to General Chat!' )
 ```

__Currently__ RabbitMQ support is experimental.

###Consumers

Consumers receive messages from the brokers in chronological order and do work with them.  The consumer must be instantiated for the corresponding message broker, ala:

```python
p = Producer( routing_key='Simple.Producer' )
p.send( 'This is a simple producer-consumer pair' )

.
.
.

from muskrat.s3consumer import Consumer

@Consumer( 'Simple.Message.Queue' )
def consume_messages( msg ):
    print msg

consume_message.consumer.consume()
```

Consumers are just functions that know what to do with the messages for a routing key.  The above example will consume as many messages are on the queue when consume call was issued.

The ```@Consumer``` decorator is a convenience that allows the consumer object to be bound to the function's dictionary.  We can still call the function directly if desired.  Using the decorator is equivalent to: 

```python
from muskrat.s3consumer import S3Consumer

s3consumer = S3Consumer( 'Simple.Message.Queue', consume_messages )
s3consumer.consume()
```

#####Cursor

S3 consumers need to track their own cursor.  This is the routing_key + timestamp of the message.  By default, the cursor is written to a file defined by 'consumer_function.__module__'.'consumer_function.__name__' in the ```cursors``` folder of the muskrat package.  This allows muskrat to pick up and and continue processing messages starting where it last stopped.  Manipulating the cursor also allows for replay of messages or the ability to skip messages.

###Config

Configuration settings are defined in a python file.  They must define the local variable CONFIG.

```python
import os

class CONFIG(object):
    s3_timestamp_format = '%Y-%m-%dT%H:%M:%S.%f'
    s3_key              = 'YOUR S3 KEY'
    s3_secret           = 'YOUR S3 SECRET'
    s3_bucket           = 'chatserver'
    s3_cursor           = {
                            'type':'file',
                            'location':os.path.join( os.path.dirname(__file__), 'cursors' )
                        } 

    timeformat          = '%Y-%m-%dT%H:%M:%S'
