Muskrat
======

A python producer-consumer library that provides a persistent multicasting message queue with a simple interface. Originally built ontop of S3 for persistent message queueing that does not require the setup of a broker tool.  Also, has experimental support of multiple brokers.

Currently supports the following brokers: 

    s3
    RabbitMQ (experimental)

Muskrat also allows Producers to 'tee' messages to multiple brokers.  As an example, a message can be tee'd to write to both RabbitMQ for high-throughput and s3 for message persistence/replay.

###Message Structure

####Routing Keys

Routing keys allow the producer to specify where the message is to be stored in the broker.  These routing keys can then be subscribed to by multiple consumers and messages can be proccessed independently by the consumer endpoints.

Routing Keys are strings separated by '.' ala:

        Frontend.Customer.Signup 
        Chatserver.General
        Job.Queue.Command
    

####Message Bodies

Message bodies are simply just strings.  The producer library implements a JSON send as a convenience for sending objects.

    Routing Key 

        Frontend.Customer.Signup

    Message
        
        'Customer signed up!'


        '{
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
        

###Producers

Producers write messages to brokers to be distributed.

#####S3Producers

S3 Producers are blocking writers that upload the supplied message to S3 for the associated routing key.  the following is an example of using an S3Producer.

```python
from muskrat.producer import S3Producer

p = S3Producer( routing_key = 'Frontend.Customer.Signup' )
p.send( 'I am producing a message to s3!' )
p.send_json({
    'email':'test@loggly.com',
    'company':'loggly' 
})
```

#####ThreadedS3Producer

An asynchronous write interface to S3.  These producers allows the creation of a thread pool of a specified size that to can issue simultaneous HTTP requests to S3.  Used to speedup multiple contiguous writes as the producer will not block on the outbound network IO. Using this producer will eventually block until all messages have been processed regardless of the execution state of the main thread. This ensures that each message will be given at least one shot at being written.

Example:

```python
from muskrat.producer import ThreadedS3Producer

p = ThreadedS3Producer( routing_key = 'ThreadTest.Messages', num_threads=100  )
for x in range( 1000 ):
    p.send_json( { 'message':x } )
```


#####RabbitMQ Producers (__experimental__)

Utilizes RabbitMQ as a message queueing/brorker service.  Does not guarantee indefinite message persistence or lifecycle polcies.

#####General Producers

General Producers can write to multiple brokers at one time.  The general producer defaults to use only an S3Producer if no other Producer objects are supplied.

```python
from muskrat.producer import Producer

p = Producer( routing_key='Chatserver.General' )
p.send( 'Welcome to General Chat!' )
```


###Consumers

Consumers receive messages from the brokers in chronological order and do work with them.  The consumer must be instantiated for the corresponding message broker, ala:

```python
p = Producer( routing_key='Simple.Message.Queue' )
p.send( 'This is a simple producer-consumer pair' )
```

```python
from muskrat.s3consumer import Consumer

@Consumer( 'Simple.Message.Queue' )
def consume_messages( msg ):
    print msg

consume_message.consumer.consume()

stdout >> This is a simple producer-consumer pair
```

Consumers are just functions that know what to do with the messages sent to a routing key.  The above example will consume as many messages are on the queue when consume call was issued.

The ```@Consumer``` decorator is a convenience that allows the consumer object to be bound directly to the function. The function can still be called directly if desired (useful for testing).  

Using the decorator is equivalent to: 

```python
from muskrat.s3consumer import S3Consumer

s3consumer = S3Consumer( 'Simple.Message.Queue', consume_messages )
s3consumer.consume()
```

#####Cursor

S3 consumers need to track their own cursor.  This is the routing_key + timestamp of the message.  By default, the cursor is written to a file defined by ```__module__.consumer_function.__name__``` in the ```cursors``` folder of the muskrat package.  This allows muskrat to pick up and and continue processing messages starting where it last stopped.  Manipulating the cursor also allows for replay of messages or the ability to skip messages.

###Config

Configuration settings are defined in a python file.  They must define the module level variable CONFIG, which is mapped to the producer or consumer object upon creation. By default, ```config.py``` of ```muskrat/config.py```.

```python
import os

class Config(object):
    s3_timestamp_format = '%Y-%m-%dT%H:%M:%S.%f' #Timestamp format of cursor terminal file name.  See datetime.strftime for details.
    s3_key              = 'YOUR S3 KEY'
    s3_secret           = 'YOUR S3 SECRET'
    s3_bucket           = 'chatserver'           #S3 Bucket to produce messages to
    s3_cursor           = {
                            'type':'file',
                            'location':os.path.join( os.path.dirname(__file__), 'cursors' ) #Directory to store cursor files under
                        } 

    timeformat          = '%Y-%m-%dT%H:%M:%S'    #Timeformat for datetime objects in JSON messages

class DevConfig(object):
    s3_bucket           = 'chatserver_dev'

CONFIG = Config
```

If a configuration file external to a muskrat package is desired, the config parameter can be set to the full path of the external config.

```python
#Where /home/hoover/muskrat_config.py is our external config file.

p = Producer( routing_key= 'Simple.Message.Queue', config='/home/hoover/muskrat_config.py' )

c = S3Consumer( 'Simple.Message.Queue', config='/home/hoover/muskrat_config.py' )

@Consumer( 'Simple.Message.Queue', config='/home/hoover/muskrat_config.py' )
def simple_consume( msg ):
    print msg
```

###TODO

