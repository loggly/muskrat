Muskrat
======

The small brother of the beaver, muskrat is a system that allows us to sync user information from many datasources (Frontend, Salesforce, Brantree, Statsserver, etc..) to many endpoints (Analytics Database, Marketo, Salesforce, etc... )

##Problem

Customer data is collected, aggregated, stored, not-stored from numerous different sources within our overall platform.  This creates a discontinuity when attempting to access those data for specific analytics tasks.  The current state of the system is an amalgamation of heavy cronned data-grabbing scripts, Frontend dependent triggers and subsequent data pushes.  This method will not scale well with growth of the customerbase (greedy data-grabs) or added complexity of sourcing more data and requiring more destinations. Along with this, the points of failure for any one script are numerous and can cause an entire segment of a sync to be lost indefinitely.

Likewise, one of the major drives of Muskrat comes from the understanding that the system currently operates with a 1-to-1 mapping of source data to destination.  Realistically, this needs to be a 1-to-N mapping in order to support all of our needs with the system and perform different routines on the source data (example: user account volume usage needs to be propogated to the analyticsdb and salesforce with different endpoint mutations/operations). 

##Solution

Create a modular sync system that separates the concerns of the datasourcing routines from that of the data-destination routines.  This should minimize the effect of failure points along with providing a more easily managed system that can debugged/grown.  This is accomplised by implementing a Producer-Consumer pattern utilizing RabbitMQ as the broker.

###RabbitMQ
RabbitMQ was chosen as the center point of the system to allow 3 main items:

1. Separation of concerns
2. PUB/SUB
3. Reliability/Persistance

In the Producer-Consumer paradigm, it acts as the Broker between the two.  It manages the means for the messages to be routed from the producers to the consumers.

#####Separation of concerns

Producers (tiggers, customer db scrapes, statsserver queries, etc... ) only need to do their single task and put the output out on the lline before they fall dormant until being woken up again based on their desired timing.

Consumers (AnalyticsDB injector, Marketo injector, salesforce injector, etc... ) are long running processes that lay in wait for their subscribed data source messages to arrive.  Syncronization timing is not an issue for these routines as they consume as much as they can receive as fast as possible. Likewise, if these consumers routines die unexpectedly, the producer messages are not affected nor are other consumers that rely on the same message.

#####PUB/SUB

Consumers define the data source locations that they wish to subscribe to.  This means that multiple consumers or data endpoints can be monitoring data messages from a single source concurrently.  Working in this way is a multicast or PUB/SUB pattern.  An example of usage can be seen in data volume collecting.  At the current time, both salesforce and the analyticsdb need access to the customer data volumes.  Allowing both these consumers to subscribe to the single data source message allows them to fulfill their roles concurrently without adding duplication to the source routine or complexity in the data marshalling routine.

#####Reliability/Persistance

RabbitMQ allows persistance of the queue.  For time-specific snapshots of user usage this is an essential reliability component.  If the data is written by the consumer then it will be present for the consumer.  Even if the consumer process were to die un-expectedly, the queue would buffer all messages being sent to various subscribers.

####

###Message Structure

The Following is a general best practice for message structure.  It is meants as a guide to help keep the system consistent.

####Routing Keys

Routing Keys are published with messages by the producer and subscribed to by the consumers.  They allow the broker enough information to discern how it should 'route' messages to specific consumers.

Because we want the producer to be as 'dumb' as possbile, we will make our messages routing keys defined by their data source location.  For example, if we were to collect Customer signup data via a trigger in the frontend database, we would give the message the routing key:

    Frontend.Customer.Signup

Routing keys should fall under the following patterns:

    Routing Key Guideline: <Source Location>.<Object>.<Source Action>

Doing this allows us to setup our consumers to listen for messages with various levels of granularity. For example,  

    Frontend.*
    Frontend.Customer.*
    Frontend.Customer.Signup
    
consumer Routing keys will all subscribe to this specific message. For more information on the versatility of routine key wildcards go here: [RabbitMQ Topics Pattern](http://www.rabbitmq.com/tutorials/tutorial-five-python.html).

####Message Bodies

Message bodies are transmitted as strings in a general structure that should be easily decoded by the consumer.  For this it is suggested that JSON is used.  The exact format of the message is left up to whomever is implementing the producer and consumer.  It is suggest that messages be tight and to the point, encompassing on the data that is most relevant as opposed to containing an entire, heavyweight object definition.

    Routing Key 

        Frontend.Customer.Signup

    Message

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
