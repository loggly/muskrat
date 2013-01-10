import json
from producer import Producer

p = Producer(routing_key='Frontend.Customer.Test')

def sendMessage():
    p.send( 'This is a simple message' )
    p.send( 'Another one' )
    p.send( 'I would now really care but here is the 3rd' )
    p.send( json.dumps( {'test':1, 'adcit':'has many items'} ) )
