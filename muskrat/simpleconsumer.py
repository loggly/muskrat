from consumer import Consumer

cons = Consumer()

@cons.consume( 'Frontend.Customer.Test' )
def printMessage(channel, method, header, body):
    print '%s' % channel
    print '%s' % method
    print '%s' % header
    print '%s' % body

def run():
    printMessage.start_consuming()
