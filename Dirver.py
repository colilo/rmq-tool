# This is the Command Line interface



import argparse
import time
import datetime
import pika
import urlparse

#exchangeType = 'direct'
#exchangeName = ''
#queueName    = ''
#routingKey   = None
#randomRoutingKey = False
#samplingInterval = 1
#producerRateLimit = 0.0
#consumerRateLimit = 0.0
#producerCount = 1
#consumerCount = 1
#producerTxSize=0
#consumerTxSize=0
#confirm=-1
#latencyLimitation=0
#autoAck=False
#multiAckEvery = 0
#channelPrefetch = 0
#consumerPrefetch = 0
#minMsgSize=0
#timeLimit=0
#producerMsgCount=0
#consumerMsgCount=0
#flags=[]
#frameMax=0
#heartbeat=0
#predeclared=False
#curi = 'amqp://localhost'
#puri = 'amqp://localhost'

parser = argparse.ArgumentParser(prog="perftest", description="Start performance testing")

parser.add_argument("-t", metavar='exchangeType',      default='direct',           dest='exchangeType'     )
parser.add_argument("-e", metavar='exchangeName',      default=None,                 dest='exchangeName'     )
parser.add_argument("-u", metavar='queueName',         default='',                 dest='queueName'        )
parser.add_argument("-k", metavar='routingKey',        default=None,               dest='routingKey'       )
parser.add_argument("-K", metavar='randomRoutingKey',  default=False,    type=bool,          dest='randomRoutingKey' )
parser.add_argument("-i", metavar='samplingInterval',  default=1,        type=int,          dest='samplingInterval' )
parser.add_argument("-r", metavar='producerRateLimit', default=0.0,      type=float,          dest='producerRateLimit')
parser.add_argument("-R", metavar='consumerRateLimit', default=0.0,      type=float,          dest='consumerRateLimit')
parser.add_argument("-x", metavar='producerCount',     default=1,        type=int,          dest='producerCount'    )
parser.add_argument("-y", metavar='consumerCount',     default=1,        type=int,          dest='consumerCount'    )
parser.add_argument("-m", metavar='producerTxSize',    default=0,        type=int,          dest='producerTxSize'   )
parser.add_argument("-n", metavar='consumerTxSize',    default=0,        type=int,          dest='consumerTxSize'   )
parser.add_argument("-c", metavar='confirm',           default=-1,       type=int,          dest='confirm'          )
parser.add_argument("-l", metavar='latencyLimitation', default=1,        type=int,          dest='latencyLimitation')
parser.add_argument("-a", metavar='autoAck',           default=False,    type=bool,          dest='autoAck'          )
parser.add_argument("-A", metavar='multiAckEvery',     default=0,        type=int,          dest='multiAckEvery'    )
parser.add_argument("-Q", metavar='channelPrefetch',   default=0,        type=int,          dest='channelPrefetch'  )
parser.add_argument("-q", metavar='consumerPrefetch',  default=0,        type=int,          dest='consumerPrefetch' )
parser.add_argument("-s", metavar='minMsgSize',        default=0,        type=int,          dest='minMsgSize'       )
parser.add_argument("-z", metavar='timeLimit',         default=0,        type=int,          dest='timeLimit'        )
parser.add_argument("-C", metavar='producerMsgCount',  default=0,        type=int,          dest='producerMsgCount' )
parser.add_argument("-D", metavar='consumerMsgCount',  default=0,        type=int,          dest='consumerMsgCount' )
parser.add_argument("-f", metavar='flags',             default=[],       type=list,          dest='flags'            )
parser.add_argument("-M", metavar='frameMax',          default=0,        type=int,          dest='frameMax'         )
parser.add_argument("-b", metavar='heartbeat',         default=0,        type=int,          dest='heartbeat'        )
parser.add_argument("-p", metavar='predeclared',       default=False,    type=bool,          dest='predeclared'      )
parser.add_argument("-w", metavar='duri',              default='amqp://localhost',  dest='curi'             )
parser.add_argument("-W", metavar='puri',              default='amqp://localhost',  dest='puri'             )

args = parser.parse_args()

print args

exchangeType = args.exchangeType
exchangeName = ''
if args.exchangeName is None:
    exchangeName = exchangeType
else:
    exchangeName = args.exchangeName
queueName    = args.queueName
routingKey   = args.routingKey
randomRoutingKey = args.randomRoutingKey
samplingInterval = args.randomRoutingKey
producerRateLimit = args.producerRateLimit
consumerRateLimit = args.consumerRateLimit
producerCount = args.producerCount
consumerCount = args.consumerCount
producerTxSize = args.producerTxSize
consumerTxSize = args.consumerTxSize
confirm=         args.confirm
latencyLimitation=args.latencyLimitation
autoAck=args.autoAck
multiAckEvery = args.multiAckEvery
channelPrefetch = args.channelPrefetch
consumerPrefetch = args.consumerPrefetch
minMsgSize= args.minMsgSize
timeLimit=args.timeLimit
producerMsgCount=args.producerMsgCount
consumerMsgCount=args.consumerMsgCount
flags=args.flags
frameMax=args.frameMax
heartbeat=args.heartbeat
predeclared=args.predeclared
curi = args.curi
puri = args.puri





def on_message(channel, method_frame, header_frame, body):
    print("on_message")
    print(method_frame.routing_key)
    print(method_frame.delivery_tag)
    print(body)
    print(time.time())
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

def getHostandPortfromURI(uri):
    hostname = urlparse.urlparse(uri).hostname
    print ("hostname:: %s" % hostname)
    if hostname == '':
        hostname = "localhost"
    port = urlparse.urlparse(uri).port
    if port is None:
        port = 5672
    return hostname, port

chostname, cport = getHostandPortfromURI(args.curi)
phostname, pport = getHostandPortfromURI(args.puri)

print ("%s :::: %d" % (phostname, pport))
pparameters = pika.ConnectionParameters(host=phostname, port=pport)
pconnection = pika.BlockingConnection(parameters=pparameters)
pchannel = pconnection.channel()
pchannel.queue_declare(queue=args.queueName)
pchannel.exchange_declare(exchange='test_exchange', type='direct')
pchannel.queue_bind(queue=args.queueName, exchange='test_exchange', routing_key='test_routing_key')
# t0 = datetime.datetime.utcnow()
t0 = time.time()
print(t0)
#print (str(t0.year)+ "-" + str(t0.month) + "-" + str(t0.day) + " " + str(t0.hour) + ":" + str(t0.minute) + ":" + str(t0.second) + "." + str(t0.microsecond) + ",")


#message = "" + str(t0.year)+ "-" + str(t0.month) + "-" + str(t0.day) + " " + str(t0.hour) + ":" + str(t0.minute) + ":" + str(t0.second) + "." + str(t0.microsecond) + ","

message = str(t0)

messageSize = len(message)

if messageSize < minMsgSize:
    message += ';' + 'a' * (minMsgSize - messageSize)

print (message)

pchannel.basic_publish('test_exchange', 'test_routing_key', message, pika.BasicProperties(content_type='text/plain', delivery_mode=1))
time.sleep(10)
# method_frame, header_fram, body = pchannel.basic_get(args.queueName)
# if method_frame:
#     print method_frame, header_fram, body
#     pchannel.basic_ack(method_frame.delivery_tag)
# else:
#     print 'No message returned'

pchannel.basic_consume(consumer_callback=on_message, queue=args.queueName)
try:
    pchannel.start_consuming()
except KeyboardInterrupt:
    pchannel.stop_consuming()

pconnection.close()


