# This is the Command Line interface

import argparse
import urlparse
# import inspect

import pika
import time

from Consumer import Consumer
from Producer import Producer
from Stats import Stats


class TestDriver(object):
    def __init__(self, args):
        self.exchangeType = args.exchangeType

        if args.exchangeName is None:
            self.exchangeName = self.exchangeType
        else:
            self.exchangeName = args.exchangeName
        self.queueName = args.queueName
        self.routingKey = args.routingKey
        self.randomRoutingKey = args.randomRoutingKey
        self.samplingInterval = args.samplingInterval
        self.producerRateLimit = args.producerRateLimit
        self.consumerRateLimit = args.consumerRateLimit
        self.producerCount = args.producerCount
        self.consumerCount = args.consumerCount
        self.producerTxSize = args.producerTxSize
        self.consumerTxSize = args.consumerTxSize
        self.confirm = args.confirm
        self.latencyLimitation = args.latencyLimitation
        self.autoAck = args.autoAck
        self.multiAckEvery = args.multiAckEvery
        self.channelPrefetch = args.channelPrefetch
        self.consumerPrefetch = args.consumerPrefetch
        self.minMsgSize = args.minMsgSize
        self.timeLimit = args.timeLimit
        self.producerMsgCount = args.producerMsgCount
        self.consumerMsgCount = args.consumerMsgCount
        self.flags = args.flags
        self.frameMax = args.frameMax
        self.heartbeat = args.heartbeat
        self.predeclared = args.predeclared
        self.curi = args.curi
        self.puri = args.puri

        if self.curi == '' and self.puri == '':
            self.curi = 'amqp://localhost'
            self.puri = 'amqp://localhost'
        elif self.curi =='' and self.puri != '':
            self.curi = self.puri
        elif self.curi != '' and self.puri == '':
            self.puri = self.curi

        print(self.autoAck)

    def gethostandportfromuri(self, uri):
        hostname = urlparse.urlparse(uri).hostname
        print ("hostname:: %s" % hostname)
        if hostname == '':
            hostname = "localhost"
        port = urlparse.urlparse(uri).port
        if port is None:
            port = 5672
        return hostname, port

    def createProducer(self, connect, stats,  routingKey):
        channel = connect.channel()


    def run(self):

        producer_threads = []
        consumer_threads = []

        producer_host, producer_port = self.gethostandportfromuri(self.puri)
        consumer_host, consumer_port = self.gethostandportfromuri(self.curi)

        producer_parameters = pika.ConnectionParameters(host=producer_host, port=producer_port)
        producer_connection = pika.BlockingConnection(parameters=producer_parameters)

        consumer_parameters = pika.ConnectionParameters(host=consumer_host, port=consumer_port)
        consumer_connection = pika.BlockingConnection(parameters=consumer_parameters)

        producer_channel = producer_connection.channel()
        consumer_channel = consumer_connection.channel()

        # stats = PrintStats(interval, latencyLimitation, sendStatsEnabled, recvStatsEnabled, returnStatsEnabled, confirmStatsEnabled, startConsumer)
        stats = PrintStats(self.samplingInterval, self.latencyLimitation, self.producerCount > 0, self.consumerCount > 0, ("mandatory" in self.flags or "immediate" in self.flags), self.confirm != -1)

        # Create Threads


        for i in range(self.consumerCount):
            t = Consumer(consumer_channel, self.routingKey, self.queueName, self.consumerRateLimit, self.consumerTxSize, self.autoAck, self.multiAckEvery, stats, self.consumerMsgCount, self.timeLimit)
            consumer_threads.append(t)

        for i in range(self.producerCount):
            t = Producer(producer_channel, self.exchangeName, self.exchangeType, self.routingKey, self.randomRoutingKey, self.flags, self.producerTxSize, self.producerRateLimit, self.producerMsgCount, self.timeLimit, self.minMsgSize, stats)
            producer_threads.append(t)

        # Start Threads
        for i in range(self.consumerCount):
            consumer_threads[i].start()

        for i in range(self.producerCount):
            producer_threads[i].start()



        # Wait Threads finished
        for i in range(self.consumerCount):
            consumer_threads[i].join()

        for i in range(self.producerCount):
            producer_threads[i].join()


        stats.printFinal()




        # producer = Producer(producer_channel, self.exchangeName, self.routingKey, self.randomRoutingKey, self.flags, self.producerTxSize, self.producerRateLimit, self.producerMsgCount, self.timeLimit, self.minMsgSize, stats)
        # consumer = Consumer(consumer_channel, self.routingKey, self.queueName, self.consumerRateLimit, self.consumerTxSize, self.autoAck, self.multiAckEvery, stats, self.consumerMsgCount, self.timeLimit)
        #
        # producer.run()
        # consumer.run()


class PrintStats(Stats):
    def __init__(self, interval, latencyLimitation, sendStatsEnabled, recvStatsEnabled, returnStatsEnabled, confirmStatsEnabled):
        super(self.__class__, self).__init__(interval, latencyLimitation)
        self.sendStatsEnabled = sendStatsEnabled
        self.recvStatsEnabled = recvStatsEnabled
        self.returnStatsEnabled = returnStatsEnabled
        self.confirmStatsEnabled = confirmStatsEnabled

    def reportnow(self, now):
        # print("time: % 6.3fs" % (now - self.startTime)),
        # print inspect.stack()
        print("time: {:8.3f}s,".format((now - self.startTime))),
        self.showRate("sent", self.sendCountInterval, self.sendStatsEnabled, self.elapsedInterval)
        self.showRate("returned", self.returnCountInterval, self.sendStatsEnabled and self.returnStatsEnabled, self.elapsedInterval)
        self.showRate("confirmed", self.confirmCountInterval, self.sendStatsEnabled and self.confirmStatsEnabled, self.elapsedInterval)
        self.showRate("nacked", self.nackCountInterval, self.sendStatsEnabled and self.confirmStatsEnabled, self.elapsedInterval)
        self.showRate("received", self.recvCountInterval, self.recvStatsEnabled, self.elapsedInterval)

        if self.latencyCountInterval > 0:
            print(", min/avg/max latency: %f/%f/%fms" % (self.minLatency, self.cumulativeLatencyInterval / self.latencyCountInterval, self.maxLatency))

        if self.recvStatsEnabled:
            print("latency lower than %fms is %dmsgs, %dmsgs; percent %f%%, average percent %f%%" % (self.latencyLimitation, self.acceptableLatencyCountInterval, self.recvCountInterval, self.acceptableLatencyCountInterval * 100 / self.recvCountInterval, self.acceptableLatencyCountTotal * 100 / self.recvCountTotal))

    def showRate(self, descr, count, display, elapsed):
        if display is True:
            print("{}: {} msgs/s".format(descr, self.formatRate(count / elapsed)))

    def printFinal(self):
        now = time.time()

        print("sending rate avg: " + self.formatRate(self.sendCountTotal / (now - self.startTime)) + " msg/s")
        elapsed = now - self.startTime
        if elapsed > 0:
            print("recving rate avg: " + self.formatRate(self.recvCountTotal / elapsed) + " msg/s")

    def formatRate(self, rate):
        if rate == 0.0:
            return "{:{width}}".format(int(rate), width=6)
        elif rate < 1:
            return "{:{width}}".format(int(rate), width=6)
        elif rate < 10:
            return "{:{width}}".format(int(rate), width=6)
        else:
            return "{:{width}}".format(int(rate), width=6)

def main():
    parser = argparse.ArgumentParser(prog="perftest", description="Start performance testing")

    parser.add_argument("-t", metavar='exchangeType', default='direct', dest='exchangeType')
    parser.add_argument("-e", metavar='exchangeName', default=None, dest='exchangeName')
    parser.add_argument("-u", metavar='queueName', default='', dest='queueName')
    parser.add_argument("-k", metavar='routingKey', default=None, dest='routingKey')
    parser.add_argument("-K", metavar='randomRoutingKey', default=False, type=bool, dest='randomRoutingKey')
    parser.add_argument("-i", metavar='samplingInterval', default=1, type=int, dest='samplingInterval')
    parser.add_argument("-r", metavar='producerRateLimit', default=0.0, type=float, dest='producerRateLimit')
    parser.add_argument("-R", metavar='consumerRateLimit', default=0.0, type=float, dest='consumerRateLimit')
    parser.add_argument("-x", metavar='producerCount', default=1, type=int, dest='producerCount')
    parser.add_argument("-y", metavar='consumerCount', default=1, type=int, dest='consumerCount')
    parser.add_argument("-m", metavar='producerTxSize', default=0, type=int, dest='producerTxSize')
    parser.add_argument("-n", metavar='consumerTxSize', default=0, type=int, dest='consumerTxSize')
    parser.add_argument("-c", metavar='confirm', default=-1, type=int, dest='confirm')
    parser.add_argument("-l", metavar='latencyLimitation', default=1, type=int, dest='latencyLimitation')
    parser.add_argument("-a", action='store_true', default=False, dest='autoAck')
    parser.add_argument("-A", metavar='multiAckEvery', default=0, type=int, dest='multiAckEvery')
    parser.add_argument("-Q", metavar='channelPrefetch', default=0, type=int, dest='channelPrefetch')
    parser.add_argument("-q", metavar='consumerPrefetch', default=0, type=int, dest='consumerPrefetch')
    parser.add_argument("-s", metavar='minMsgSize', default=0, type=int, dest='minMsgSize')
    parser.add_argument("-z", metavar='timeLimit', default=0, type=int, dest='timeLimit')
    parser.add_argument("-C", metavar='producerMsgCount', default=0, type=int, dest='producerMsgCount')
    parser.add_argument("-D", metavar='consumerMsgCount', default=0, type=int, dest='consumerMsgCount')
    parser.add_argument("-f", metavar='flags', default=[], type=list, dest='flags')
    parser.add_argument("-M", metavar='frameMax', default=0, type=int, dest='frameMax')
    parser.add_argument("-b", metavar='heartbeat', default=0, type=int, dest='heartbeat')
    parser.add_argument("-p", metavar='predeclared', default=False, type=bool, dest='predeclared')
    parser.add_argument("-w", metavar='curi', default='', dest='curi')
    parser.add_argument("-W", metavar='puri', default='', dest='puri')

    args = parser.parse_args()

    driver = TestDriver(args)

    driver.run()

if __name__ == "__main__":
    main()

# parser = argparse.ArgumentParser(prog="perftest", description="Start performance testing")
#
# parser.add_argument("-t", metavar='exchangeType',      default='direct',             dest='exchangeType'     )
# parser.add_argument("-e", metavar='exchangeName',      default=None,                 dest='exchangeName'     )
# parser.add_argument("-u", metavar='queueName',         default='',                   dest='queueName'        )
# parser.add_argument("-k", metavar='routingKey',        default=None,                 dest='routingKey'       )
# parser.add_argument("-K", metavar='randomRoutingKey',  default=False,    type=bool,  dest='randomRoutingKey' )
# parser.add_argument("-i", metavar='samplingInterval',  default=1,        type=int,   dest='samplingInterval' )
# parser.add_argument("-r", metavar='producerRateLimit', default=0.0,      type=float, dest='producerRateLimit')
# parser.add_argument("-R", metavar='consumerRateLimit', default=0.0,      type=float, dest='consumerRateLimit')
# parser.add_argument("-x", metavar='producerCount',     default=1,        type=int,   dest='producerCount'    )
# parser.add_argument("-y", metavar='consumerCount',     default=1,        type=int,   dest='consumerCount'    )
# parser.add_argument("-m", metavar='producerTxSize',    default=0,        type=int,   dest='producerTxSize'   )
# parser.add_argument("-n", metavar='consumerTxSize',    default=0,        type=int,   dest='consumerTxSize'   )
# parser.add_argument("-c", metavar='confirm',           default=-1,       type=int,   dest='confirm'          )
# parser.add_argument("-l", metavar='latencyLimitation', default=1,        type=int,   dest='latencyLimitation')
# parser.add_argument("-a", metavar='autoAck',           default=False,    type=bool,  dest='autoAck'          )
# parser.add_argument("-A", metavar='multiAckEvery',     default=0,        type=int,   dest='multiAckEvery'    )
# parser.add_argument("-Q", metavar='channelPrefetch',   default=0,        type=int,   dest='channelPrefetch'  )
# parser.add_argument("-q", metavar='consumerPrefetch',  default=0,        type=int,   dest='consumerPrefetch' )
# parser.add_argument("-s", metavar='minMsgSize',        default=0,        type=int,   dest='minMsgSize'       )
# parser.add_argument("-z", metavar='timeLimit',         default=0,        type=int,   dest='timeLimit'        )
# parser.add_argument("-C", metavar='producerMsgCount',  default=0,        type=int,   dest='producerMsgCount' )
# parser.add_argument("-D", metavar='consumerMsgCount',  default=0,        type=int,   dest='consumerMsgCount' )
# parser.add_argument("-f", metavar='flags',             default=[],       type=list,  dest='flags'            )
# parser.add_argument("-M", metavar='frameMax',          default=0,        type=int,   dest='frameMax'         )
# parser.add_argument("-b", metavar='heartbeat',         default=0,        type=int,   dest='heartbeat'        )
# parser.add_argument("-p", metavar='predeclared',       default=False,    type=bool,  dest='predeclared'      )
# parser.add_argument("-w", metavar='duri',              default='amqp://localhost',   dest='curi'             )
# parser.add_argument("-W", metavar='puri',              default='amqp://localhost',   dest='puri'             )
#
# args = parser.parse_args()
#
# print args
#
# exchangeType      = args.exchangeType
# exchangeName      = ''
# if args.exchangeName is None:
#     exchangeName = exchangeType
# else:
#     exchangeName = args.exchangeName
# queueName         = args.queueName
# routingKey        = args.routingKey
# randomRoutingKey  = args.randomRoutingKey
# samplingInterval  = args.randomRoutingKey
# producerRateLimit = args.producerRateLimit
# consumerRateLimit = args.consumerRateLimit
# producerCount     = args.producerCount
# consumerCount     = args.consumerCount
# producerTxSize    = args.producerTxSize
# consumerTxSize    = args.consumerTxSize
# confirm           = args.confirm
# latencyLimitation = args.latencyLimitation
# autoAck           = args.autoAck
# multiAckEvery     = args.multiAckEvery
# channelPrefetch   = args.channelPrefetch
# consumerPrefetch  = args.consumerPrefetch
# minMsgSize        = args.minMsgSize
# timeLimit         = args.timeLimit
# producerMsgCount  = args.producerMsgCount
# consumerMsgCount  = args.consumerMsgCount
# flags             = args.flags
# frameMax          = args.frameMax
# heartbeat         = args.heartbeat
# predeclared       = args.predeclared
# curi              = args.curi
# puri              = args.puri





# def on_message(channel, method_frame, header_frame, body):
#     print("on_message")
#     print(method_frame.routing_key)
#     print(method_frame.delivery_tag)
#     print(body)
#     print(time.time())
#     channel.basic_ack(delivery_tag=method_frame.delivery_tag)
#
# def getHostandPortfromURI(uri):
#     hostname = urlparse.urlparse(uri).hostname
#     print ("hostname:: %s" % hostname)
#     if hostname == '':
#         hostname = "localhost"
#     port = urlparse.urlparse(uri).port
#     if port is None:
#         port = 5672
#     return hostname, port
#
# chostname, cport = getHostandPortfromURI(args.curi)
# phostname, pport = getHostandPortfromURI(args.puri)
#
# print ("%s :::: %d" % (phostname, pport))
# pparameters = pika.ConnectionParameters(host=phostname, port=pport)
# pconnection = pika.BlockingConnection(parameters=pparameters)
# pchannel = pconnection.channel()
# pchannel.queue_declare(queue=args.queueName)
# pchannel.exchange_declare(exchange='test_exchange', type='direct')
# pchannel.queue_bind(queue=args.queueName, exchange='test_exchange', routing_key='test_routing_key')
# # t0 = datetime.datetime.utcnow()
# t0 = time.time()
# print(t0)
# #print (str(t0.year)+ "-" + str(t0.month) + "-" + str(t0.day) + " " + str(t0.hour) + ":" + str(t0.minute) + ":" + str(t0.second) + "." + str(t0.microsecond) + ",")
#
#
# #message = "" + str(t0.year)+ "-" + str(t0.month) + "-" + str(t0.day) + " " + str(t0.hour) + ":" + str(t0.minute) + ":" + str(t0.second) + "." + str(t0.microsecond) + ","
#
# message = str(t0)
#
# messageSize = len(message)
#
# if messageSize < minMsgSize:
#     message += ';' + 'a' * (minMsgSize - messageSize)
#
# print (message)
#
# pchannel.basic_publish('test_exchange', 'test_routing_key', message, pika.BasicProperties(content_type='text/plain', delivery_mode=1))
# time.sleep(10)
# # method_frame, header_fram, body = pchannel.basic_get(args.queueName)
# # if method_frame:
# #     print method_frame, header_fram, body
# #     pchannel.basic_ack(method_frame.delivery_tag)
# # else:
# #     print 'No message returned'
#
# pchannel.basic_consume(consumer_callback=on_message, queue=args.queueName)
# try:
#     pchannel.start_consuming()
# except KeyboardInterrupt:
#     pchannel.stop_consuming()
#
# pconnection.close()


