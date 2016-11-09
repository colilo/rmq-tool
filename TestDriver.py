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
        self.routingPattern = args.routingPattern
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

    def createProducer(self, connect, stats):
        channel = connect.channel()
        if self.producerTxSize > 0:
            channel.tx_select()
        if self.confirm >= 0:
            channel.confirm_delivery()
        channel.exchange_declare(self.exchangeName, self.exchangeType)

        return Producer(channel, self.exchangeName, self.exchangeType, self.routingKey, self.randomRoutingKey,
                        self.flags, self.producerTxSize, self.producerRateLimit, self.producerMsgCount, self.timeLimit,
                        self.minMsgSize, stats)

    def createConsumer(self, connect, stats, routingKey, autoDelete):
        channel = connect.channel()
        if self.consumerTxSize > 0:
            channel.tx_select()
        channel.exchange_declare(self.exchangeName, self.exchangeType)
        result = channel.queue_declare(queue=self.queueName, durable="persistent" in self.flags, auto_delete=autoDelete)
        self.queueName = result.method.queue

        channel.queue_bind(self.queueName, self.exchangeName, routingKey)

        channel.basic_qos(self.consumerPrefetch)
        channel.basic_qos(self.channelPrefetch, True)

        return Consumer(channel, routingKey, self.queueName, self.consumerRateLimit, self.consumerTxSize,
                        self.autoAck, self.multiAckEvery, stats, self.consumerMsgCount, self.timeLimit)

    def run(self, announceStartup):

        producerConnectionList = []
        consumerConnectionList = []

        consumer_threads = []
        producer_threads = []

        stats = PrintStats(self.samplingInterval, self.latencyLimitation, self.producerCount > 0,
                           self.consumerCount > 0, ("mandatory" in self.flags or "immediate" in self.flags),
                           self.confirm != -1)

        # Create connection
        producer_host, producer_port = self.gethostandportfromuri(self.puri)
        consumer_host, consumer_port = self.gethostandportfromuri(self.curi)

        producer_parameters = pika.ConnectionParameters(host=producer_host, port=producer_port, frame_max=self.frameMax,
                                                        heartbeat_interval=self.heartbeat)

        consumer_parameters = pika.ConnectionParameters(host=consumer_host, port=consumer_port, frame_max=self.frameMax,
                                                        heartbeat_interval=self.heartbeat)

        # support all exchange types
        if self.exchangeType == 'direct':
            # direct
            for i in range(self.consumerCount):
                if announceStartup:
                    print("starting consumer *" + str(i))

                conn = pika.BlockingConnection(parameters=consumer_parameters)
                consumerConnectionList.append(conn)
                consumer_threads.append(self.createConsumer(conn, stats, self.routingKey, autoDelete=True))

            for i in range(self.producerCount):
                if announceStartup:
                    print("starting producer *" + str(i))
                conn = pika.BlockingConnection(parameters=producer_parameters)
                producerConnectionList.append(conn)
                producer_threads.append(self.createProducer(conn, stats))
        elif self.exchangeType == 'topic':
            # topic
            for i in range(self.consumerCount):
                if announceStartup:
                    print("starting consumer *" + str(i))

                conn = pika.BlockingConnection(parameters=consumer_parameters)
                consumerConnectionList.append(conn)
                consumer_threads.append(self.createConsumer(conn, stats, self.routingPattern, autoDelete=True))

            for i in range(self.producerCount):
                if announceStartup:
                    print("starting producer *" + str(i))
                conn = pika.BlockingConnection(parameters=producer_parameters)
                producerConnectionList.append(conn)
                producer_threads.append(self.createProducer(conn, stats))

        elif self.exchangeType == 'fanout':
            # fanout
            for i in range(self.consumerCount):
                if announceStartup:
                    print("starting consumer *" + str(i))

                conn = pika.BlockingConnection(parameters=consumer_parameters)
                consumerConnectionList.append(conn)
                consumer_threads.append(self.createConsumer(conn, stats, self.routingKey, autoDelete=True))

            for i in range(self.producerCount):
                if announceStartup:
                    print("starting producer *" + str(i))
                conn = pika.BlockingConnection(parameters=producer_parameters)
                producerConnectionList.append(conn)
                producer_threads.append(self.createProducer(conn, stats))
        elif self.exchangeType == 'headers':
            # headers
            for i in range(self.consumerCount):
                if announceStartup:
                    print("starting consumer *" + str(i))

                conn = pika.BlockingConnection(parameters=consumer_parameters)
                consumerConnectionList.append(conn)
                consumer_threads.append(self.createConsumer(conn, stats, self.routingKey, autoDelete=True))

            for i in range(self.producerCount):
                if announceStartup:
                    print("starting producer *" + str(i))
                conn = pika.BlockingConnection(parameters=producer_parameters)
                producerConnectionList.append(conn)
                producer_threads.append(self.createProducer(conn, stats))
                producer_threads.append(Producer(conn, self.exchangeName, self.exchangeType, self.routingKey,
                                                 self.randomRoutingKey,
                                                 self.flags, self.producerTxSize, self.producerRateLimit,
                                                 self.producerMsgCount, self.timeLimit, self.minMsgSize, stats))

        # Start Threads
        for i in range(len(consumer_threads)):
            consumer_threads[i].start()

        for i in range(len(producer_threads)):
            producer_threads[i].start()

        # Wait Threads finished
        for i in range(len(consumer_threads)):
            consumer_threads[i].join()
            consumerConnectionList[i].close()

        for i in range(len(producer_threads)):
            producer_threads[i].join()
            producerConnectionList[i].close()

        stats.printFinal()


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

    parser.add_argument("-K", action='store_true', default=False, dest='randomRoutingKey')

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
    parser.add_argument("-M", metavar='frameMax', default=None, type=int, dest='frameMax')
    parser.add_argument("-b", metavar='heartbeat', default=0, type=int, dest='heartbeat')

    parser.add_argument("-p", action='store_true', default=False, dest='predeclared')

    parser.add_argument("-P", metavar='routingPattern', default=None, dest='routingPattern')

    parser.add_argument("-w", metavar='curi', default='', dest='curi')
    parser.add_argument("-W", metavar='puri', default='', dest='puri')

    args = parser.parse_args()

    driver = TestDriver(args)

    driver.run(True)

if __name__ == "__main__":
    main()


