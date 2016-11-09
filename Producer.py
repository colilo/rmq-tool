import threading
import time
import uuid
import io

import pika

import ProducerConsumerBase


class Producer(ProducerConsumerBase.ProducerConsumerBase, threading.Thread):
    def __init__(self, channel, exchangeName, exchangeType, id, randomRoutingKey, flags, txSize, rateLimit, msgLimit, timeLimit, minMsgSize, stats):
        threading.Thread.__init__(self)
        self.channel          = channel
        self.exchangeName     = exchangeName
        self.exchangeType     = exchangeType
        self.id               = id
        self.randomRoutingKey = randomRoutingKey
        self.mandatory        = "mandatory" in flags  #flags.contains("mandatory")
        self.immediate        = "immediate" in flags  #flags.contains("immediate")
        self.persistent       = "persistent" in flags #flags.contains("persistent")
        self.txSize           = txSize
        self.rateLimit        = rateLimit
        self.msgLimit         = msgLimit
        self.timeLimit        = timeLimit
        self.message          = ''
        self.minMsgSize       = minMsgSize
        self.stats            = stats




    def publish(self, msg):
        routingKey = None
        if self.randomRoutingKey is None:
            routingKey = uuid.uuid4()
        else:
            routingKey = self.id

        deliveryMode = 1
        if self.persistent is not None:
            deliveryMode = 2

        basicProperties = pika.BasicProperties(content_type='text/plain', delivery_mode=deliveryMode)

        self.channel.basic_publish(self.exchangeName, routingKey, msg, basicProperties)

    def createMessage(self, sequenceNumber):
        now = time.time()
        message = str(sequenceNumber) + ';' + "{:.6f}".format(now)

        msgSize = len(message)

        if msgSize < self.minMsgSize:
            message += ';' + 'a' * (self.minMsgSize - msgSize - 1)
        return message

    def run(self):
        startTime = time.time()
        now = startTime
        self.lastStatsTime = startTime
        self.msgCount = 0
        totalMsgCount = 0

        self.channel.exchange_declare(exchange=self.exchangeName, exchange_type=self.exchangeType)

        while (self.timeLimit == 0 or now < startTime + self.timeLimit) and (self.msgLimit == 0 or self.msgCount < self.msgLimit):

            self.delay(now)

            self.publish(self.createMessage(totalMsgCount))

            totalMsgCount += 1
            self.msgCount += 1

            if self.txSize != 0 and totalMsgCount % self.txSize == 0:
                self.channel.txCommit()

            self.stats.handleSend()
            now = time.time()



        print("msgCount: %d, startTime: %f, timeLimit: %f, startTime + timeLimit: %f, now: %f" % (self.msgCount, startTime, self.timeLimit, startTime + self.timeLimit, now))
        self.stats.handleSend()

