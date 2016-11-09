import time
import sys


class Stats(object):
    # protected final long    interval
    #
    # protected final long    startTime
    # protected long    lastStatsTime
    #
    # protected int     sendCountInterval
    # protected int     returnCountInterval
    # protected int     confirmCountInterval
    # protected int     nackCountInterval
    # protected int     recvCountInterval
    #
    # protected int     sendCountTotal
    # protected int     recvCountTotal
    #
    # protected int     latencyCountInterval
    # protected int     latencyCountTotal
    # protected long    minLatency
    # protected long    maxLatency
    # protected long    cumulativeLatencyInterval
    # protected long    cumulativeLatencyTotal
    #
    # protected long    latencyLimitation
    # protected long    acceptableLatencyCountInterval
    # protected long    acceptableLatencyCountTotal
    #
    # protected long    elapsedInterval
    # protected long    elapsedTotal

    def __init__(self, interval, latencyLimitation):
        self.interval = interval
        self.latencyLimitation = latencyLimitation # nanoseconds
        self.sendCountTotal = 0
        self.recvCountTotal = 0
        self.elapsedTotal = 0
        self.startTime = time.time()
        self.reset(self.startTime)


    def reset(self, lastStatsTime):
        self.lastStatsTime             = lastStatsTime

        self.sendCountInterval         = 0
        self.returnCountInterval       = 0
        self.confirmCountInterval      = 0
        self.nackCountInterval         = 0
        self.recvCountInterval         = 0
        self.minLatency                = sys.maxint
        self.maxLatency                = -sys.maxint - 1
        self.latencyCountInterval      = 0
        self.cumulativeLatencyInterval = 0L
        self.acceptableLatencyCountInterval    = 0L


    def report(self):
        now = time.time()
        self.elapsedInterval = now - self.lastStatsTime

        print("now: %f, lastStatsTime: %f, elapsedInterval: %f" % (now, self.lastStatsTime, self.elapsedInterval))

        if self.elapsedInterval >= self.interval:
            self.elapsedTotal += self.elapsedInterval
            self.reportnow(now)
            self.reset(now)



    # protected abstract void report(long now)
    def reportnow(self, now):
        pass

    def handleSend(self):
        self.sendCountInterval += 1
        self.sendCountTotal += 1
        self.report()

    def handleReturn(self):
        self.returnCountInterval += 1
        self.report()

    def handleConfirm(self, numConfirms):
        self.confirmCountInterval += numConfirms
        self.report()

    def handleNack(self, numAcks):
        self.nackCountInterval += numAcks
        self.report()

    def handleRecv(self, latency):
        self.recvCountInterval += 1
        self.recvCountTotal += 1
        if latency > 0:
            self.minLatency = min(self.minLatency, latency)
            self.maxLatency = max(self.maxLatency, latency)
            self.cumulativeLatencyInterval += latency
            self.cumulativeLatencyTotal += latency
            self.latencyCountInterval += 1
            self.latencyCountTotal += 1

            if latency < self.latencyLimitation:
                self.acceptableLatencyCountInterval += 1
                self.acceptableLatencyCountTotal += 1

        self.report()
