import threading
import time


class ProducerConsumerBase(object):

    def __init__(self, rateLimit, lastStatsTime, msgCount):
        self.rateLimit = rateLimit
        self.lastStatsTime = lastStatsTime
        self.msgCount = msgCount

    def delay(self, now):
        elapsed = now - self.lastStatsTime

        pause = 0.0

        if self.rateLimit == 0:
            pause = 0.0
        else:
            pause = self.msgCount / self.rateLimit - elapsed

        if pause > 0:
                time.sleep(pause)
