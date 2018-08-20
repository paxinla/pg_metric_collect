#!/usr/bin/env python
# coding=utf-8


import bernhard
import traceback
import logging

logger = logging.getLogger("pg_metric_collect")


class EventAgent(object):
    def __init__(self, rm_tcp_host, rm_tcp_port, this_host):
        logger.debug("Use remote riemman uri: {}:{}".format(rm_tcp_host, rm_tcp_port))
        self.client = bernhard.Client(host=rm_tcp_host, port=rm_tcp_port)
        logger.debug("Tag this worker by host={}".format(this_host))
        self.this_host = this_host

    def send(self, msg):
        if not isinstance(msg, dict):
            raise TypeError("Method `send` require a dict as input parameter.")
        msg.update({"host": self.this_host})
        try:
            logger.debug("Sending message => {!s}".format(msg))
            self.client.send(msg)
        except bernhard.TransportError:
            logger.warn("Could not open TCP socket.")
        except:
            logger.error(msg)
            logger.error(traceback.format_exc())


if __name__ == "__main__":
    testc = EventAgent("127.0.0.1", 5555, "test_local")
    testc.send({"service": "test_message",
                "state": "ok",
                "tags": ["indicator_1a"],
                "metric": 13.34})
