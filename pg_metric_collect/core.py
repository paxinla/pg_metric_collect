#!/usr/bin/env python
# coding=utf-8

import sys
import logging
import argparse
import configparser
import multiprocessing
import signal
import traceback

from pg_metric_collect.postgresql_tool import PGAgent
from pg_metric_collect.riemann_tool import EventAgent
from pg_metric_collect.os_tool import OSInfo
from pg_metric_collect.worker import Sender
from pg_metric_collect.worker import PGMonitor
from pg_metric_collect.worker import SysMonitor


logFormatter = logging.Formatter('%(asctime)s [%(levelname)s] (%(pathname)s:%(lineno)d@%(funcName)s) -> %(message)s')
logger = logging.getLogger("pg_metric_collect")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
logger.setLevel(logging.INFO)


def parse_input():
    parser = argparse.ArgumentParser(description="PostgreSQL host information collector.")

    parser.add_argument("--conf", dest="config_filepath", type=str, required=True,
                        help="Absolute path of config.ini .")
    parser.add_argument("--nosys", dest="nosys", action='store_true', required=False,
                        help="Do not collect information of OS.")
    parser.add_argument("--noalldb", dest="noalldb", action='store_true', required=False,
                        help="Do not collect general information of all databases.")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    return vars(parser.parse_args())


def load_conf(conf_path):
    conf_loader = configparser.ConfigParser()
    logger.debug("Load config file from: {}".format(conf_path))
    conf_loader.read(conf_path)
    return conf_loader


def killall(signum, frame):
    logger.info("Kill all workers ... ...")
    for child in multiprocessing.active_children():
        child.terminate()


def combind_all_components(cmd_args , conf):
    try:
        component_agent_map = [(Sender, EventAgent(conf.get("riemann", "tcp_host"),
                                                   conf.get("riemann", "tcp_port"),
                                                   conf.get("riemann", "host_tag")), None)]
        if not cmd_args["nosys"]:
            component_agent_map.append((SysMonitor, OSInfo(), None))

        if not cmd_args["noalldb"]:
            component_agent_map.append((PGMonitor, PGAgent(conf.get("postgresql", "uri")), True))
        else:
            component_agent_map.append((PGMonitor, PGAgent(conf.get("postgresql", "uri")), False))

        workers = []
        message_queue = multiprocessing.Queue()

        for each_component in component_agent_map:
            logger.debug("Spawn worker {}".format(each_component[0]))
            wrk = multiprocessing.Process(target=each_component[0](message_queue, each_component[1], each_component[2]))
            wrk.start()
            workers.append(wrk)

        signal.signal(signal.SIGTERM, killall)

        for j in workers:
            j.join()
    except:
        traceback.print_exc()
        killall(None, None)
