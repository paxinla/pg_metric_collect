#!/usr/bin/env python
# coding=utf-8

import logging
import time
import queue

logger = logging.getLogger("pg_metric_collect")

WAIT_INTERVAL = 2


class Sender(object):
    def __init__(self, mq, client, nouse):
        self.q = mq
        self.client = client

    def __call__(self):
        while True:
            new_msg = None
            try:
                new_msg = self.q.get(block=False)
            except queue.Empty:
                new_msg = None

            if new_msg is None:
                time.sleep(WAIT_INTERVAL)
                continue

            self.client.send(new_msg)


class PGMonitor(object):
    def __init__(self, mq, pgagent, full=True):
        self.q = mq
        self.agent = pgagent
        self.fullmode = full


    def put_metrics_into_queue(self, metrics):
        for metric in metrics:
            self.q.put(metric)


    def __call__(self):
        while True:
            if self.fullmode:
                self.put_metrics_into_queue(self.agent.metric_database_size())
                self.put_metrics_into_queue(self.agent.metric_database_connections())
                self.put_metrics_into_queue(self.agent.metric_database_active_connections())
                self.put_metrics_into_queue(self.agent.metric_new_connections_in_5sec())
                self.put_metrics_into_queue(self.agent.metric_max_connection_in_use())
                self.put_metrics_into_queue(self.agent.metric_qps())
                self.put_metrics_into_queue(self.agent.metric_tps())
                self.put_metrics_into_queue(self.agent.metric_handled_rows_per_second())
                #self.put_metrics_into_queue(self.agent.metric_new_dirty_page_per_second())
                #self.put_metrics_into_queue(self.agent.metric_write_dirty_page_per_second())
                self.put_metrics_into_queue(self.agent.metric_long_query_5sec())
                self.put_metrics_into_queue(self.agent.metric_long_transaction_5sec())
                self.put_metrics_into_queue(self.agent.metric_long_idle_in_transaction_5sec())
                self.put_metrics_into_queue(self.agent.metric_wait_session())
                self.put_metrics_into_queue(self.agent.metric_dead_lock_number())
                self.put_metrics_into_queue(self.agent.metric_udi_rows())
                #self.put_metrics_into_queue(self.agent.metric_replication_lag())

            self.put_metrics_into_queue(self.agent.metric_seq_idx_scan())
            self.put_metrics_into_queue(self.agent.metric_index_hit_ratio())
            self.put_metrics_into_queue(self.agent.metric_cache_hit_ratio())
            self.put_metrics_into_queue(self.agent.metric_top10_long_query_in_db())
            self.put_metrics_into_queue(self.agent.metric_top10_history_long_query_in_db())

            time.sleep(WAIT_INTERVAL)


class SysMonitor(object):
    def __init__(self, mq, osagent, nouse):
        self.q = mq
        self.agent = osagent


    def put_metrics_into_queue(self, metrics):
        for metric in metrics:
            self.q.put(metric)


    def __call__(self):
        while True:
            self.put_metrics_into_queue(self.agent.metric_boot_time())
            self.put_metrics_into_queue(self.agent.metric_average_load())
            self.put_metrics_into_queue(self.agent.metric_cpu_cores())
            self.put_metrics_into_queue(self.agent.metric_cpu_percent())
            self.put_metrics_into_queue(self.agent.metric_memory())
            self.put_metrics_into_queue(self.agent.metric_disk_usage())
            self.put_metrics_into_queue(self.agent.metric_disk_io())

            time.sleep(WAIT_INTERVAL)
