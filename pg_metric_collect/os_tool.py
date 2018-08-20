#!/usr/bin/env python
# coding=utf-8

import time
import logging
import os

import psutil

logger = logging.getLogger("pg_metric_collect")


class OSInfo(object):
    def bytes2human(self, n):
        symbols = ("KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        prefix = {}
        for i, s in enumerate(symbols):
            prefix[s] = 1 << (i + 1) * 10
        for s in reversed(symbols):
            if n >= prefix[s]:
                value = float(n) / prefix[s]
                return "%.1f%s" % (value, s)
        return "%sB" % n


    def metric_boot_time(self):
        return [{"service": "boot_time",
                 "time": int(time.mktime(time.localtime())),
                 "metric": time.mktime(time.localtime(psutil.boot_time()))}]
    

    def metric_average_load(self):
        avg_load = os.getloadavg()
        the_time = int(time.mktime(time.localtime()))
        return [{"service": "1min",
                 "time": the_time,
                 "tags": ["avg_load"],
                 "metric": avg_load[0]},
                {"service": "5min",
                 "time": the_time,
                 "tags": ["avg_load"],
                 "metric": avg_load[1]},
                {"service": "15min",
                 "time": the_time,
                 "tags": ["avg_load"],
                 "metric": avg_load[2]}]


    def metric_cpu_cores(self):
        the_time = int(time.mktime(time.localtime()))
        cpu_cores = psutil.cpu_count()
        return [{"service": "cpu_cores",
                 "time": the_time,
                 "metric": cpu_cores}]


    def metric_cpu_percent(self):
        cp = psutil.cpu_times_percent()
        the_time = int(time.mktime(time.localtime()))
        return [{"service": "user",
                 "tags": ["cpu_percent"],
                 "time": the_time,
                 "metric": cp.user},
                {"service": "system",
                 "tags": ["cpu_percent"],
                 "time": the_time,
                 "metric": cp.system},
                {"service": "idle",
                 "tags": ["cpu_percent"],
                 "time": the_time,
                 "metric": cp.idle},
                {"service": "iowait",
                 "tags": ["cpu_percent"],
                 "time": the_time,
                 "metric": cp.iowait}]


    def metric_memory(self):
        vm = psutil.virtual_memory()
        the_time = int(time.mktime(time.localtime()))
        return [{"service": "memory_total",
                 "tags": ["memory_info"],
                 "time": the_time,
                 "metric": vm.total},
                {"service": "memory_available",
                 "tags": ["memory_info"],
                 "time": the_time,
                 "metric": vm.available},
                {"service": "memory_used",
                 "tags": ["memory_info"],
                 "time": the_time,
                 "metric": vm.used},
                {"service": "memory_free",
                 "tags": ["memory_info"],
                 "time": the_time,
                 "metric": vm.free},
                {"service": "memory_percent",
                 "tags": ["memory_info"],
                 "time": the_time,
                 "metric": vm.percent}]


    def metric_disk_usage(self, mount_points=None):
        if mount_points is None or (not isinstance(mount_points, list)):
            all_parts = psutil.disk_partitions()
            mount_points = []
            for each_mp in all_parts:
                if not each_mp.mountpoint.startswith("/boot"):
                    mount_points.append(each_mp.mountpoint)

        rtn = []
        for each_mount_point in mount_points:
            du = psutil.disk_usage(each_mount_point)
            the_time = int(time.mktime(time.localtime()))
            rtn.append({"service": "disk_total",
                        "tags": ["disk_info", each_mount_point],
                        "time": the_time,
                        "metric": du.total})
            rtn.append({"service": "disk_used",
                        "tags": ["disk_info", each_mount_point],
                        "time": the_time,
                        "metric": du.used})
            rtn.append({"service": "disk_free",
                        "tags": ["disk_info", each_mount_point],
                        "time": the_time,
                        "metric": du.free})
            rtn.append({"service": "disk_percent",
                        "tags": ["disk_info", each_mount_point],
                        "time": the_time,
                        "metric": du.percent})
        return rtn


    def metric_disk_io(self):
        dio = psutil.disk_io_counters(perdisk=True)
        rtn = []
        for device, per_io in dio.items():
            dev_path = "/dev/{}".format(device)
            the_time = int(time.mktime(time.localtime()))
            rtn.append({"service": "read_bytes",
                        "tags": ["disk_io", dev_path],
                        "time": the_time,
                        "metric": per_io.read_bytes})
            rtn.append({"service": "write_bytes",
                        "tags": ["disk_io", dev_path],
                        "time": the_time,
                        "metric": per_io.write_bytes})
            rtn.append({"service": "read_time",
                        "tags": ["disk_io", dev_path],
                        "time": the_time,
                        "metric": per_io.read_time})
            rtn.append({"service": "write_time",
                        "tags": ["disk_io", dev_path],
                        "time": the_time,
                        "metric": per_io.write_time})
        return rtn
