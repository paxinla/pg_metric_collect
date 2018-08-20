#!/usr/bin/env python
# coding=utf-8

from pg_metric_collect.core import parse_input
from pg_metric_collect.core import load_conf
from pg_metric_collect.core import combind_all_components


def main():
    in_args = parse_input()
    config_filepath = in_args["config_filepath"]
    combind_all_components(in_args, load_conf(config_filepath))


if __name__ == "__main__":
    main()
