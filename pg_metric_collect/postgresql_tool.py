#!/usr/bin/env python
# coding=utf-8

import traceback
import logging
import time

from sqlalchemy import create_engine
from sqlalchemy import text

logger = logging.getLogger("pg_metric_collect")


class PGAgent(object):
    def __init__(self, uri):
        logger.debug("Use database uri: {}".format(uri))
        self.eng = create_engine(uri, pool_size=10, pool_recycle=300)
        self.cur_dbname = uri.split('/')[-1]

        self.first_run = True


    def execute_sql(self, sql_str, params=None):
        with self.eng.connect() as conn:
            sql_text = text(sql_str).execution_options(autocommit=False)
            trans = conn.begin()
            try:
                if params is not None and isinstance(params, dict):
                    qrs = conn.execute(sql_text, params)
                else:
                    qrs = conn.execute(sql_text)

                trans.commit()
                return qrs
            except:
                trans.rollback()
                logger.error("ERR SQL: {}".format(sql_str))
                logger.error(traceback.format_exc())


    def _get_dbname(self):
        if self.first_run:
            rspxy = self.execute_sql("""SELECT current_database() """)
            rs = rspxy.fetchone()
            if rs is not None:
                self.cur_dbname = rs[0]
            self.first_run = False


    def metric_database_size(self):
        rspxy = self.execute_sql("""SELECT datname
                                         , extract(epoch from now())::int
                                         , pg_database_size(datname)
                                      FROM pg_database
                                     WHERE datname NOT IN ('contrib_regression', 'postgres', 'template1', 'template0')
                                  ORDER BY pg_database_size(datname) DESC """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            for r in rs:
                rtn.append({"service": "database_size",
                            "tags": [r[0]],
                            "time": r[1],
                            "metric": r[2]})
        return rtn


    def metric_database_connections(self):
        rspxy = self.execute_sql("""SELECT datname
                                         , COUNT(1)
                                      FROM pg_stat_activity
                                  GROUP BY datname """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            for r in rs:
                rtn.append({"service": "db_conns",
                            "tags": ["db_connection", r[0]],
                            "time": int(time.mktime(time.localtime())),
                            "metric": r[1]})
        return rtn


    def metric_database_active_connections(self):
        rspxy = self.execute_sql("""SELECT datname
                                         , SUM(CASE WHEN state = 'active'
                                                    THEN 1
                                                    ELSE 0
                                               END)
                                      FROM pg_stat_activity
                                  GROUP BY datname """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            for r in rs:
                rtn.append({"service": "active_conns",
                            "tags": ["db_connection", r[0]],
                            "time": int(time.mktime(time.localtime())),
                            "metric": r[1]})
        return rtn


    def metric_new_connections_in_5sec(self):
        rspxy = self.execute_sql("""SELECT datname
                                         , COUNT(1)
                                      FROM pg_stat_activity
                                     WHERE now()-backend_start <= '5 second'
                                  GROUP BY datname""")
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            for r in rs:
                rtn.append({"service": "new_conns_5s",
                            "tags": ["db_connection", r[0]],
                            "time": int(time.mktime(time.localtime())),
                            "metric": r[1]})
        return rtn


    def metric_max_connection_in_use(self):
        rspxy = self.execute_sql("""SELECT COUNT(1)::float
                                         / current_setting('max_connections')::float
                                      FROM pg_stat_activity """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            rtn.append({"service": "max_connection_in_use",
                        "time": int(time.mktime(time.localtime())),
                        "metric": rs[0]})
        return rtn


    def metric_index_hit_ratio(self):
        rspxy = self.execute_sql("""SELECT (SUM(idx_blks_hit) - SUM(idx_blks_read)) / CASE WHEN SUM(idx_blks_hit) = 0 THEN 1 ELSE SUM(idx_blks_hit) END
                                      FROM pg_statio_user_indexes """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            self._get_dbname()
            rtn.append({"service": "index_hit_ratio",
                        "time": int(time.mktime(time.localtime())),
                        "tags": [self.cur_dbname],
                        "metric": rs[0]})
        return rtn


    def metric_cache_hit_ratio(self):
        rspxy = self.execute_sql("""SELECT SUM(heap_blks_hit) / CASE WHEN (SUM(heap_blks_hit) + SUM(heap_blks_read)) = 0 THEN 1 ELSE (SUM(heap_blks_hit) + SUM(heap_blks_read)) END
                                      FROM pg_statio_user_tables """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            self._get_dbname()
            rtn.append({"service": "cache_hit_ratio",
                        "time": int(time.mktime(time.localtime())),
                        "tags": [self.cur_dbname],
                        "metric": rs[0]})
        return rtn


    def metric_qps(self):
        rspxy = self.execute_sql("""WITH a AS (SELECT SUM(calls) s, SUM(CASE WHEN ltrim(query,' ') ~* '^select' THEN calls ELSE 0 END) q FROM pg_stat_statements),   
                                         b AS (SELECT SUM(calls) s, SUM(CASE WHEN ltrim(query,' ') ~* '^select' THEN calls ELSE 0 END) q FROM pg_stat_statements , pg_sleep(1))   
                                    SELECT b.s-a.s  AS QPS  
                                         , b.q-a.q  AS RQPS  
                                         , b.s-b.q-a.s+a.q  AS WQPS  
                                      FROM a, b """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            for r in rs:
                the_time = int(time.mktime(time.localtime()))
                rtn.append({"service": "qps",
                            "tags": ["qps"],
                            "time": the_time,
                            "metric": r[0]})
                rtn.append({"service": "read_qps",
                            "tags": ["qps"],
                            "time": the_time,
                            "metric": r[1]})
                rtn.append({"service": "write_qps",
                            "tags": ["qps"],
                            "time": the_time,
                            "metric": r[2]})
        return rtn


    def metric_tps(self):
        """两次结果之差除于时间间隔即为 TPS 。"""
        # first
        rspxy = self.execute_sql("""SELECT datname
                                         , extract(epoch from now())::int AS ts
                                         , xact_commit+xact_rollback      AS txn_num
                                      FROM pg_catalog.pg_stat_database
                                     WHERE datname NOT IN ('contrib_regression', 'postgres', 'template0', 'template1') """)
        rs = rspxy.fetchall()
        qa = {}
        if rs is not None:
            for r in rs:
                qa[r[0]] = (r[1], r[2])

        time.sleep(3)

        # second
        rspxy = self.execute_sql("""SELECT datname
                                         , extract(epoch from now())::int AS ts
                                         , xact_commit+xact_rollback      AS txn_num
                                      FROM pg_catalog.pg_stat_database
                                     WHERE datname NOT IN ('contrib_regression', 'postgres', 'template0', 'template1') """)
        rs = rspxy.fetchall()
        qb = {}
        if rs is not None:
            for r in rs:
                qb[r[0]] = (r[1], r[2])

        rtn = []
        if qb is not None:
            the_time =  int(time.mktime(time.localtime()))
            for dbname, vals in qb.items():
                last_txnnum = qa[dbname][1] if qa.get(dbname, None) is not None else vals[0]
                last_txntim = qa[dbname][0] if qa.get(dbname, None) is not None else vals[1]-1
                rtn.append({"service": "tps",
                            "tags": [dbname],
                            "time": the_time,
                            "metric": (vals[1] - last_txnnum) / (vals[0] - last_txntim)})
        return rtn


    def metric_handled_rows_per_second(self):
        rspxy = self.execute_sql("""WITH a AS (SELECT SUM(rows) AS rs FROM pg_stat_statements)
                                       , b AS (SELECT SUM(rows) AS rs FROM pg_stat_statements, pg_sleep(1))
                                    SELECT b.rs-a.rs
                                      FROM a, b """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            rtn.append({"service": "handled_rows_per_second",
                        "time": int(time.mktime(time.localtime())),
                        "metric": rs[0]})
        return rtn


    def metric_new_dirty_page_per_second(self):
        rspxy = self.execute_sql("""WITH a AS (SELECT SUM(shared_blks_dirtied) AS rs, SUM(local_blks_dirtied) AS lrs FROM pg_stat_statements)
                                       , b AS (SELECT SUM(shared_blks_dirtied) AS rs, SUM(local_blks_dirtied) AS lrs FROM pg_stat_statements, pg_sleep(1))
                                    SELECT b.rs-a.rs
                                         , b.lrs-a.lrs
                                      FROM a, b """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            the_time = int(time.mktime(time.localtime()))
            rtn.append({"service": "new_dirty_page_per_second",
                        "time": the_time, 
                        "tags": ["shared_buffer"],
                        "metric": rs[0]})
            rtn.append({"service": "new_dirty_page_per_second",
                        "time": the_time,
                        "tags": ["local_buffer"],
                        "metric": rs[1]})
        return rtn


    def metric_write_dirty_page_per_second(self):
        rspxy = self.execute_sql("""WITH a AS (SELECT SUM(shared_blks_written) AS rs, SUM(local_blks_written) AS lrs FROM pg_stat_statements)
                                       , b AS (SELECT SUM(shared_blks_written) AS rs, SUM(local_blks_written) AS lrs FROM pg_stat_statements, pg_sleep(1))
                                    SELECT b.rs-a.rs
                                         , b.lrs-a.lrs
                                      FROM a, b """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            the_time = int(time.mktime(time.localtime()))
            rtn.append({"service": "write_dirty_page_per_second",
                        "time": the_time,
                        "tags": ["shared_buffer"],
                        "metric": rs[0]})
            rtn.append({"service": "write_dirty_page_per_second",
                        "time": the_time,
                        "tags": ["local_buffer"],
                        "metric": rs[1]})
        return rtn


    def metric_long_query_5sec(self):
        rspxy = self.execute_sql("""SELECT usename
                                         , datname
                                         , pid
                                      FROM pg_stat_activity
                                     WHERE state='active'
                                       AND query NOT LIKE 'autovacuum:%'
                                       AND now()-query_start > interval '5 second' """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            for r in rs:
                rtn.append({"service": r[1],
                            "time": int(time.mktime(time.localtime())),
                            "tags": ["long_query_5sec", r[0]],
                            "metric": r[2]})
        return rtn


    def metric_long_transaction_5sec(self):
        rspxy = self.execute_sql("""SELECT COUNT(1)
                                      FROM pg_stat_activity
                                     WHERE state='active'
                                       AND query NOT LIKE 'autovacuum:%'
                                       AND now()-xact_start > interval '5 second' """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            rtn.append({"service": "long_transaction_5sec",
                        "time": int(time.mktime(time.localtime())),
                        "metric": rs[0]})
        return rtn


    def metric_long_idle_in_transaction_5sec(self):
        rspxy = self.execute_sql("""SELECT COUNT(1)
                                      FROM pg_stat_activity
                                     WHERE state='idle in transaction'
                                       AND query NOT LIKE 'autovacuum:%'
                                       AND now()-state_change > interval '5 second' """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            rtn.append({"service": "long_idle_in_transaction_5sec",
                        "time": int(time.mktime(time.localtime())),
                        "metric": rs[0]})
        return rtn


    def metric_wait_session(self):
        rspxy = self.execute_sql("""SELECT COUNT(1)
                                      FROM pg_stat_activity
                                     WHERE wait_event IS NOT NULL """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            rtn.append({"service": "wait_session",
                        "time": int(time.mktime(time.localtime())),
                        "metric": rs[0]})
        return rtn


    def metric_dead_lock_number(self):
        rspxy = self.execute_sql("""SELECT SUM(deadlocks)
                                      FROM pg_stat_database """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            rtn.append({"service": "dead_lock_number",
                        "time": int(time.mktime(time.localtime())),
                        "metric": rs[0]})
        return rtn


    def metric_udi_rows(self):
        rspxy = self.execute_sql("""SELECT datname
                                         , tup_deleted
                                         , tup_updated
                                         , tup_inserted
                                      FROM pg_stat_database
                                     WHERE datname NOT IN ('contrib_regression', 'postgres', 'template0', 'template1') """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            for r in rs:
                the_time = int(time.mktime(time.localtime()))
                rtn.append({"service": "rows_update",
                            "tags": ["rows_rdi", r[0]],
                            "time": the_time,
                            "metric": r[2]})
                rtn.append({"service": "rows_insert",
                            "tags": ["rows_rdi", r[0]],
                            "time": the_time,
                            "metric": r[3]})
                rtn.append({"service": "rows_delete",
                            "tags": ["rows_rdi", r[0]],
                            "time": the_time,
                            "metric": r[1]})
        return rtn


    def metric_replication_lag(self):
        """只可在从库上执行。"""
        rspxy = self.execute_sql("""SELECT extract(epoch from now()-pg_last_xact_replay_timestamp()) """)
        rs = rspxy.fetchone()
        rtn = []
        if rs is not None:
            rtn.append({"service": "replication_lag",
                        "time": int(time.mktime(time.localtime())),
                        "metric": int(rs[0])})
        return rtn


    def metric_seq_idx_scan(self):
        rspxy = self.execute_sql("""SELECT schemaname
                                         , SUM(COALESCE(seq_scan, 0))
                                         , SUM(COALESCE(idx_scan, 0))
                                      FROM pg_stat_all_tables
                                     WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                                  GROUP BY schemaname
                                  ORDER BY schemaname """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            self._get_dbname()
            for r in rs:
                the_time =  int(time.mktime(time.localtime()))
                rtn.append({"service": "seq_scan",
                            "tags": ["seq_idx_scan", r[0], self.cur_dbname],
                            "time": the_time,
                            "metric": r[1]})
                rtn.append({"service": "idx_scan",
                            "tags": ["seq_idx_scan", r[0], self.cur_dbname],
                            "time": the_time,
                            "metric": r[2]})
        return rtn


    def metric_top10_long_query_in_db(self):
        rspxy = self.execute_sql("""SELECT usename
                                         , pid
                                      FROM pg_stat_activity
                                     WHERE datname = current_database()
                                  ORDER BY now() - query_start DESC
                                     LIMIT 10 """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            self._get_dbname()
            the_time =  int(time.mktime(time.localtime()))
            for r in rs:
                rtn.append({"service": "top10_long_query",
                            "time": the_time,
                            "tags": [r[0], self.cur_dbname],
                            "metric": r[1]})
        return rtn


    def metric_top10_history_long_query_in_db(self):
        rspxy = self.execute_sql("""SELECT pg_get_userbyid(userid)
                                         , queryid
                                      FROM pg_stat_statements
                                     WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                                  ORDER BY mean_time DESC
                                  LIMIT 10 """)
        rs = rspxy.fetchall()
        rtn = []
        if rs is not None:
            self._get_dbname()
            the_time =  int(time.mktime(time.localtime()))
            for r in rs:
                rtn.append({"service": "top10_his_long_query",
                            "time": the_time,
                            "tags": [r[0], self.cur_dbname],
                            "metric": r[1]})
        return rtn
