# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
from collections import OrderedDict
import logging

from marvin_backend import utils
from marvin_backend import queries

LOGGER = logging.getLogger(__name__)


class HeartBeats(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp):
        all_heartbeats_sql = "SELECT * FROM heartbeat"
        all_heartbeats = self._db.execute_query(all_heartbeats_sql)

        return all_heartbeats


class SingleServerHeartbeats(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint_404_on_empty
    @utils.api_endpoint
    def on_get(self, req, resp, sid):
        server_beats_sql = "SELECT * FROM heartbeat WHERE server_session=%(sid)s"
        server_beats = self._db.execute_query(server_beats_sql, {'sid': sid})

        return server_beats


class CPUload(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp, sid):
        cpuload_sql = "SELECT h.heartbeat_id, h.clk, avg(c.val) AS cpuload FROM cpuload AS c JOIN heartbeat AS h ON c.heartbeat_id=h.heartbeat_id WHERE h.server_session=%(sid)s GROUP BY h.heartbeat_id, h.clk"
        cpuload = self._db.execute_query(cpuload_sql, {'sid': sid})

        return cpuload


class QueryLoad(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp, qid):
        # First find all the executions related to this query
        # TODO I should not have to instantiate this class
        qe = queries.QueryExecutions(self._db)
        execution_ids = list(map(int, qe.gather_executions(qid)))

        # Find the the server sessions of these executions
        sessions_sql = "SELECT server_session FROM mal_execution WHERE execution_id=%(eid)s"
        sessions = set()
        for ex_id in execution_ids:
            s = self._db.execute_query(sessions_sql, {'eid': ex_id})
            sessions.add(s['server_session'][0])

        LOGGER.debug("*" * 20)
        LOGGER.debug("session UUIDs = %s", sessions)
        LOGGER.debug("*" * 20)
        # TODO probably does not do what it's supposed to do... I suspect it
        # interacts badly with following fetches...
        # cursor.executemany(sessions_sql, execution_ids)

        # TODO I used an OrderedDict for de-duplication. Probably need to
        # revisit this.
        # sessions_list = list(OrderedDict.fromkeys([session for session_list in cursor.fetchall() for session in session_list]).keys())

        # Find the execution time limits: the earliest and the latest timestamp
        # of each execution.
        times_sql = "SELECT min(start_time) AS start_t, max(end_time) AS end_t FROM instructions WHERE mal_execution_id=%(eid)s"
        times = list()
        LOGGER.debug("flaf %s", execution_ids)
        for ex_id in execution_ids:
            t = self._db.execute_query(times_sql, {'eid': ex_id})
            LOGGER.debug("=== %s", t)
            times.append([t['start_t'][0], t['end_t'][0]])

        LOGGER.debug("*" * 20)
        LOGGER.debug("Times = %s", times)
        LOGGER.debug("*" * 20)

        return {"token": []}
