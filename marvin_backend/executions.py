# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
import json
import logging

import falcon

from marvin_backend import utils


class Executions(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp):
        all_executions_sql = "SELECT * FROM mal_execution"
        return self._db.execute_query(all_executions_sql)


class SingleExecution(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint_404_on_empty
    @utils.api_endpoint
    def on_get(self, req, resp, eid):
        execution_sql = "SELECT * FROM mal_execution WHERE execution_id=%(eid)s"
        return self._db.execute_query(execution_sql, {'eid': eid})


class ExecutionStatements(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint_404_on_empty
    @utils.api_endpoint
    def on_get(self, req, resp, eid):
        all_statements_sql = "SELECT e.pc, e.short_statement, s.relative_time as start_time, e.relative_time as end_time, e.relative_time - s.relative_time AS duration, e.thread, e.mal_execution_id, e.mal_module, e.instruction FROM (SELECT * FROM profiler_event WHERE execution_state=1 AND mal_execution_id=%(eid)s) AS e JOIN (SELECT * FROM profiler_event AS WHERE execution_state=0 AND mal_execution_id=%(eid)s) s ON e.pc=s.pc AND e.mal_execution_id=s.mal_execution_id ORDER BY start_time ASC"
        return self._db.execute_query(all_statements_sql, {'eid': eid})
