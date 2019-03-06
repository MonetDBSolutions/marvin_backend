# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
import json
import logging

import falcon

from marvin_backend.utils import DLtoLD, NumpyJSONEncoder, find_query_execution_ids


class Executions(object):
    def __init__(self, db):
        self._db = db

    def on_get(self, req, resp):
        all_executions_sql = "SELECT * FROM mal_execution"
        all_executions = self._db.execute_query(all_executions_sql)
        result = DLtoLD(all_executions)

        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
            'data_length': len(result),
        }

        resp.body = json.dumps(doc, ensure_ascii=False, cls=NumpyJSONEncoder)
        resp.status = falcon.HTTP_200


class SingleExecution(object):
    def __init__(self, db):
        self._db = db

    def on_get(self, req, resp, eid):
        execution_sql = "SELECT * FROM mal_execution WHERE execution_id=%(eid)s"
        execution = self._db.execute_query(execution_sql, {'eid': eid})
        result = DLtoLD(execution)

        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
            'data_length': len(result),
        }

        resp.body = json.dumps(doc, ensure_ascii=False, cls=NumpyJSONEncoder)
        resp.status = falcon.HTTP_200

class ExecutionStatements(object):
    def __init__(self, db):
        self._db = db

    def on_get(self, req, resp, eid):
        all_statements_sql = "SELECT pc, short_statement FROM profiler_event WHERE mal_execution_id=%(eid)s AND execution_state=0 ORDER BY pc ASC"
        statements = self._db.execute_query(all_statements_sql, {'eid': eid})
        result = DLtoLD(statements)

        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
            'data_length': len(result),
        }

        resp.body = json.dumps(doc, ensure_ascii=False, cls=NumpyJSONEncoder)
        resp.status = falcon.HTTP_200
