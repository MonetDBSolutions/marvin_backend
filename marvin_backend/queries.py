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

LOGGER = logging.getLogger(__name__)


class Queries(object):
    def __init__(self, db):
        self._db = db

    def on_get(self, req, resp):
        all_queries_sql = "SELECT * from query"
        all_queries = self._db.execute_query(all_queries_sql)
        result = utils.DLtoLD(all_queries)

        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
            'data_length': len(result),
        }

        # TODO: pagination
        resp.body = json.dumps(doc, ensure_ascii=False, cls=utils.NumpyJSONEncoder)
        resp.status = falcon.HTTP_200


class SingleQuery(object):
    def __init__(self, db):
        self._db = db

    def on_get(self, req, resp, qid):
        query_sql = "SELECT * FROM query WHERE query_id=%(qid)s"

        query = self._db.execute_query(query_sql, {'qid': qid})
        result = utils.DLtoLD(query)

        if len(result) == 0:
            # No query with the given qid. This is a 404 error.
            resp.status = falcon.HTTP_404
            return

        if len(result) != 1:
            # This cannot happen unless the db constraints in
            # mal_analytics have somehow failed.
            msg = 'Query "{}" (qid={}) returned {} results. We were expecting 1.'.format(query_sql, qid, len(result))
            LOGGER.error(msg)
            doc = {
                'links': {
                    'url': req.url,
                },
                'error': msg
            }
            resp.status = falcon.HTTP_500
            return

        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
            'data_length': len(result),
        }

        resp.body = json.dumps(doc, ensure_ascii=False, cls=utils.NumpyJSONEncoder)
        resp.status = falcon.HTTP_200


class QueryExecutions(object):
    def __init__(self, db):
        self._db = db

    def on_get(self, req, resp, qid):
        executions_sql = """
SELECT sup.worker_id FROM
                     supervises_executions AS sup JOIN query AS q
                     ON sup.supervisor_id = q.supervisor_execution_id
       WHERE q.query_id=%(qid)s
"""

        executions = self._db.execute_query(executions_sql, {'qid': qid})
        result = utils.DLtoLD(executions)

        if len(result) == 0:
            resp.status = falcon.HTTP_404
            return

        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
            'data_length': len(result),
        }

        resp.body = json.dumps(doc, ensure_ascii=False, cls=utils.NumpyJSONEncoder)
        resp.status = falcon.HTTP_200
