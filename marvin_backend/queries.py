# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
import json
import logging
# TODO: Refactor001 remove this
from collections import deque

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

    def on_put(self, req, resp, qid):
        if req.content_length:
            doc = json.load(req.stream)
            label = doc.get('label')
            if label is None:
                # Bad request
                msg = 'Field "label" required in body'
                doc = {
                    'links': {
                        'url': req.url,
                    },
                    'error': msg,
                }
                resp.status = falcon.HTTP_400
                resp.body = json.dumps(doc, ensure_ascii=False, cls=utils.NumpyJSONEncoder)
                LOGGER.error(msg)
                return
        else:
            # Bad request
            msg = 'JSON body is required for this call'
            doc = {
                'links': {
                    'url': req.url,
                },
                'error': msg,
            }
            resp.status = falcon.HTTP_400
            resp.body = json.dumps(doc, ensure_ascii=False, cls=utils.NumpyJSONEncoder)
            LOGGER.error(msg)
            return

        # BUG @ mal_analytics: the following sql has an error (qid is
        # an unknown identifier), but we are not notified that
        # something has gone wrong. We need to get an exception here.
        # add_label_sql = "UPDATE query SET query_label=%(label)s WHERE qid=%(qid)s"

        add_label_sql = "UPDATE query SET query_label=%(label)s WHERE query_id=%(qid)s"
        self._db.execute_query(add_label_sql, dict([("label", label), ("qid", qid)]))

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
#         # The query is wrong. We need the transitive closure of the
#         # tree (maybe forrest?).
#         executions_sql = """
# SELECT sup.child_id FROM
#                      initiates_executions AS sup JOIN query AS q
#                      ON sup.parent_id = q.root_execution_id
#        WHERE q.query_id=%(qid)s
# """

        edges_sql = "SELECT parent_id, child_id FROM initiates_executions"
        start_node_sql = """
SELECT e.execution_id FROM
            mal_execution AS e JOIN query AS q
            ON e.execution_id = q.root_execution_id
        WHERE q.query_id=%(qid)s"""

        exec_graph_edges = self._db.execute_query(edges_sql, {'qid': qid})
        start_node = self._db.execute_query(start_node_sql, {'qid': qid})

        LOGGER.debug("Start node: %s", start_node)
        LOGGER.debug("Edges data: %s", exec_graph_edges)

        if len(exec_graph_edges["child_id"]) == 0:
            resp.status = falcon.HTTP_404
            return

        execution_ids = find_query_execution_ids(start_node, exec_graph_edges)  # Do we need to abstract this by passing a function to be executed for every visited node?

        doc = {
            'links': {
                'url': req.url,
            },
            'data': execution_ids,
            'data_length': len(execution_ids),
        }

        resp.body = json.dumps(doc, ensure_ascii=False, cls=utils.NumpyJSONEncoder)
        resp.status = falcon.HTTP_200

    # TODO: Refactor001 this. It needs to move out of this class
    @staticmethod
    def _tail(edge):
        # edge is a dict with 2 keys: parent_id and child_id. The edge
        # is in the sense parent->child.
        return edge[0]

    @staticmethod
    def _head(edge):
        return edge[1]

    @staticmethod
    def _transform(edges_data):
        return zip(edges_data['parent_id'], edges_data['child_id'])

    @staticmethod
    def _bfs(start_node, edges_data):
        ret = list()
        q = deque()
        q.append(start_node)

        LOGGER.debug("edges_data = %s", QueryExecutions._transform(edges_data))
        while len(q) != 0:
            LOGGER.debug("q = %s", q)
            n = q.popleft()
            ret.append(n)
            neighbors = [QueryExecutions._head(e) for e in QueryExecutions._transform(edges_data) if QueryExecutions._tail(e) == n and QueryExecutions._head(e) not in ret]
            LOGGER.debug("head = %d tails = %s", n, neighbors)
            q.extend(neighbors)
            LOGGER.debug("q = %s", q)

        return ret
