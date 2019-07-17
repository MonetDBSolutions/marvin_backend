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

    @utils.api_endpoint
    def on_get(self, req, resp):
        all_queries_sql = "SELECT * FROM query"
        all_queries = self._db.execute_query(all_queries_sql)

        return all_queries


class SingleQuery(object):
    def __init__(self, db):
        self._db = db

    def on_patch(self, req, resp, qid):
        if req.content_length:
            doc = json.loads(req.stream.read(req.content_length))
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

    @utils.api_endpoint_singleton_result
    @utils.api_endpoint_404_on_empty
    @utils.api_endpoint
    def on_get(self, req, resp, qid):
        query_sql = "SELECT * FROM query WHERE query_id=%(qid)s"

        query = self._db.execute_query(query_sql, {'qid': qid})
        LOGGER.debug("QUERY=%s", query)
        return query


class QueryExecutions(object):
    def __init__(self, db):
        self._db = db

    # TODO Move this method out of this class. Maybe add a layer that handles
    # all the interactions with the DB?
    def gather_executions(self, qid):
        edges_sql = "SELECT parent_id, child_id FROM initiates_executions"
        start_node_sql = "SELECT root_execution_id FROM query WHERE query_id=%(qid)s"

        all_nodes = self._db.execute_query("SELECT execution_id FROM mal_execution")
        exec_graph_edges = self._db.execute_query(edges_sql)
        start_node = self._db.execute_query(start_node_sql, {'qid': qid})

        LOGGER.debug("*" * 30)
        LOGGER.debug("All nodes: %s", all_nodes['execution_id'])
        LOGGER.debug("All edges: %s", list(zip(exec_graph_edges['parent_id'], exec_graph_edges['child_id'])))
        LOGGER.debug("Start node: %s", start_node)
        LOGGER.debug("Edges data: %s", exec_graph_edges)
        LOGGER.debug("*" * 30)

        if exec_graph_edges["child_id"].size == 0 or not start_node["execution_id"]:
            raise Exception

        return utils.find_query_execution_ids(start_node, exec_graph_edges)  # Do we need to abstract this by passing a function to be executed for every visited node?

    def on_get(self, req, resp, qid):

        try:
            execution_ids = self.gather_executions(qid)
        except Exception:
            resp.status = falcon.HTTP_404
            return

        doc = {
            'links': {
                'url': req.url,
            },
            'data': execution_ids,
            'data_length': len(execution_ids),
        }

        resp.body = json.dumps(doc, ensure_ascii=False, cls=utils.NumpyJSONEncoder)
        resp.status = falcon.HTTP_200
