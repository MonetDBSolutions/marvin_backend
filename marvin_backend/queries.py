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
        all_queries_q = "SELECT * from query"
        all_queries = self._db.execute_query(all_queries_q)
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
        query_q = "SELECT * FROM query WHERE query_id=%(qid)s"

        query = self._db.execute_query(query_q, {'qid': qid})
        result_t = dict([(k, v.tolist()) for k, v in query.items()])
        result = utils.DLtoLD(result_t)

        if len(result) == 0:
            # No query with the given qid. This is a 404 error.
            resp.status = falcon.HTTP_404
            return

        if len(result) != 1:
            # This cannot happen unless the db constraints in
            # mal_analytics have somehow failed.
            msg = 'Query "{}" (qid={}) returned {} results. We were expecting 1.'.format(query_q, qid, len(result))
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
