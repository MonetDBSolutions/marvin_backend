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


class Query(object):
    """Endpoints to execute arbitrary queries against the database"""
    def __init__(self, db):
        self._db = db

    def on_put(self, req, resp):
        if not req.content_length:
            # Bad request
            msg = 'JSON body is required for this call'
            doc = {
                'links': {
                    'url': req.url,
                },
                'error': msg,
            }
            resp.status = falcon.HTTP_400
            resp.body = json.dumps(doc, ensure_ascii=False, cls=NumpyJSONEncoder)
            LOGGER.error(msg)
            return

        doc = json.load(req.stream)
        query = doc.get('query')
        params = doc.get('params')
        if params:
            sql_result = self._db.execute_query(query, params)
        else:
            sql_result = self._db.execute_query(query)

        result = DLtoLD(sql_result)

        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
            'data_length': len(result),
        }

        # TODO: pagination
        resp.body = json.dumps(doc, ensure_ascii=False, cls=NumpyJSONEncoder)
        resp.status = falcon.HTTP_200
