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
        # numpy arrays are not JSON serializable, so convert
        # everything to a normal python list
        result_t = dict([(k, v.tolist()) for k, v in all_queries.items()])
        result = utils.DLtoLD(result_t)

        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
        }

        resp.body = json.dumps(doc, ensure_ascii=False)

        resp.status = falcon.HTTP_200
