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
        }

        resp.body = json.dumps(doc, ensure_ascii=False, cls=utils.NumpyJSONEncoder)

        resp.status = falcon.HTTP_200
