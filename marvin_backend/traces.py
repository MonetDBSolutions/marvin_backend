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


class Traces(object):
    def __init__(self, db):
        self._db = db

    def on_post(self, req, resp):
        # If Content-Length happens to be 0, or the header is missing
        # altogether, this will not block.
        trace_string = req.stream.read(req.content_length or 0)

        try:
            self._db.parse_trace(trace_string)
            resp.status = falcon.HTTP_CREATED
            resp.body = None
        except Exception as e:
            doc = {
                'links': {
                },
                'error': list(e.args)
            }
            resp.body = json.dumps(doc)
            resp.status = falcon.HTTP_BAD_REQUEST
