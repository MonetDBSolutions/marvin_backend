# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
import json
import logging

import falcon

LOGGER = logging.getLogger(__name__)


class Traces(object):
    def __init__(self, db):
        self._db = db

    def on_post(self, req, resp):
        # If Content-Length happens to be 0, or the header is missing
        # altogether, this will not block.
        req_body_bytes = req.stream.read(req.content_length or 0)

        LOGGER.debug("Incoming data length %d", len(req_body_bytes))

        try:
            self._db.parse_trace(req_body_bytes.decode("utf-8"))
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
