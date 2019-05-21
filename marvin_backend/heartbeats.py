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


class HeartBeats(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp):
        all_heartbeats_sql = "SELECT * FROM heartbeat"
        all_heartbeats = self._db.execute_query(all_heartbeats_sql)

        return all_heartbeats


class SingleServerHeartbeats(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint_404_on_empty
    @utils.api_endpoint
    def on_get(self, req, resp, sid):
        server_beats_sql = "SELECT * FROM heartbeat WHERE server_session=%(sid)s"
        server_beats = self._db.execute_query(server_beats_sql, {'sid': sid})

        return server_beats


class CPUload(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp, sid):
        cpuload_sql = "SELECT h.heartbeat_id, h.clk, avg(c.val) AS cpuload FROM cpuload AS c JOIN heartbeat AS h ON c.heartbeat_id=h.heartbeat_id WHERE h.server_session=%(sid)s GROUP BY h.heartbeat_id, h.clk"
        cpuload = self._db.execute_query(cpuload_sql, {'sid': sid})

        return cpuload

