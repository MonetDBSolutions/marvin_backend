# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
from unittest.mock import call

import falcon


class TestTraceEndpoints(object):
    def test_upload_trace(self, client, mock_db):
        trace = b'{"json":"mock json structure"}'
        response = client.simulate_post(
            '/traces',
            body=trace,
            headers={'content-type': 'text/plain'}
        )

        # DatabaseManager::parse_trace does not return anything
        mock_db.parse_trace.assert_called_with(trace.decode('utf-8'))
        assert response.status == falcon.HTTP_CREATED

    def test_upload_wrong_trace(self, client, mock_db):
        bad_trace = b'{"json":"\"bad\" mock json structure"}'

        # DatabaseManager::parse_trace should raise an exception
        mock_db.parse_trace.side_effect = Exception

        response = client.simulate_post(
            '/traces',
            body=bad_trace,
            headers={'content-type': 'text/plain'}
        )

        mock_db.parse_trace.assert_called_with(bad_trace.decode('utf-8'))
        assert response.status == falcon.HTTP_BAD_REQUEST

    def test_upload_empty_trace(self, client, mock_db):
        # DatabaseManager::parse_trace should raise an exception
        mock_db.parse_trace.side_effect = Exception

        response = client.simulate_post(
            '/traces',
        )

        mock_db.parse_trace.assert_called_with("")
        assert response.status == falcon.HTTP_BAD_REQUEST
