# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
from unittest.mock import call

import falcon


class TestExecutionEndpoints(object):
    def test_all_executions(self, client, mock_db):
        query_result = {
            "execution_id": [1 ,2],
            "server_session": ["f5b3c0cd-94a1-45d6-9630-0260154531f7",
                               "f49790dc-f4ff-4883-831c-ddfd01d86a2d"],
            "tag": [1, 1],
            "server_version": ["11.32.0 (hg id: b29eefa306fc)", "11.32.0 (hg id: b29eefa306fc)"],
            "user_function": ["L1", "L1"]
        }
        mock_db.execute_query.return_value = query_result

        doc = {
            "data": [
                {
                    "execution_id": 1,
                    "server_session": "f5b3c0cd-94a1-45d6-9630-0260154531f7",
                    "tag": 1,
                    "server_version": "11.32.0 (hg id: b29eefa306fc)",
                    "user_function": "L1"
                },
                {
                    "execution_id": 2,
                    "server_session": "f49790dc-f4ff-4883-831c-ddfd01d86a2d",
                    "tag": 1,
                    "server_version": "11.32.0 (hg id: b29eefa306fc)",
                    "user_function": "L1"
                },
            ],
            "data_length": 2,
            "links": {
                "url": "http://falconframework.org/executions"
            }
        }

        response = client.simulate_get('/executions')
        result_doc = response.json

        mock_db.execute_query.assert_called_with("SELECT * FROM mal_execution")
        assert result_doc == doc
        assert response.status == falcon.HTTP_OK

    def test_single_execution(self, client, mock_db):
        query_result = {
            "execution_id": [1],
            "server_session": ["f5b3c0cd-94a1-45d6-9630-0260154531f7"],
            "tag": [1],
            "server_version": ["11.32.0 (hg id: b29eefa306fc)"],
            "user_function": ["L1"]
        }
        mock_db.execute_query.return_value = query_result

        doc = {
            "data": [
                {
                    "execution_id": 1,
                    "server_session": "f5b3c0cd-94a1-45d6-9630-0260154531f7",
                    "tag": 1,
                    "server_version": "11.32.0 (hg id: b29eefa306fc)",
                    "user_function": "L1"
                },
            ],
            "data_length": 1,
            "links": {
                "url": "http://falconframework.org/executions/1"
            }
        }

        response = client.simulate_get('/executions/1/')
        result_doc = response.json

        mock_db.execute_query.assert_called_with("SELECT * FROM mal_execution WHERE execution_id=%(eid)s", {'eid': '1'})
        assert result_doc == doc
        assert response.status == falcon.HTTP_OK

    def test_execution_statements(self, client, mock_db):
        query_result = {
            'pc': [1, 2, 3],
            'short_statement': ['X_1:=inst_a(x, y)', 'X_2:=inst_b(z)', 'X_3:=inst_c()'],
            'start_time': [1, 27, 42],
            'end_time': [7, 79, 45],
            'duration': [6, 52, 3],
            'thread': [2, 4, 6],
            'mal_execution_id': [1, 1, 1],
            'start_event_id': [1, 2, 3],
            'end_event_id': [4, 6, 5],
            'mal_module': ['mod_a', 'mod_a', 'mod_c'],
            'instruction': ['inst_a', 'inst_b', 'inst_c']
        }
        mock_db.execute_query.return_value = query_result

        doc = {
            "data": [
                {
                    "pc": 1,
                    "short_statement": "X_1:=inst_a(x, y)",
                    "start_time": 1,
                    "end_time": 7,
                    "duration": 6,
                    "thread": 2,
                    "mal_execution_id": 1,
                    "start_event_id": 1,
                    "end_event_id": 4,
                    "mal_module": "mod_a",
                    "instruction": "inst_a"
                },
                {
                    "pc": 2,
                    "short_statement": "X_2:=inst_b(z)",
                    "start_time": 27,
                    "end_time": 79,
                    "duration": 52,
                    "thread": 4,
                    "mal_execution_id": 1,
                    "start_event_id": 2,
                    "end_event_id": 6,
                    "mal_module": "mod_a",
                    "instruction": "inst_b"
                },
                {
                    "pc": 3,
                    "short_statement": "X_3:=inst_c()",
                    "start_time": 42,
                    "end_time": 45,
                    "duration": 3,
                    "thread": 6,
                    "mal_execution_id": 1,
                    "start_event_id": 3,
                    "end_event_id": 5,
                    "mal_module": "mod_c",
                    "instruction": "inst_c"
                },
            ],
            "data_length": 3,
            "links": {
                "url": "http://falconframework.org/executions/1/statements"
            }
        }

        response = client.simulate_get('/executions/1/statements/')
        result_doc = response.json

        mock_db.execute_query.assert_called_with("SELECT * FROM instructions WHERE mal_execution_id=%(eid)s", {'eid': '1'})
        assert result_doc == doc
        assert response.status == falcon.HTTP_OK
