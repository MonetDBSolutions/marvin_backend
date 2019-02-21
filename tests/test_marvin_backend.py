#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `marvin_backend` package."""

import falcon
from falcon import testing
from numpy import array
from numpy.ma import masked_array
import pytest
from unittest.mock import call, MagicMock, mock_open

from marvin_backend import marvin_backend

@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def client(mock_db):
    api = marvin_backend.create_app(mock_db)
    return testing.TestClient(api)


def test_queries(client, mock_db):
    query_result = {
        "query_id": array([1, 2]),
        "query_text": array(["SELECT * FROM foo;", "SELECT * FROM bar;"]),
        "query_label": masked_array(data=["null label", "non null label"], mask=[True, False]),
        "root_execution_id": array([1, 40])
    }
    mock_db.execute_query.return_value = query_result

    doc = {
        "data": [
            {
                "query_id": 1,
                "query_label": None,
                "query_text": "SELECT * FROM foo;",
                "root_execution_id": 1
            },
            {
                "query_id": 2,
                "query_text": "SELECT * FROM bar;",
                "query_label": "non null label",
                "root_execution_id": 40
            }
        ],
        "data_length": 2,
        "links": {
            "url": "http://falconframework.org/queries"
        }
    }

    response = client.simulate_get('/queries')
    result_doc = response.json

    print(result_doc)
    assert result_doc == doc
    assert response.status == falcon.HTTP_OK
