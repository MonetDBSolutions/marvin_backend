#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `marvin_backend` package."""

import falcon
from falcon import testing
from numpy import array
from numpy.ma import masked_array
import pytest
from unittest.mock import MagicMock, call

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

    mock_db.execute_query.assert_called_with("SELECT * FROM query")
    assert result_doc == doc
    assert response.status == falcon.HTTP_OK


def test_single_query(client, mock_db):
    query_result = {
        "query_id": array([1]),
        "query_text": array(["SELECT * FROM foo;"]),
        "query_label": masked_array(data=["null"], mask=[True]),
        "root_execution_id": array([1])
    }
    mock_db.execute_query.return_value = query_result

    doc = {
        "data": [
            {
                "query_id": 1,
                "query_label": None,
                "query_text": "SELECT * FROM foo;",
                "root_execution_id": 1
            }
        ],
        "data_length": 1,
        "links": {
            "url": "http://falconframework.org/queries/1"
        }
    }

    response = client.simulate_get("/queries/1")
    result_doc = response.json

    mock_db.execute_query.assert_called_with("SELECT * FROM query WHERE query_id=%(qid)s", {'qid': '1'})
    assert result_doc == doc
    assert response.status == falcon.HTTP_OK


def test_single_query_nonexistent(client, mock_db):
    mock_db.execute_query.return_value = dict()

    response = client.simulate_get("/queries/8")
    mock_db.execute_query.assert_called_with("SELECT * FROM query WHERE query_id=%(qid)s", {'qid': '8'})
    assert response.status == falcon.HTTP_NOT_FOUND


def test_single_query_handles_bad_results(client, mock_db):
    query_result = {
        "query_id": array([1, 2]),
        "query_text": array(["SELECT * FROM foo;", "SELECT * FROM bar;"]),
        "query_label": masked_array(data=["null label", "non null label"], mask=[True, False]),
        "root_execution_id": array([1, 40])
    }

    mock_db.execute_query.return_value = query_result

    response = client.simulate_get("/queries/1")
    mock_db.execute_query.assert_called_with("SELECT * FROM query WHERE query_id=%(qid)s", {'qid': '1'})
    assert response.status == falcon.HTTP_INTERNAL_SERVER_ERROR


def test_set_label(client, mock_db):
    response = client.simulate_patch("/queries/1", body='{"label":"foo"}')

    mock_db.execute_query.assert_called_with("UPDATE query SET query_label=%(label)s WHERE query_id=%(qid)s", dict([("label", "foo"), ("qid", "1")]))
    assert response.status == falcon.HTTP_OK


def test_set_label_fails_without_body(client):
    response = client.simulate_patch("/queries/1")

    assert response.status == falcon.HTTP_BAD_REQUEST


def test_set_label_fails_without_label_in_body(client):
    response = client.simulate_patch("/queries/1", body='{"foo": 1}')

    assert response.status == falcon.HTTP_BAD_REQUEST


def test_get_executions(client, mock_db):
    db_results = [
        # run a query that gives back the call graph nodes
        {'execution_id': array([1, 2, 3, 4, 5])},
        # result of a query that gets the edges
        {
            "parent_id": array([1, 1, 2, 3, 5]),
            "child_id":  array([1, 2, 3, 4, 5])
        },
        {
            "execution_id": array([1])
        }
    ]

    mock_db.execute_query.side_effect = db_results
    response = client.simulate_get("/queries/1/executions")

    doc = {
        "data": [
            1,
            2,
            3,
            4
        ],
        "data_length": 4,
        "links": {
            "url": "http://falconframework.org/queries/1/executions"
        }
    }

    calls = [
        call("SELECT execution_id FROM mal_execution"),
        call("SELECT parent_id, child_id FROM initiates_executions"),
        call("""
SELECT e.execution_id FROM
            mal_execution AS e JOIN query AS q
            ON e.execution_id = q.root_execution_id
        WHERE q.query_id=%(qid)s""", {"qid":"1"})
    ]

    mock_db.execute_query.assert_has_calls(calls)
    assert mock_db.execute_query.call_count == 3
    assert response.json == doc
    assert response.status == falcon.HTTP_OK


def test_get_executions_bad_qid(client, mock_db):
    db_results = [
        # run a query that gives back the call graph nodes
        {'execution_id': array([1, 2, 3, 4, 5])},
        # result of a query that gets the edges
        {
            "parent_id": array([]),
            "child_id":  array([])
        },
        {
            "execution_id": array([])
        }
    ]

    mock_db.execute_query.side_effect = db_results
    response = client.simulate_get("/queries/8/executions")

    calls = [
        call("SELECT execution_id FROM mal_execution"),
        call("SELECT parent_id, child_id FROM initiates_executions"),
        call("""
SELECT e.execution_id FROM
            mal_execution AS e JOIN query AS q
            ON e.execution_id = q.root_execution_id
        WHERE q.query_id=%(qid)s""", {"qid":"8"})
    ]

    mock_db.execute_query.assert_has_calls(calls)

    assert mock_db.execute_query.call_count == 3
    assert response.status == falcon.HTTP_NOT_FOUND
