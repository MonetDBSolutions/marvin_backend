# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019

import argparse
import logging
import os
from pathlib import Path
# import sys

import falcon
import gunicorn.app.base
from gunicorn.six import iteritems
from mal_analytics import db_manager

from marvin_backend import queries, executions, developer, heartbeats, traces
"""Main module."""

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def create_app(manager):
    api = falcon.API()

    # add the endpoints
    # /queries
    all_queries = queries.Queries(manager)
    api.add_route('/queries', all_queries)

    single_query = queries.SingleQuery(manager)
    api.add_route('/queries/{qid}', single_query)

    query_executions = queries.QueryExecutions(manager)
    api.add_route('/queries/{qid}/executions', query_executions)

    all_executions = executions.Executions(manager)
    api.add_route('/executions', all_executions)

    single_execution = executions.SingleExecution(manager)
    api.add_route('/executions/{eid}', single_execution)

    execution_statements = executions.ExecutionStatements(manager)
    api.add_route('/executions/{eid}/statements', execution_statements)

    all_heartbeats = heartbeats.HeartBeats(manager)
    api.add_route('/heartbeats', all_heartbeats)

    server_heartbeats = heartbeats.SingleServerHeartbeats(manager)
    api.add_route('/heartbeats/{sid}', server_heartbeats)

    trace_uploads = traces.Traces(manager)
    api.add_route('/traces', trace_uploads)

    cpu_loads = heartbeats.CPUload(manager)
    api.add_route('/cpuload/{sid}', cpu_loads)

    cpu_loads_per_query = heartbeats.QueryLoad(manager)
    api.add_route('/queries/{qid}/load', cpu_loads_per_query)

    # Mostly for debugging, but could be useful for users as well.
    arbitrary_sql = developer.SQLQuery(manager)
    api.add_route('/developer/query', arbitrary_sql)

    return api


def get_app(database_path=None):  # pragma: no coverage
    curr_path = Path().cwd()
    # db_path = database_path or os.environ.get('MARVIN_DB_PATH', './marvin_db')
    # For dev purposes the default db location is at the
    # db_path/dev_db subdirectory of the current directory.
    db_path = database_path or os.environ.get('MARVIN_DB_PATH',
                                              './db_path/dev_db')

    # This works both if db_path is relative, or absolute.
    actual_path = str((curr_path / db_path).resolve())

    dbm = db_manager.DatabaseManager(actual_path)
    LOGGER.info('Started db_manager at %s', actual_path)

    return create_app(dbm)


class Marvin(gunicorn.app.base.BaseApplication):  # pragma: no coverage
    def __init__(self, app, options=None):
        self._options = options or {}
        self._application = app
        super(Marvin, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self._options)
                       if key in self.cfg.settings and value is not None])

        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self._application


def parse_cli():  # pragma: no coverage
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbpath',
                        '-d',
                        help='Path to the database holding the traces')

    parser.add_argument('--port',
                        '-p',
                        default='8000',
                        help='Port number that the server will listen to')
    parser.add_argument('--workers',
                        '-w',
                        type=int,
                        default=1,
                        help='Number of workers')

    return parser.parse_args()


def main():  # pragma: no coverage
    arguments = parse_cli()
    print(arguments)
    options = {
        'bind': '%s:%s' % ('127.0.0.1', arguments.port),
        'workers': arguments.workers
    }
    Marvin(get_app(arguments.dbpath), options).run()


if __name__ == '__main__':  # pragma: no coverage
    main()
