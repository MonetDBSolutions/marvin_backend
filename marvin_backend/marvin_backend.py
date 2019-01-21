# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019

# import argparse
import logging
import os
from pathlib import Path
# import sys

import falcon
# import gunicorn.app.base
from mal_analytics import db_manager

from marvin_backend import queries

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

    return api


def get_app(db_path=None):
    curr_path = Path().cwd()
    if db_path is None:
        db_path = os.environ.get('MARVIN_DB_PATH', './marvin_db')

    # This works both if db_path is relative, or absolute.
    actual_path = str((curr_path / db_path).resolve())

    dbm = db_manager.DatabaseManager(actual_path)
    LOGGER.info('Started db_manager at %s', actual_path)

    return create_app(dbm)

# class Marvin()

# def main():
