# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
import logging
from pathlib import Path

import falcon
from mal_analytics import db_manager

from marvin_backend import queries

"""Main module."""

logging.basicConfig(level=logging.DEBUG)

# For development purposes. These need to be set up properly at
# initialization
CURR_PATH = Path().cwd()
DB_PATH = CURR_PATH / 'db_path/dev_db'

api = application = falcon.API()
db_manager = db_manager.DatabaseManager(str(DB_PATH.resolve()))

all_queries = queries.Queries(db_manager)
api.add_route('/queries', all_queries)
single_query = queries.SingleQuery(db_manager)
api.add_route('/queries/{qid}', single_query)
