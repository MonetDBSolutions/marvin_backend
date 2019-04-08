# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
from unittest.mock import MagicMock, call

from falcon import testing
import pytest

from marvin_backend import marvin_backend


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def client(mock_db):
    api = marvin_backend.create_app(mock_db)
    return testing.TestClient(api)
