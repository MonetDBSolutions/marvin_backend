# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
from marvin_backend import utils

class TestUtils(object):
    def test_dict_to_list(self):
        d = {'key1':[1, 2], 'key2':['a', 'b']}

        l = utils.DLtoLD(d)
        assert l == [{'key1': 1, 'key2': 'a'}, {'key1': 2, 'key2': 'b'}]

    def test_list_to_dict(self):
        l = [{'key1': 1, 'key2': 'a'}, {'key1': 2, 'key2': 'b'}]

        d = utils.LDtoDL(l)
        assert d == {'key1':[1, 2], 'key2':['a', 'b']}
