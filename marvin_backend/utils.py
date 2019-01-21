# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019


def DLtoLD(dl):
    """Turn a dict of lists into a list of dicts

Example:
``DLtoLD({'key1':[1, 2], 'key2':['a', 'b']}) = [{'key1':1, 'key2':'a'}, {'key1':2, 'key2':'b'}]``
    
This essentially transposes the representation of tabular data from
column based to row based.
    
:param dl: A dictionary of lists
:returns: A list of dictionaries

"""

    # 1. dl.values() => [[1, 2], ['a', 'b']]

    # 2. zip(*dl.values()) => [(1, 'a'), (2, 'b')] This will work for
    # any number of lists

    # 3. for t in zip(*dl.values()) => t is assigned each tuple in the
    # above list in turn

    # 4. for k in dl => returns a list of the keys in dl

    # 5. [zip(dl, t) for t in zip(*dl.values())] => produces a list of
    # correct dictionaries provided that the tuples in step 3 and the
    # keys in step 4 are in the correct order. This should be true for
    # Python 3.6 and 3.7, because the order of the keys in the
    # dictionary is guaranteed (unofficially for 3.6) to be the
    # insertion order. For Python 3.5 there is no such guarantee but
    # the order should be consistent.
    return [dict(zip(dl, t)) for t in zip(*dl.values())]


def LDtoDL(ld):
    """Turn a list of dicts into a dict of lists

Example:
``LDtoDL([{'key1':1, 'key2':'a'}, {'key1':2, 'key2':'b'}}) = {'key1':[1, 2], 'key2':['a', 'b']}``

This essentially transposes the representation of tabular data from
row based to column based.

:param ld: A list of dictionaries with the same keys
:returns: A dictionary of lists
"""

    return {k: [dic[k] for dic in ld] for k in ld[0]}
