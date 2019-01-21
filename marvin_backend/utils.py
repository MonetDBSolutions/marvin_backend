# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
import json
import numpy as np

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


# numpy types are not JSON serializable, so we need to convert
# everything to a normal python type. Unfortunately this does not work
# with ujson, and this might create a performance issue down the line.
class NumpyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            # this works for masked arrays as well because the masked
            # array is a subtype of np.ndarray
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
