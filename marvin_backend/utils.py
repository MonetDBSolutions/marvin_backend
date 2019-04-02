# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
from collections import deque
import functools
import json
import logging
import numpy as np

import falcon

LOGGER = logging.getLogger(__name__)


def DLtoLD(dl):
    """Turn a dict of lists into a list of dicts

    This essentially transposes the representation of tabular data
    from column based to row based.

    Args:
        dl: A dictionary of lists

    Returns:
        A list of dictionaries

    Examples:
        >>> DLtoLD({'key1':[1, 2], 'key2':['a', 'b']})
        [{'key1':1, 'key2':'a'}, {'key1':2, 'key2':'b'}]

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

    This essentially transposes the representation of tabular data
    from row based to column based.

    Args:
        ld: A list of dictionaries with the same keys

    Returns:
         A dictionary of lists

    Examples:
        >>> LDtoDL([{'key1':1, 'key2':'a'}, {'key1':2, 'key2':'b'}})
        {'key1':[1, 2], 'key2':['a', 'b']}

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


# Graph utilities: To find all the executions associated with a given
# query we need to traverse the execution graph starting at the
# root_exeqution_id of the query.

# The representation of the graph is just a list of edges. Edges are
# represented by a pair of numbers, that are taken to mean nodes. That
# is if (1, 2) is in the list of edges, then there is an edge from
# node 1 to node 2.
class SimpleGraph(object):
    def __init__(self, edges):
        self._edges = edges

    def bfs(self, start_node):
        ret = list()
        q = deque()
        q.append(start_node)

        while len(q) != 0:
            LOGGER.debug(q)
            n = q.popleft()
            ret.append(n)
            neighbors = [self._head(e) for e in self._edges if self._tail(e) == n and self._head(e) not in ret]
            LOGGER.debug("%d->%s", n, neighbors)
            q.extend(neighbors)

        return ret

    def _head(self, edge):
        return edge[1]

    def _tail(self, edge):
        # edge is a dict with 2 keys: parent_id and child_id. The edge
        # is in the sense parent->child.
        return edge[0]


# The interface of the graph tools to the rest of the system.
def find_query_execution_ids(query_root_execution, execution_relation):
    g = SimpleGraph(list(zip(execution_relation['parent_id'], execution_relation['child_id'])))
    return g.bfs(query_root_execution['execution_id'][0])


def api_endpoint(func):
    @functools.wraps(func)
    def api_endpoint_impl(cls, req, resp, *args, **kwargs):
        result = DLtoLD(func(cls, req, resp, *args, **kwargs))

        print("API DECORATOR", result)
        doc = {
            'links': {
                'url': req.url,
            },
            'data': result,
            'data_length': len(result),
        }

        # TODO: pagination
        resp.body = json.dumps(doc, ensure_ascii=False, cls=NumpyJSONEncoder)
        resp.status = falcon.HTTP_200

        return result

    return api_endpoint_impl

def api_endpoint_404_on_empty(func):
    @functools.wraps(func)
    def api_endpoint_404_on_empty_impl(cls, req, resp, *args, **kwargs):
        result = func(cls, req, resp, *args, **kwargs)

        print("EMPTY DECORATOR", result)
        if not result:
            resp.status = falcon.HTTP_404
            resp.body = None

        return result

    return api_endpoint_404_on_empty_impl

def api_endpoint_singleton_result(func):
    @functools.wraps(func)
    def api_endpoint_singleton_result_impl(cls, req, resp, *args, **kwargs):
        result = func(cls, req, resp, *args, **kwargs)

        print("SINGLETON DECORATOR", result)
        if result and len(result) > 1:
            msg = "Expecting single result, but got {}.".format(len(result))
            doc = {
                'links': {
                    'url': req.url,
                },
                'error': msg,
            }

            resp.status = falcon.HTTP_500

        return result

    return api_endpoint_singleton_result_impl
