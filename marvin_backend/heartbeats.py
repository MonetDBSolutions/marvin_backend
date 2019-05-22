# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019
import logging

from marvin_backend import utils
from marvin_backend import queries

LOGGER = logging.getLogger(__name__)


class HeartBeats(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp):
        all_heartbeats_sql = "SELECT * FROM heartbeat"
        all_heartbeats = self._db.execute_query(all_heartbeats_sql)

        return all_heartbeats


class SingleServerHeartbeats(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint_404_on_empty
    @utils.api_endpoint
    def on_get(self, req, resp, sid):
        server_beats_sql = "SELECT * FROM heartbeat WHERE server_session=%(sid)s"
        server_beats = self._db.execute_query(server_beats_sql, {'sid': sid})

        return server_beats


class CPUload(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp, sid):
        cpuload_sql = "SELECT h.heartbeat_id, h.ctime, avg(c.val) AS cpuload FROM cpuload AS c JOIN heartbeat AS h ON c.heartbeat_id=h.heartbeat_id WHERE h.server_session=%(sid)s GROUP BY h.heartbeat_id, h.clk"
        cpuload = self._db.execute_query(cpuload_sql, {'sid': sid})

        return cpuload


class QueryLoad(object):
    def __init__(self, db):
        self._db = db

    @utils.api_endpoint
    def on_get(self, req, resp, qid):
        # This method is heavily commented because it is written in a hurry and
        # will need to be heavily refactored. Consider creating a data
        # structure for intervals, with getters for start and end time.

        # First find all the executions related to this query
        # TODO I should not have to instantiate this class
        qe = queries.QueryExecutions(self._db)
        execution_ids = list(map(int, qe.gather_executions(qid)))

        # Find the the server sessions of these executions
        sessions_sql = "SELECT server_session FROM mal_execution WHERE execution_id=%(eid)s"
        sessions = set()
        for ex_id in execution_ids:
            s = self._db.execute_query(sessions_sql, {'eid': ex_id})
            sessions.add(s['server_session'][0])

        # TODO probably does not do what it's supposed to do... I suspect it
        # interacts badly with following fetches...
        # cursor.executemany(sessions_sql, execution_ids)

        # Find the execution time limits: the earliest and the latest timestamp
        # of each execution.
        times_sql = "SELECT m.server_session AS server, min(i.astart_time) AS start_t, max(i.aend_time) AS end_t FROM instructions AS i JOIN mal_execution AS m ON i.mal_execution_id=m.execution_id WHERE mal_execution_id=%(eid)s GROUP BY m.server_session"
        times = dict()
        for ex_id in execution_ids:
            t = self._db.execute_query(times_sql, {'eid': ex_id})
            if not times.get(t['server'][0]):
                times[t['server'][0]] = list()
            times[t['server'][0]].append([t['start_t'][0], t['end_t'][0]])

        # TODO the following functionality regarding intervals should be
        # factored out of this class.

        # Consolidate intervals. We could consider using an interval tree [1],
        # but we would only need it for the initialization (no inserts,
        # deletes, dynamic queries etc). Normally if we have $n$ intervals we
        # would need to check $O(n^2)$ pairs. We can however sort the intervals
        # in $O(n\log n)$ by start time and then we would only need to make one
        # sweep through the list.
        #
        # [1] https://en.wikipedia.org/wiki/Interval_tree

        # There are the following cases for two intervals A, B, where A.start_time < B.start_time:
        #   1. The intervals do not overlap (A.end_time < B.start_time)
        #   2. Interval B starts before A finishes (B.start_time < A.end_time)
        #   3. Interval B is enclosed in A (B.end_time < A.end_time)

        timelines = dict()
        # We maintain the invariant that `current_interval` holds A and
        # `interval_iterator` holds B.
        for server, raw_intervals in times.items():
            raw_intervals.sort(
                key=lambda x: x[0])  # Make sure the interval list is sorted
            current_interval = raw_intervals.pop(0)
            intervals = list()

            while raw_intervals:
                interval_iterator = raw_intervals.pop(0)
                # Case 1, non overlapping intervals. Add the current interval
                # to the final list and make the iterator the new current
                # interval.
                if current_interval[1] < interval_iterator[0]:
                    intervals.append(current_interval)
                    current_interval = interval_iterator
                # Case 2: Merge the two intervals by extending A up to the end
                # of B, and discard B.
                elif interval_iterator[0] < current_interval[1]:
                    current_interval[1] = interval_iterator[1]
                # Case 3: B is contained in A, so just discard it. We do not
                # really need to examine this case but I leave it here for
                # completeness.
                elif interval_iterator[1] < current_interval[1]:
                    pass

            # Don't forget to put the last interval in the list
            intervals.append(current_interval)
            timelines[server] = intervals

        # Now that we have the relevant timelines find all the cpuload objects
        # from the database.

        cpuload_sql = "SELECT h.server_session, h.heartbeat_id, h.ctime, avg(c.val) AS cpuload FROM cpuload AS c JOIN heartbeat AS h ON c.heartbeat_id=h.heartbeat_id WHERE h.server_session=%(sid)s AND h.ctime>=%(start_time)s AND h.ctime<%(end_time)s GROUP BY h.heartbeat_id, h.ctime, h.server_session"
        cpuload = dict()
        for server, timeline in timelines.items():
            for interval in timeline:
                result = self._db.execute_query(
                    cpuload_sql, {
                        "sid": server,
                        "start_time": int(interval[0]),
                        "end_time": int(interval[1])
                    })
                for k, v in result.items():
                    if not cpuload.get(k):
                        cpuload[k] = v.tolist()
                    else:
                        cpuload[k].extend(v.tolist())

        return cpuload
