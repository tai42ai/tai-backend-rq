"""The heartbeat-freshness contract that decides whether a registered RQ worker
is actually alive.

RQ registers a worker's death only on a graceful exit: a worker killed with
SIGKILL never runs ``register_death``, so its ``rq:worker:<name>`` registry entry
survives until the key's own expiry (``worker_ttl + 60``, minutes) and
``Worker.all()`` keeps returning it. A running worker instead refreshes
``last_heartbeat`` on every idle dequeue poll, and every ``job_monitoring_interval``
while it runs a job — the forking worker from its horse monitor, the non-forking one
from its heartbeat thread (both in :mod:`tai_backend_rq.worker`). So a heartbeat
older than the window below is what proves a registered worker is a registry ghost
rather than a live process.

One window, one contract: the ``backend_*`` worker tools report liveness with it,
so a registered worker the tool surface calls alive is one whose heartbeat is
fresh rather than a registry ghost.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from rq.defaults import DEFAULT_JOB_MONITORING_INTERVAL

# The longest gap a HEALTHY worker leaves between two heartbeats is one
# ``job_monitoring_interval`` — its in-job refresh tick (an idle worker refreshes far
# more often, once per dequeue poll). The window is that gap plus RQ's own 60s
# margin, the same ``interval + 60`` formula RQ sizes the heartbeat key's TTL with,
# so freshness and RQ's key expiry cannot disagree: a fresh heartbeat implies the
# registry key still exists. A busy worker therefore never reads as dead, while a
# SIGKILLed one leaves the census in far less than the registry entry's own
# multi-minute expiry.
HEARTBEAT_FRESH_SECONDS = DEFAULT_JOB_MONITORING_INTERVAL + 60


def heartbeat_fresh(last_heartbeat: datetime | None) -> bool:
    """Whether a worker's ``last_heartbeat`` is recent enough to call it live.

    A worker with no recorded heartbeat is never live. A naive timestamp is read
    as UTC — RQ stores heartbeats in UTC.
    """
    if last_heartbeat is None:
        return False
    if last_heartbeat.tzinfo is None:
        last_heartbeat = last_heartbeat.replace(tzinfo=UTC)
    return (datetime.now(UTC) - last_heartbeat) < timedelta(seconds=HEARTBEAT_FRESH_SECONDS)
