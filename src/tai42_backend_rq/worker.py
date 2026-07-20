"""The RQ worker runtime: worker classes, fork safety, and the ``worker``
launch entrypoint.

An RQ worker process receives fleet ops through the app's own worker bus (the
single long-lived subscription the app context spawns before ``launch`` runs),
exactly like a serving HTTP worker — the backend carries no control plane of its
own.

``launch(["worker", ...])`` runs the blocking ``worker.work()`` loop on a
worker thread (:func:`run_rq_worker`), keeping the process's event loop
responsive: the app's bus subscription lives on that loop, so a blocked loop
would starve op delivery.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import threading
import time
import urllib.request
from typing import Any

import click
from redis import Redis
from rq import Queue, SimpleWorker, Worker
from rq.exceptions import StopRequested
from rq.timeouts import HorseMonitorTimeoutException, TimerDeathPenalty, UnixSignalDeathPenalty
from rq.worker import WorkerStatus
from tai42_contract.app import tai42_app

from tai42_backend_rq.settings import rq_settings

logger = logging.getLogger(__name__)

# Upper bound on one idle dequeue block. rq's default poll blocks for
# ``worker_ttl - 15`` seconds and relies on a main-thread signal to interrupt
# it; this worker runs off the main thread, where a warm-shutdown request only
# takes effect when the work loop next checks its stop flag — so each poll is
# kept short to bound the shutdown latency.
_DEQUEUE_POLL_SECONDS = 5

# How often the prefork parent re-checks whether its work-horse has exited. Short
# enough that the monitor loop wakes promptly on the horse's exit, and that its
# self-imposed ``job_monitoring_interval`` deadline is hit within one tick.
_HORSE_POLL_SECONDS = 0.25

# Upper bound on the wait for the app to become ready before the work loop starts
# (see ``run_rq_worker``). The app latches ready the instant its boot self-resync
# rebuilds the tool registry — seconds under any healthy boot — so this is a loud
# backstop for a boot that never completes (a dead bus subscription), never the
# expected path: a worker that timed out here would fork against a half-built
# registry, so it refuses and exits instead.
_APP_READY_TIMEOUT_SECONDS = 120


class _TaiWorkerMixin:
    """Worker behavior shared by both worker classes.

    * Skips rq's signal-handler install when the work loop runs off the main
      thread (``signal.signal`` requires the main thread; shutdown then
      arrives via :func:`request_warm_shutdown` instead).
    * Bounds each idle dequeue poll (see ``_DEQUEUE_POLL_SECONDS``) so a
      warm-shutdown request from another thread is honored within seconds.
    """

    name: str  # provided by the RQ worker base class

    # The work loop runs off the main thread (``run_rq_worker`` keeps the event loop
    # responsive), where rq's signal-based death penalty cannot be armed at all
    # (``signal.signal`` raises ``ValueError`` off the main thread). The timer-based
    # penalty is the thread-agnostic one — the same class rq selects on platforms
    # without SIGALRM — so it is what this worker arms in the PARENT process: the
    # in-process ``SimpleWorker`` job timeout, and the stopped-job callback.
    #
    # Its limits are real and are worked around where they bite: an async exception
    # is only delivered at a Python bytecode boundary, so it can interrupt neither a
    # thread blocked in a C call nor C-bound job code. The prefork parent's horse
    # monitor therefore does NOT rely on it (``CustomRQWorker.wait_for_horse`` polls
    # and times itself out), and the work-horse child restores the signal penalty
    # (``CustomRQWorker.main_work_horse``), where SIGALRM works and does interrupt a
    # C-blocked job.
    death_penalty_class = TimerDeathPenalty

    def _install_signal_handlers(self) -> None:
        if threading.current_thread() is threading.main_thread():
            super()._install_signal_handlers()  # pyright: ignore[reportAttributeAccessIssue]
            return
        logger.info(
            "worker %r runs off the main thread; shutdown is driven by warm-shutdown requests, not signal handlers",
            self.name,
        )

    @property
    def dequeue_timeout(self) -> int:
        return _DEQUEUE_POLL_SECONDS


class CustomRQWorker(_TaiWorkerMixin, Worker):
    """The prefork worker: RQ's native forking ``execute_job`` (a monitored
    work-horse child per job, with timeout enforcement)."""

    def __init__(self, queues: Any, *args: Any, **kwargs: Any) -> None:
        super().__init__(queues, *args, **kwargs)
        # The horse pid ``kill_horse`` last targeted: the monitor calls
        # ``wait_for_horse`` a SECOND time after ``kill_horse``, as an unbounded final
        # reap, and that call must not self-timeout (see ``wait_for_horse``).
        self._killed_horse_pid = 0

    def fork_work_horse(self, job: Any, queue: Any) -> None:
        # Each new horse starts un-killed: clear the last-killed pid so a pid the OS
        # later reuses for a fresh horse can never be mistaken for the killed one (which
        # would make ``wait_for_horse`` skip its self-timeout for a healthy job).
        self._killed_horse_pid = 0
        super().fork_work_horse(job, queue)

    def kill_horse(self, sig: signal.Signals = signal.SIGKILL) -> None:
        self._killed_horse_pid = self.horse_pid
        super().kill_horse(sig)

    def wait_for_horse(self) -> tuple[int | None, int | None, Any | None]:
        """Reap the work-horse, POLLING rather than blocking, so the parent's
        monitor loop keeps ticking while a job runs.

        RQ's monitor wraps this call in a death penalty sized to
        ``job_monitoring_interval`` and, when it fires, refreshes the worker /
        execution / job heartbeats and enforces the runaway-horse backstop. Off the
        main thread that penalty can only be the timer one, whose async exception is
        never delivered to a thread blocked in ``os.wait4`` — so a blocking reap
        would silence the monitor for the WHOLE job: no heartbeat (the census would
        drop a live worker, and the execution's registry entry would expire and be
        abandoned mid-run) and no runaway backstop.

        Polling with ``WNOHANG`` and raising ``HorseMonitorTimeoutException`` here
        keeps the loop cycling on real time, independent of any signal — the deadline
        is one tick SHORT of the interval, so the ambient timer penalty is always
        cancelled before it could fire. BUT the monitor also calls this a second time,
        AFTER ``kill_horse``, as an unbounded final reap OUTSIDE any penalty context;
        raising there would escape ``monitor_work_horse`` and drop the job's failure
        handling. So once we have killed the current horse, poll until it is reaped
        and never self-timeout — matching RQ's own blocking reap on that path.
        """
        reaping_killed_horse = self._killed_horse_pid == self.horse_pid
        deadline = time.monotonic() + max(self.job_monitoring_interval - _HORSE_POLL_SECONDS, _HORSE_POLL_SECONDS)
        while True:
            try:
                pid, stat, rusage = os.wait4(self.horse_pid, os.WNOHANG)
            except ChildProcessError:
                # Already reaped (rq's own semantics for a vanished horse).
                return None, None, None
            if pid:
                return pid, stat, rusage
            if not reaping_killed_horse and time.monotonic() >= deadline:
                raise HorseMonitorTimeoutException
            time.sleep(_HORSE_POLL_SECONDS)

    def main_work_horse(self, job: Any, queue: Any) -> None:
        """Run the forked child's work loop under rq's SIGNAL death penalty.

        The forking thread becomes the child's main thread, so ``signal.signal``
        works here even though it does not in the parent — and only SIGALRM can
        interrupt job code blocked in a C call, which is what a job timeout must do.
        """
        self.death_penalty_class = UnixSignalDeathPenalty
        super().main_work_horse(job, queue)

    def perform_job(self, job: Any, queue: Any) -> bool:
        """Run one job in the work-horse child, flushing monitoring before exit.

        The work-horse leaves via ``os._exit``, which skips every atexit path —
        without an explicit flush, the spans buffered by the child's
        lazily-rebuilt monitoring client would be silently lost. The monitoring
        contract's ``flush`` propagates errors, so a failed flush is logged at
        ERROR here (raising would turn a finished job's outcome into a horse
        failure) — the span loss is surfaced, never silent.
        """
        try:
            return super().perform_job(job, queue)
        finally:
            try:
                tai42_app.monitoring.active.writer.flush()
            except Exception:
                logger.error(
                    "work-horse %s: monitoring flush failed; buffered spans are lost", os.getpid(), exc_info=True
                )


class CustomRQSimpleWorker(_TaiWorkerMixin, SimpleWorker):
    """The non-forking worker (``solo`` and ``gevent`` pools): jobs run in the
    worker process itself."""

    def execute_job(self, job: Any, queue: Any) -> None:
        """Run the job in this process, refreshing the heartbeats while it runs.

        A forking worker gets this from the parent's horse monitor; a non-forking
        one has no monitor — the work loop IS the job — so nothing would refresh the
        worker/execution/job heartbeats for the job's whole duration. A refresher
        thread ticks them on the same ``job_monitoring_interval`` the forking worker
        uses, so both pools honour ONE liveness contract: a live worker is a worker
        whose heartbeat is fresh, busy or idle.
        """
        self.prepare_execution(job)
        done = threading.Event()
        beat = threading.Thread(
            target=self._maintain_heartbeats_until, args=(job, done), name=f"rq-heartbeat-{job.id}", daemon=True
        )
        beat.start()
        try:
            self.perform_job(job, queue)
        finally:
            done.set()
            beat.join(timeout=self.job_monitoring_interval)
        self.set_state(WorkerStatus.IDLE)

    def _maintain_heartbeats_until(self, job: Any, done: threading.Event) -> None:
        """Refresh this worker's heartbeats every monitoring interval until the job
        finishes. A failed refresh is logged at ERROR and retried: it cannot be
        swallowed silently — a heartbeat that stops means the census drops this
        worker and the next fleet op fails loudly."""
        while not done.wait(self.job_monitoring_interval):
            try:
                self.maintain_heartbeats(job)
            except Exception:
                logger.error("worker %r: heartbeat refresh failed while running %s", self.name, job.id, exc_info=True)


def setup_gevent() -> None:
    """Monkey-patch blocking primitives so the gevent pool can overlap
    I/O-bound jobs inside the single non-forking worker process."""
    from gevent import monkey

    monkey.patch_all()


def _after_fork_in_work_horse() -> None:
    """Fork-safety for every work-horse child.

    The monitoring writer is evicted (``shutdown`` is the contract's fork-safe
    evict): the parent's vendor client carries background threads that do not
    survive ``fork()`` and would hang every flush in the child. The child
    rebuilds a clean client lazily on first use — telemetry is not silently
    disabled, and this log line records the evict.
    """
    logger.info("work-horse %s: evicting inherited monitoring client (fork safety); it rebuilds lazily", os.getpid())
    tai42_app.monitoring.active.writer.shutdown()


_fork_hooks_installed = False


def prepare_forking_worker() -> None:
    """One-time fork-safety setup before a prefork worker starts forking.

    Drops the monitoring client in the parent (its exporter's background
    thread would not survive ``fork()`` and would hang flushes in every
    child), installs the per-child evict hook, and on macOS disables system
    proxy detection — ``urllib``'s proxy lookup calls into platform frameworks
    that deadlock in a forked child the moment SSL initializes.
    """
    global _fork_hooks_installed
    if sys.platform == "darwin":
        logger.warning(
            "macOS forking worker: disabling system proxy detection "
            "(urllib.getproxies deadlocks in forked work-horses during SSL setup)"
        )
        os.environ["no_proxy"] = "*"
        urllib.request.getproxies = dict
    logger.warning(
        "forking worker: shutting down the monitoring writer before the first fork; "
        "each work-horse rebuilds its own client lazily"
    )
    tai42_app.monitoring.active.writer.shutdown()
    if not _fork_hooks_installed:
        os.register_at_fork(after_in_child=_after_fork_in_work_horse)
        _fork_hooks_installed = True


def _build_worker(
    redis_url: str | None,
    name: str | None,
    results_ttl: int,
    pool: str,
) -> tuple[CustomRQWorker | CustomRQSimpleWorker, Redis]:
    """Build the RQ worker for the selected pool; returns it with its
    dedicated connection (closed by the caller after the worker exits).

    ``prefork`` (the default) forks a monitored work-horse per job; ``solo``
    runs jobs in-process; ``gevent`` runs jobs in-process on green threads.
    ``results_ttl`` becomes the worker's default result TTL, applied to every
    finished job that did not set its own. A ``name`` of ``None`` lets RQ
    auto-generate a unique per-process worker name.
    """
    url = redis_url or rq_settings().redis_url

    if pool == "gevent":
        setup_gevent()
        worker_class: type[CustomRQWorker | CustomRQSimpleWorker] = CustomRQSimpleWorker
    elif pool == "solo":
        worker_class = CustomRQSimpleWorker
    else:  # prefork
        worker_class = CustomRQWorker
        prepare_forking_worker()

    redis_conn = Redis.from_url(url)
    queue = Queue(connection=redis_conn)
    worker = worker_class([queue], name=name, connection=redis_conn, default_result_ttl=results_ttl)
    return worker, redis_conn


def request_warm_shutdown(worker: Any) -> None:
    """Ask a worker whose work loop runs on another thread for a warm shutdown.

    On the main thread this drives rq's own signal path: ``request_stop``
    marks a BUSY worker to stop after the current job (and re-binds the
    process signal handlers so a repeated signal escalates to rq's cold
    shutdown). When the worker is idle, rq instead raises ``StopRequested`` —
    an exception meant to unwind the work loop on the thread that runs it,
    which this caller is not — so the stop flag is set here directly.

    Off the main thread (a task cancellation delivered on a non-main-thread
    event loop), rq's ``request_stop`` cannot run at all: its signal re-bind
    (``signal.signal``) is main-thread-only and would raise ``ValueError``.
    The stop flag is set directly instead. ``_stop_requested`` is the flag
    rq's own ``_shutdown`` sets; the work loop honors it on its next dequeue
    poll (bounded by ``_DEQUEUE_POLL_SECONDS``) and its teardown stops the
    embedded scheduler. rq exposes no public cross-thread stop.
    """
    logger.warning("rq worker %r: warm shutdown requested", worker.name)
    if threading.current_thread() is not threading.main_thread():
        worker._stop_requested = True
        return
    try:
        worker.request_stop(signal.SIGTERM, None)
    except StopRequested:
        worker._stop_requested = True


async def run_rq_worker(
    redis_url: str | None,
    name: str | None,
    loglevel: str,
    burst: bool,
    results_ttl: int,
    pool: str,
) -> None:
    """Run the RQ worker on a worker thread, keeping this event loop alive.

    The app's worker-bus subscription lives on this loop, delivering fleet ops
    to this process. Running the blocking ``worker.work()`` inline would freeze
    the loop and starve op delivery, so the work loop runs off-loop.

    Loop-level SIGTERM/SIGINT handlers drive rq's warm shutdown (rq's own
    handlers require the work loop's thread to be the main thread); if this
    coroutine is cancelled instead, the same warm shutdown is requested and the
    worker's exit awaited, so the process never leaves a live work loop behind.
    """
    # The app's tool registry is (re)built by the boot self-resync running
    # concurrently with this launch on the app context's loop. A prefork worker that
    # dequeued a job before it finished would fork a work-horse against a half-built
    # registry, and the job's ``run_tool`` would raise ``UnknownToolError`` — which rq
    # fails permanently, never requeuing. So the work loop must not consume anything
    # until the app is ready. Await the readiness latch first; a timeout here is a boot
    # that never completed (e.g. a dead bus subscription) — fail loudly rather than
    # accept work this worker cannot serve.
    try:
        await asyncio.wait_for(tai42_app.lifecycle.wait_until_ready(), timeout=_APP_READY_TIMEOUT_SECONDS)
    except TimeoutError as exc:
        raise RuntimeError(
            f"rq worker: the app did not become ready within {_APP_READY_TIMEOUT_SECONDS}s "
            "(boot self-resync never completed); refusing to consume jobs against a half-built tool registry"
        ) from exc

    worker, redis_conn = _build_worker(redis_url, name, results_ttl, pool)
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, request_warm_shutdown, worker)
        except (RuntimeError, ValueError, NotImplementedError) as exc:
            # Loop signal handlers need the main thread (and POSIX); without
            # them shutdown must come via task cancellation.
            logger.warning("cannot install %s handler for worker shutdown: %s", sig.name, exc)
        else:
            installed.append(sig)

    work = asyncio.ensure_future(
        asyncio.to_thread(worker.work, burst=burst, logging_level=loglevel.upper(), with_scheduler=True)
    )
    try:
        await asyncio.shield(work)
    except asyncio.CancelledError:
        request_warm_shutdown(worker)
        await work
        raise
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)
        redis_conn.close()


def start_rq_worker(
    redis_url: str | None,
    name: str | None,
    loglevel: str,
    burst: bool,
    results_ttl: int,
    pool: str,
) -> None:
    """Build and run the RQ worker inline, blocking the calling thread.

    The direct CLI path: on the main thread rq installs its own signal
    handlers, so warm/cold shutdown behaves exactly as a plain ``rq worker``.
    (The host ``launch`` path uses :func:`run_rq_worker` instead, which keeps
    the event loop responsive.)
    """
    worker, redis_conn = _build_worker(redis_url, name, results_ttl, pool)
    try:
        worker.work(burst=burst, logging_level=loglevel.upper(), with_scheduler=True)
    finally:
        redis_conn.close()


@click.command("tai-backend-rq-worker")
@click.option("--redis-url", default=None, help="Redis URL (default: the RQ_REDIS_URL setting)")
@click.option(
    "--name",
    "-n",
    default=None,
    help="Worker name (default: an auto-generated unique name). RQ refuses a second "
    "active worker registered under an existing name, so a fixed name would block "
    "running multiple workers and restarting a SIGKILLed one within its registry TTL.",
)
@click.option("--loglevel", default="INFO", help="Log level")
@click.option("--burst", is_flag=True, help="Run in burst mode")
@click.option("--results-ttl", type=int, default=500, help="Default result TTL in seconds")
@click.option(
    "--pool",
    type=click.Choice(["prefork", "solo", "gevent"]),
    default="prefork",
    help="Pool type: prefork (forking), solo (no fork), gevent (green threads)",
)
def main(redis_url: str | None, name: str | None, loglevel: str, burst: bool, results_ttl: int, pool: str) -> None:
    """Run an RQ worker; ``launch(["worker", ...])`` parses its args here."""
    start_rq_worker(redis_url, name, loglevel, burst, results_ttl, pool)
