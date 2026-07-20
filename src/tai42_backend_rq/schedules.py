"""Schedule shapes: the portable :class:`ScheduleRecord` and the one code path
that writes a canonical schedule into the RQ scheduler.

``ScheduleRecord`` is the backend-neutral, JSON-serializable form a schedule
takes through the export/import backup round-trip. ``apply_normalized_schedule``
is shared by the schedule-create, schedule-update, and import paths so every
schedule lands in the scheduler the same way: the interval seconds or cron
string is mirrored into the job ``meta`` so the schedule can be read straight
back off the job.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator


class ScheduleRecord(BaseModel):
    """One schedule in a backend-neutral, JSON-serializable form.

    ``name`` identifies the schedule; ``args`` and ``kwargs`` are the positional
    and keyword arguments handed to the scheduled call; ``schedule`` holds the
    canonical interval-or-crontab dict that ``normalize_schedule`` produces;
    ``enabled`` marks whether the schedule is active.

    ``model_dump`` yields a plain JSON-ready dict and ``model_validate`` reads
    one back, so a record survives a round trip through a backup document
    unchanged.
    """

    name: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    schedule: dict[str, Any]
    enabled: bool

    @model_validator(mode="after")
    def _check_schedule_kind(self) -> ScheduleRecord:
        kind = self.schedule.get("__type__")
        if kind not in {"interval", "crontab"}:
            raise ValueError(f"schedule '__type__' must be 'interval' or 'crontab', got {kind!r}")
        if kind == "interval":
            every = self.schedule.get("every")
            # bool is an int subclass; a boolean 'every' is not a valid period.
            if not isinstance(every, int | float) or isinstance(every, bool):
                raise ValueError(f"interval schedule requires numeric 'every', got {every!r}")
            relative = self.schedule.get("relative")
            if relative is not None and not isinstance(relative, bool):
                raise ValueError(f"interval 'relative' must be a bool, got {relative!r}")
        else:  # crontab
            missing = [
                field
                for field in ("minute", "hour", "day_of_month", "month_of_year", "day_of_week")
                if field not in self.schedule
            ]
            if missing:
                raise ValueError(f"crontab schedule missing required field(s): {missing}")
        return self


def crontab_string(norm: dict[str, Any]) -> str:
    """The 5-field cron expression of a canonical crontab dict."""
    return f"{norm['minute']} {norm['hour']} {norm['day_of_month']} {norm['month_of_year']} {norm['day_of_week']}"


def interval_seconds(norm: dict[str, Any]) -> int:
    """The whole-second interval of a canonical interval dict.

    The RQ scheduler stores and re-arms an interval as ``int(interval)``
    seconds: a fractional value would silently recur at the truncated rate,
    and one below a second would truncate to 0 and never recur at all. An
    interval the scheduler cannot represent exactly is rejected loudly here
    instead.
    """
    every = float(norm["every"])
    seconds = int(every)
    if seconds != every or seconds < 1:
        raise ValueError(
            f"The RQ scheduler recurs on whole seconds (>= 1); interval 'every'={every!r} cannot be represented"
        )
    return seconds


async def apply_normalized_schedule(
    scheduler: Any,
    norm: dict[str, Any],
    func: Any,
    args: list[Any],
    kwargs: dict[str, Any],
    schedule_name: str,
) -> None:
    """Create a scheduled job named ``schedule_name`` from a canonical schedule dict.

    ``norm`` is the canonical interval-or-crontab dict that ``normalize_schedule``
    produces. An interval schedule becomes a recurring ``scheduler.schedule``
    call (whole seconds — see :func:`interval_seconds`) and a crontab schedule
    becomes a ``scheduler.cron`` call; ``func``, ``args`` and ``kwargs`` are
    the call the job runs. The interval seconds or cron string is mirrored
    into the job ``meta`` so the schedule can be read straight back off the
    job. Any other ``__type__`` raises. The scheduler calls are blocking Redis
    writes, so they run off the event loop.
    """
    meta_data: dict[str, Any] = {}

    if norm["__type__"] == "interval":
        interval_val = interval_seconds(norm)
        meta_data["interval"] = interval_val

        await asyncio.to_thread(
            lambda: scheduler.schedule(
                scheduled_time=datetime.now(UTC),
                func=func,
                args=args,
                kwargs=kwargs,
                interval=interval_val,
                id=schedule_name,
                meta=meta_data,
            )
        )
    elif norm["__type__"] == "crontab":
        cron = crontab_string(norm)
        meta_data["cron_string"] = cron

        await asyncio.to_thread(
            lambda: scheduler.cron(
                cron,
                func=func,
                args=args,
                kwargs=kwargs,
                id=schedule_name,
                meta=meta_data,
            )
        )
    else:
        raise ValueError(f"Unsupported schedule type: {norm['__type__']}")
