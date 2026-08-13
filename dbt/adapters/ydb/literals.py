from datetime import date, datetime, timezone
from typing import Union

from dbt_common.exceptions import DbtRuntimeError


def timestamp_literal(value: Union[datetime, date, str]) -> str:
    """Renders a value as a YQL `Timestamp` literal.

    YQL has no bare datetime literals -- a value is built by its type constructor from
    ISO 8601 text with a `T` separator, so the `'2020-01-01 00:00:00+00:00'` string dbt
    renders by default is not something a Date/Datetime/Timestamp column can be
    compared with.
    """
    dt = _as_datetime(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    rendered = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        rendered = f"{rendered}.{dt.microsecond:06d}"

    return f'Timestamp("{rendered}Z")'


def _as_datetime(value: Union[datetime, date, str]) -> datetime:
    if isinstance(value, datetime):
        return value

    # a plain date is a midnight timestamp
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    if isinstance(value, str):
        # `fromisoformat` only learned to read the `Z` suffix in 3.11
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            raise DbtRuntimeError(f"Unable to read '{value}' as a timestamp")

    raise DbtRuntimeError(
        f"Unable to render {type(value).__name__} as a YQL timestamp literal"
    )
