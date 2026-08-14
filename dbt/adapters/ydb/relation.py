from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, EventTimeFilter, Policy

from dbt.adapters.ydb.literals import timestamp_literal


@dataclass
class YDBQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True

@dataclass
class YDBIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True

@dataclass(frozen=True, eq=False, repr=False)
class YDBRelation(BaseRelation):
    quote_character: str = '`'
    quote_policy: Policy = field(default_factory=lambda: YDBQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: YDBIncludePolicy())

    def render(self) -> str:
        # if there is nothing set, this will return the empty string.
        res = "/".join([part for part in [self.schema, self.identifier] if part and part != "/"])
        if not self.quote_policy.identifier:
            return res
        return self.quoted(res)

    def _render_event_time_filtered(self, event_time_filter: EventTimeFilter) -> str:
        """The `[start, end)` window a microbatch input is read through.

        Same window as the base implementation, but with YQL datetime literals: the
        base one compares the event time column with a quoted string, which YQL
        refuses to type-check against a Date/Datetime/Timestamp column.
        """
        conditions = []

        if event_time_filter.start:
            conditions.append(
                f"{event_time_filter.field_name} >= {timestamp_literal(event_time_filter.start)}"
            )
        if event_time_filter.end:
            conditions.append(
                f"{event_time_filter.field_name} < {timestamp_literal(event_time_filter.end)}"
            )

        return " and ".join(conditions)
