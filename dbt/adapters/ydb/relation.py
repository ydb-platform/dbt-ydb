from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy


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
        return self.quoted("/".join([part for part in [self.schema, self.identifier] if part]))
