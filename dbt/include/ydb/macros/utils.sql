/*
  Cross-database ("shim") macros for YDB / YQL.

  These override the default implementations that dbt-core (and packages such as
  dbt_utils / dbt_expectations) dispatch to, so that generated SQL is valid YQL.
  Covered by dbt.tests.adapter.utils.* (see tests/functional/adapter/utils).
*/

-- ------------------------------------------------------------------ strings --

{% macro ydb__length(expression) -%}
    LENGTH({{ expression }})
{%- endmacro %}

{% macro ydb__position(substring_text, string_text) -%}
    COALESCE(FIND({{ string_text }}, {{ substring_text }}) + 1, 0)
{%- endmacro %}

{% macro ydb__replace(field, old_chars, new_chars) -%}
    String::ReplaceAll({{ field }}, {{ old_chars }}, {{ new_chars }})
{%- endmacro %}

{% macro ydb__hash(field) -%}
    CAST(Digest::Md5Hex(CAST(({{ field }}) AS String)) AS Utf8)
{%- endmacro %}

{% macro ydb__right(string_text, length_expression) -%}
    CASE
        WHEN {{ length_expression }} = 0 THEN CAST('' AS Utf8)
        ELSE CAST(SUBSTRING(
            CAST({{ string_text }} AS String),
            CAST(LENGTH({{ string_text }}) - CAST({{ length_expression }} AS Uint32) AS Uint32)
        ) AS Utf8)
    END
{%- endmacro %}

{% macro ydb__escape_single_quotes(expression) -%}
    {{ expression | replace("'", "\\'") }}
{%- endmacro %}

{#-- String::SplitToList wants String args (not Utf8); cast in and back out.
     part_number is 1-based; negative counts from the end. --#}
{% macro ydb__split_part(string_text, delimiter_text, part_number) -%}
    {%- set src = 'CAST(' ~ string_text ~ ' AS String)' -%}
    {#-- SplitToList needs a non-optional String delimiter, hence the `?? ''`. --#}
    {%- set delim = '(CAST(' ~ delimiter_text ~ " AS String) ?? '')" -%}
    {%- if part_number >= 0 -%}
        CAST(String::SplitToList({{ src }}, {{ delim }}){{ '[' ~ (part_number - 1) ~ ']' }} AS Utf8)
    {%- else -%}
        CAST(ListReverse(String::SplitToList({{ src }}, {{ delim }})){{ '[' ~ ((part_number * -1) - 1) ~ ']' }} AS Utf8)
    {%- endif -%}
{%- endmacro %}

-- --------------------------------------------------------------- aggregates --

{#-- YDB's BOOL_OR propagates NULL ({false, null} -> null). Emulate the standard
     null-skipping aggregate so a group of {false, null} yields false. --#}
{% macro ydb__bool_or(expression) -%}
    MAX(CASE WHEN ({{ expression }}) IS NULL THEN NULL WHEN ({{ expression }}) THEN 1 ELSE 0 END) = 1
{%- endmacro %}

{% macro ydb__any_value(expression) -%}
    SOME({{ expression }})
{%- endmacro %}

-- -------------------------------------------------------------------- dates --

{#-- YDB's DateTime:: interval/shift UDFs take a non-optional Int32, so the
     (nullable) interval is normalised to one. --#}
{% macro ydb__dateadd(datepart, interval, from_date_or_time) -%}
    {%- set dp = datepart | lower -%}
    {%- set n = 'COALESCE(CAST(' ~ interval ~ ' AS Int32), 0)' -%}
    {%- if dp in ['year', 'years'] -%}
        DateTime::MakeDatetime(DateTime::ShiftYears(DateTime::Split({{ from_date_or_time }}), {{ n }}))
    {%- elif dp in ['quarter', 'quarters'] -%}
        DateTime::MakeDatetime(DateTime::ShiftMonths(DateTime::Split({{ from_date_or_time }}), {{ n }} * 3))
    {%- elif dp in ['month', 'months'] -%}
        DateTime::MakeDatetime(DateTime::ShiftMonths(DateTime::Split({{ from_date_or_time }}), {{ n }}))
    {%- elif dp in ['week', 'weeks'] -%}
        {{ from_date_or_time }} + DateTime::IntervalFromDays({{ n }} * 7)
    {%- elif dp in ['day', 'days'] -%}
        {{ from_date_or_time }} + DateTime::IntervalFromDays({{ n }})
    {%- elif dp in ['hour', 'hours'] -%}
        {{ from_date_or_time }} + DateTime::IntervalFromHours({{ n }})
    {%- elif dp in ['minute', 'minutes'] -%}
        {{ from_date_or_time }} + DateTime::IntervalFromMinutes({{ n }})
    {%- elif dp in ['second', 'seconds'] -%}
        {{ from_date_or_time }} + DateTime::IntervalFromSeconds({{ n }})
    {%- elif dp in ['millisecond', 'milliseconds'] -%}
        {{ from_date_or_time }} + DateTime::IntervalFromMilliseconds({{ n }})
    {%- elif dp in ['microsecond', 'microseconds'] -%}
        {{ from_date_or_time }} + DateTime::IntervalFromMicroseconds({{ n }})
    {%- else -%}
        {{ exceptions.raise_compiler_error("dateadd: unsupported datepart '" ~ datepart ~ "' for YDB") }}
    {%- endif -%}
{%- endmacro %}

{% macro ydb__date_trunc(datepart, date) -%}
    {%- set dp = datepart | lower -%}
    {%- if dp in ['year', 'years'] -%}
        DateTime::MakeDatetime(DateTime::StartOfYear({{ date }}))
    {%- elif dp in ['quarter', 'quarters'] -%}
        DateTime::MakeDatetime(DateTime::StartOfQuarter({{ date }}))
    {%- elif dp in ['month', 'months'] -%}
        DateTime::MakeDatetime(DateTime::StartOfMonth({{ date }}))
    {%- elif dp in ['week', 'weeks'] -%}
        DateTime::MakeDatetime(DateTime::StartOfWeek({{ date }}))
    {%- elif dp in ['day', 'days'] -%}
        DateTime::MakeDatetime(DateTime::StartOfDay({{ date }}))
    {%- elif dp in ['hour', 'hours'] -%}
        DateTime::MakeDatetime(DateTime::StartOf({{ date }}, Interval("PT1H")))
    {%- elif dp in ['minute', 'minutes'] -%}
        DateTime::MakeDatetime(DateTime::StartOf({{ date }}, Interval("PT1M")))
    {%- else -%}
        {{ exceptions.raise_compiler_error("date_trunc: unsupported datepart '" ~ datepart ~ "' for YDB") }}
    {%- endif -%}
{%- endmacro %}

{#-- Boundary-crossing datediff (second - first), matching dbt semantics.
     Inputs are expected to be Date/Datetime/Timestamp typed; sub-second parts
     are only meaningful for Timestamp inputs. --#}
{% macro ydb__datediff(first_date, second_date, datepart) -%}
    {%- set dp = datepart | lower -%}
    {%- set f = first_date -%}
    {%- set s = second_date -%}
    {%- set yf = 'CAST(DateTime::GetYear(' ~ f ~ ') AS Int64)' -%}
    {%- set ys = 'CAST(DateTime::GetYear(' ~ s ~ ') AS Int64)' -%}
    {%- set mf = 'CAST(DateTime::GetMonth(' ~ f ~ ') AS Int64)' -%}
    {%- set ms = 'CAST(DateTime::GetMonth(' ~ s ~ ') AS Int64)' -%}
    {%- set secf = 'CAST(DateTime::ToSeconds(' ~ f ~ ') AS Int64)' -%}
    {%- set secs = 'CAST(DateTime::ToSeconds(' ~ s ~ ') AS Int64)' -%}
    {%- if dp in ['year', 'years'] -%}
        ({{ ys }} - {{ yf }})
    {%- elif dp in ['quarter', 'quarters'] -%}
        (({{ ys }} * 4 + ({{ ms }} - 1) / 3) - ({{ yf }} * 4 + ({{ mf }} - 1) / 3))
    {%- elif dp in ['month', 'months'] -%}
        (({{ ys }} * 12 + {{ ms }}) - ({{ yf }} * 12 + {{ mf }}))
    {%- elif dp in ['week', 'weeks'] -%}
        (CAST(DateTime::ToSeconds(DateTime::MakeDatetime(DateTime::StartOfWeek({{ s }}))) AS Int64) / 604800
         - CAST(DateTime::ToSeconds(DateTime::MakeDatetime(DateTime::StartOfWeek({{ f }}))) AS Int64) / 604800)
    {%- elif dp in ['day', 'days'] -%}
        ({{ secs }} / 86400 - {{ secf }} / 86400)
    {%- elif dp in ['hour', 'hours'] -%}
        ({{ secs }} / 3600 - {{ secf }} / 3600)
    {%- elif dp in ['minute', 'minutes'] -%}
        ({{ secs }} / 60 - {{ secf }} / 60)
    {%- elif dp in ['second', 'seconds'] -%}
        ({{ secs }} - {{ secf }})
    {%- elif dp in ['millisecond', 'milliseconds'] -%}
        (({{ secs }} - {{ secf }}) * 1000)
    {%- elif dp in ['microsecond', 'microseconds'] -%}
        (({{ secs }} - {{ secf }}) * 1000000)
    {%- else -%}
        {{ exceptions.raise_compiler_error("datediff: unsupported datepart '" ~ datepart ~ "' for YDB") }}
    {%- endif -%}
{%- endmacro %}

{% macro ydb__last_day(date, datepart) -%}
    {%- set dp = datepart | lower -%}
    {%- if dp == 'month' -%}
        DateTime::MakeDate(DateTime::ShiftMonths(DateTime::StartOfMonth({{ date }}), 1)) - Interval("P1D")
    {%- elif dp == 'quarter' -%}
        DateTime::MakeDate(DateTime::ShiftMonths(DateTime::StartOfQuarter({{ date }}), 3)) - Interval("P1D")
    {%- elif dp == 'year' -%}
        DateTime::MakeDate(DateTime::ShiftYears(DateTime::StartOfYear({{ date }}), 1)) - Interval("P1D")
    {%- else -%}
        {{ exceptions.raise_compiler_error("last_day: unsupported datepart '" ~ datepart ~ "' for YDB") }}
    {%- endif -%}
{%- endmacro %}

-- -------------------------------------------------------------------- casts --

{% macro ydb__cast_bool_to_text(field) -%}
    CASE
        WHEN {{ field }} THEN 'true'
        WHEN NOT {{ field }} THEN 'false'
        ELSE NULL
    END
{%- endmacro %}

{% macro ydb__safe_cast(field, type) -%}
    CAST({{ field }} AS {{ type }}?)
{%- endmacro %}

-- ------------------------------------------------------------------- types --

{% macro ydb__type_string() -%}
    {{ return("Text") }}
{%- endmacro %}

{% macro ydb__type_int() -%}
    {{ return("Int64") }}
{%- endmacro %}

{% macro ydb__type_bigint() -%}
    {{ return("Int64") }}
{%- endmacro %}

{% macro ydb__type_float() -%}
    {{ return("Double") }}
{%- endmacro %}

{% macro ydb__type_numeric() -%}
    {{ return("Decimal(22, 9)") }}
{%- endmacro %}

{% macro ydb__type_boolean() -%}
    {{ return("Bool") }}
{%- endmacro %}

{% macro ydb__type_timestamp() -%}
    {{ return("Timestamp") }}
{%- endmacro %}
