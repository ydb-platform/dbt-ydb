SELECT
    {% if is_incremental() %}
    updates.user_id as user_id,
    COALESCE(existing.first_seen_at, Unwrap(CAST('1970-01-01T00:00:00Z' as Timestamp))) as first_seen_at, -- to make this NOT NULL
    updates.last_seen_at as last_seen_at,
    updates.sex as sex,
    updates.age as age,
    updates.income_level as income_level,
    updates.region_id as region_id
    {% else %}
    updates.*
    {% endif %}
FROM (
    SELECT -- GET NEW DATA
        user_id,
        MIN(event_time) AS first_seen_at,
        MAX(event_time) AS last_seen_at,
        MAX(sex) AS sex,
        MAX(age) AS age,
        MAX(income_level) AS income_level,
        MAX(region_id) AS region_id
    FROM {{ ref('stg_clicks') }}
    WHERE
        user_id IS NOT NULL
        AND event_time > {{ incremental_operand_safe('last_seen_at') }}
    GROUP BY user_id
) AS updates
{% if is_incremental() %}
LEFT JOIN {{ this }} AS existing -- JOIN WITH existing data to take first_seen
    ON existing.user_id = updates.user_id
{% endif %}