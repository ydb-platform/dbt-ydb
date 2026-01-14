-- Purpose: Direct wrapper over source. No logic, no transformations.
-- Used to isolate source dependency and enable testing.
SELECT * FROM {{ source('external', 'hits') }}