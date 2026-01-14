-- Time dimension table
-- One row per unique year/semester_type combination

with distinct_time as (
    select distinct 
        year,
        semester_type
    from {{ ref('stg_students_with_credits') }}
    where year IS NOT NULL AND semester_type IS NOT NULL
)

select
    ROW_NUMBER() OVER (ORDER BY year, semester_type) AS time_id,
    year,
    semester_type,
    case 
        when semester_type = 'SoSe' then 1 
        when semester_type = 'WiSe' then 0
        else null 
    end as semester_type_numeric,
    case
        when semester_type = 'SoSe' then make_date(2000 + year, 3, 1)
        when semester_type = 'WiSe' then make_date(2000 + year, 10, 1)
        else null
    end as time_label
from distinct_time

