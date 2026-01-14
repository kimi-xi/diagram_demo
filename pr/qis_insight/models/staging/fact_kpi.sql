-- Fact table for KPI analysis
-- One row per student-module-time combination

select
    s.student_id,
    m.module_id,
    z.time_id,
    cast(s.grade as DOUBLE) AS grade_numeric,
    case when s.exam_status in ('bestanden') then 1 else 0 end AS passed_flag,
    s.credits_earned,
    s.exam_status,
    s.tries_count,
    s.withdrawal_count,
    s.term_counter
from {{ ref('stg_students_with_credits') }} s
inner join {{ ref('dim_modul') }} m
    on s.module_key = m.module_key
left join {{ ref('dim_zeit') }} z
    on s.year = z.year AND s.semester_type = z.semester_type

