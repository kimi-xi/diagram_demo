-- Dimension table for students with non-changing attributes
-- One row per student

{{ config(
    pre_hook=[
        "CREATE TYPE IF NOT EXISTS academic_semester AS ENUM ('WiSe', 'SoSe');"
    ]
) }}

with students_base as (
    select * from {{ ref('stg_students_clean') }}
),

student_attributes as (
    select distinct
        student_id,
        year_of_matriculation,
        semester_of_matriculation,
        case when year_of_matriculation < 18 then 11 else 18 end as study_regulations_year,
        field_of_study
    from students_base
),

degree_completion as (
    select
        student_id,
        sum(case when module_number = 'I707' and exam_status = 'bestanden' then 1 else 0 end) as i707_pass_count
    from students_base
    group by student_id
),

term_totals as (
    select
        student_id,
        max(term_counter) as terms_taken_total_count
    from students_base
    group by student_id
),

start_semester_index as (
    select
        student_id,
        (year_of_matriculation * 2 +
         case when semester_of_matriculation = 'SoSe' then 1 else 2 end
        ) as start_semester_index
    from student_attributes
),

current_semester_index as (
    select
        max(year * 2 +
            case when semester_type = 'SoSe' then 1 else 2 end
        ) as current_semester_index
    from students_base
)

select
    s.student_id,
    s.year_of_matriculation,
    s.semester_of_matriculation,
    s.study_regulations_year,
    s.field_of_study,
    case when coalesce(d.i707_pass_count, 0) >= 1 then true else false end as graduation_flag,
    t.terms_taken_total_count,
    case
        when (csi.current_semester_index - ssi.start_semester_index) >= 8
         and coalesce(d.i707_pass_count, 0) = 0
        then true
        else false
    end as inactivity_flag
from student_attributes s
cross join current_semester_index csi
left join degree_completion d on s.student_id = d.student_id
left join term_totals t on s.student_id = t.student_id
left join start_semester_index ssi on s.student_id = ssi.student_id

