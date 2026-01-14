-- Student dimension table with aggregated credits and comprehensive sanity checks
-- One row per student with total earned credits and data quality flags
-- Credits are calculated using exact module_key from dim_modul (via stg_students_with_credits)
-- which ensures proper matching even when students study out of their field_of_study

with student_credits as (
    select
        student_id,
        cast(sum(credits_earned) as INT) as credits_earned_total_count,
        cast(count(case when module_number in ('XWM', 'XZM') and exam_status = 'bestanden' then 1 end) as INT) as passed_xwm_xzm_count,
        max(case when sanity_check = true then 1 else 0 end) as any_core_module_sanity_fail
    from {{ ref('stg_students_with_credits') }}
    group by student_id
),

students_enriched as (
    select
        s.student_id,
        s.graduation_flag,
        s.year_of_matriculation,
        s.semester_of_matriculation,
        s.study_regulations_year,
        s.field_of_study,
        s.terms_taken_total_count,
        cast(coalesce(c.credits_earned_total_count, 0) as INT) as credits_earned_total_count,
        coalesce(c.passed_xwm_xzm_count, 0) as passed_xwm_xzm_count,
        -- Calculate potential credits if we add 5 per passed XWM/XZM
        cast(coalesce(c.credits_earned_total_count, 0) + (coalesce(c.passed_xwm_xzm_count, 0) * 5) as INT) as total_credits_with_xwm_xzm_bonus,
        coalesce(c.any_core_module_sanity_fail, 0) as any_core_module_sanity_fail
    from {{ ref('dim_students') }} s
    left join student_credits c on s.student_id = c.student_id
)

select
    student_id,
    graduation_flag,
    year_of_matriculation,
    semester_of_matriculation,
    study_regulations_year,
    field_of_study,
    terms_taken_total_count,
    credits_earned_total_count,
    passed_xwm_xzm_count,
    total_credits_with_xwm_xzm_bonus,
    case
        when graduation_flag = true and (
            any_core_module_sanity_fail = 1
            or credits_earned_total_count < 120 and total_credits_with_xwm_xzm_bonus < 120
        ) then true
        else false
    end as sanity_check
from students_enriched

