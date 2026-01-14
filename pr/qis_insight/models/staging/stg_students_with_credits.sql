-- Join students with module data and calculate earned credits
-- Guarantees: one output row per input row from stg_students_clean

with students as (
    select * from {{ ref('stg_students_clean') }}
),

-- Get student dimension data
dim_students as (
    select * from {{ ref('dim_students') }}
),

-- Pass-tracking per student/module
students_with_pass_tracking as (
    select
        s.*,
        d.study_regulations_year,
        d.graduation_flag,
        -- Count of passed attempts up to current row (first pass => passed_so_far = 1)
        sum(case when s.exam_status = 'bestanden' then 1 else 0 end) over (
            partition by s.student_id, s.module_number
            order by s.term_counter
            rows between unbounded preceding and current row
        ) as passed_so_far
    from students s
    inner join dim_students d on s.student_id = d.student_id
),

-- Pick the closest/fuzziest single module match per student-row via lateral subquery
-- Uses dim_modul to get exact module_key
matched as (
    select
        s.student_id,
        s.module_number,
        s.grade,
        s.grade_numeric,
        s.exam_status,
        s.semester_type,
        s.year,
        s.exam_annotation,
        s.field_of_study,
        s.term_counter,
        s.year_of_matriculation,
        s.study_regulations_year,
        s.semester_of_matriculation,
        s.graduation_flag,
        m.module_title as module_title,
        m.module_type,
        m.credits,
        m.module_acronym,
        m.recommended_semester as module_semester,
        m.module_key,
        m.graduation_year,
        m.graduation_semester,
        m.field_of_study as module_field_of_study,
        -- compute match_quality consistent with our fuzzy rules
        case
            when m.module_title is null then 3
            when s.field_of_study = m.field_of_study 
             and s.study_regulations_year = m.graduation_year
             and s.semester_of_matriculation = m.graduation_semester then 0
            when s.study_regulations_year = m.graduation_year
             and s.semester_of_matriculation = m.graduation_semester then 1
            when s.study_regulations_year = m.graduation_year then 2
            else 3
        end as match_quality,
        s.passed_so_far
    from students_with_pass_tracking s
    left join lateral (
        select
            dm.module_title,
            dm.module_type,
            dm.credits,
            dm.recommended_semester,
            dm.module_acronym,
            dm.module_key,
            dm.graduation_year,
            dm.graduation_semester,
            dm.field_of_study,
            -- order by the same scoring used above to select the closest match
            case
                when s.field_of_study = dm.field_of_study
                 and s.study_regulations_year = dm.graduation_year
                 and s.semester_of_matriculation = dm.graduation_semester then 0
                when s.study_regulations_year = dm.graduation_year
                 and s.semester_of_matriculation = dm.graduation_semester then 1
                when s.study_regulations_year = dm.graduation_year then 2
                else 3
            end as oq
        from {{ ref('dim_modul') }} dm
        where dm.module_id = s.module_number
        order by oq,  -- best quality first
                 case when s.field_of_study = dm.field_of_study then 0 else 1 end, -- prefer same direction as tiebreaker
                 dm.graduation_year, dm.graduation_semester
        limit 1
    ) m on true
),

with_earned as (
    select
        student_id,
        module_number,
        grade,
        grade_numeric,
        exam_status,
        semester_type,
        year,
        exam_annotation,
        field_of_study,
        term_counter,
        year_of_matriculation,
        study_regulations_year,
        semester_of_matriculation,
        graduation_flag,
        module_title,
        module_type,
        credits,
        module_acronym,
        module_semester,
        module_key,
        graduation_year,
        graduation_semester,
        module_field_of_study,
        match_quality,
        passed_so_far,
        -- credits only on the first successful pass and only for perfect in-plan match
        case
            when exam_status = 'bestanden'
             and passed_so_far = 1
             and match_quality = 0
             and credits is not null then credits
            else 0
        end as credits_earned,
        case
            when exam_status = 'bestanden' and module_semester is not null and match_quality = 0 then term_counter - module_semester
            else null
        end as exam_plan_dif,
        case when match_quality = 0 then false else true end as module_not_in_plan,
        case
            when graduation_flag = true
             and module_number not in ('XZM', 'XWM', 'XQQ')
             and max(case when exam_status = 'bestanden' then 1 else 0 end) over (
                partition by student_id, module_number
             ) = 0
            then true
            else false
        end as sanity_check
    from matched
),


with_tries_counter as (
    select
        *,
        max(term_counter) over (
            partition by student_id, module_number
        ) as max_term_counter,

        case
            when exam_status = 'nicht bestanden' then true
            when exam_status = 'bestanden' and term_counter = max(term_counter) over (partition by student_id, module_number) then true
            else false
        end as should_count_try
    from with_earned
)

select
    student_id,
    module_number,
    grade_numeric as grade,
    exam_status,
    semester_type,
    year,
    exam_annotation,
    term_counter,
    module_title,
    module_acronym,
    module_key,
    credits_earned,
    exam_plan_dif,
    module_not_in_plan,
    match_quality,
    sanity_check,
   
    count(distinct case when should_count_try then term_counter end) over (
        partition by student_id, module_number
        order by term_counter
        rows between unbounded preceding and current row
    ) as tries_count,
    count(distinct case when exam_status = 'abgemeldet' then term_counter end) over (
        partition by student_id, module_number
        order by term_counter
        rows between unbounded preceding and current row
    ) as withdrawal_count,
    -- Fake date for each term: SoSe = 1.March, WiSe = 1.October
    -- year values are 14, 15, 16... so we append 20 to get 2014, 2015, 2016...
    case
        when semester_type = 'SoSe' then make_date(2000 + year, 3, 1)
        when semester_type = 'WiSe' then make_date(2000 + year, 10, 1)
        else null
    end as term_date
from with_tries_counter 
