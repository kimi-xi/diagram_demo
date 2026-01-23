with dim_students_base as (
    select * from {{ ref('dim_students') }}
),

basis as (
    select 
        field_of_study,
        year_of_matriculation,
        COUNT(distinct student_id) as matriculations_count
    from dim_students_base
    group by field_of_study, year_of_matriculation
),

active_students as (
    select distinct
        s.field_of_study,
        z.year,
        f.student_id
    from {{ ref('fact_kpi') }} f
    join {{ ref('dim_zeit') }} z
        on f.time_id = z.time_id
    join {{ ref('dim_students') }} s
        on f.student_id = s.student_id
    where f.exam_status <> 'abgemeldet'
),

active_students_agg as (
    select
        field_of_study,
        year,
        count(student_id) as students_active_count
    from active_students
    group by field_of_study, year
),

avg_grade_per_year as (
    select
        s.field_of_study,
        z.year,

        round(sum(
                case
                    when f.passed_flag = 1
                        and f.grade_numeric is not null
                    then f.grade_numeric * f.credits_earned
                end
            )/
            nullif(sum(
                    case
                        when f.passed_flag = 1
                            and f.grade_numeric is not null
                        then f.credits_earned
                    end
                ),
                0
            ),
            2
        ) as weighted_avg_grade_per_year

    from {{ ref('fact_kpi') }} f
    join {{ ref('dim_zeit') }} z
        on f.time_id = z.time_id
    join {{ ref('dim_students') }} s
        on f.student_id = s.student_id
    group by
        s.field_of_study,
        z.year
),

dropouts as (
    select distinct
        s.field_of_study,
        z.year,
        s.student_id
    from {{ ref('fact_kpi') }} f
    join {{ ref('dim_zeit') }} z
        on f.time_id = z.time_id
    join {{ ref('dim_students') }} s
        on f.student_id = s.student_id
    where(
            f.module_id = 'I707'
            and f.passed_flag = 0
            and f.tries_count >= 3
        )
        or s.inactivity_flag = 1
),

dropout_rate as (
    select
        d.field_of_study,
        d.year,
        round(
            count(distinct d.student_id) * 1.0
            / nullif(a.students_active_count, 0),
            4
        ) as dropout_rate
    from dropouts d
    left join active_students_agg a
        on d.field_of_study = a.field_of_study
       and d.year = a.year
    group by
        d.field_of_study,
        d.year,
        a.students_active_count
),

ewma_calc AS (
    SELECT
        b.field_of_study,
        year_of_matriculation,
        matriculations_count,
        LAG(matriculations_count, 1) over (
            partition by b.field_of_study
            order by b.year_of_matriculation
        ) as matriculations_prev_year_count,

        ROUND(
            (
                (b.matriculations_count * 1.0) -
                LAG(b.matriculations_count, 1) over (
                    partition by b.field_of_study
                    order by b.year_of_matriculation
                )
            )
            /
            NULLIF(
                LAG(b.matriculations_count, 1) over (
                    partition by b.field_of_study
                    order by b.year_of_matriculation
                ), 
                0
            )
            * 100,
            2
        ) as matriculations_growth_rate,

        a.students_active_count as students_active_count,
        g.weighted_avg_grade_per_year as weighted_avg_grade_per_year,
        d.dropout_rate as dropout_rate,

        (
            0.5 * matriculations_count + 0.3 * COALESCE(
                LAG(matriculations_count, 1) OVER (
                    PARTITION BY b.field_of_study
                    ORDER BY year_of_matriculation
                ),
                matriculations_count
            )
            + 0.2 * COALESCE(
                LAG(matriculations_count, 2) OVER (
                    PARTITION BY b.field_of_study
                    ORDER BY year_of_matriculation
                ),
                matriculations_count
            )
        ) AS ewma_enrollments
    FROM basis b
    left join active_students_agg a
        on b.field_of_study = a.field_of_study
        and b.year_of_matriculation = a.year
    left join avg_grade_per_year g
        on b.field_of_study = g.field_of_study
        and b.year_of_matriculation = g.year
    left join dropout_rate d
        on b.field_of_study = d.field_of_study
        and b.year_of_matriculation = d.year
    )

SELECT
    field_of_study,
    year_of_matriculation,
    matriculations_count,
    matriculations_prev_year_count,
    matriculations_growth_rate,
    
    students_active_count,
    weighted_avg_grade_per_year,
    dropout_rate,
    
    ROUND(ewma_enrollments, 2) AS ewma_enrollments,
    ROUND(
        (matriculations_count - ewma_enrollments) / NULLIF(ewma_enrollments, 0) * 100,
        2
    ) AS growth_rate_vs_ewma_percent,
    ROUND(REGR_SLOPE(matriculations_count, year_of_matriculation)
        OVER (PARTITION BY field_of_study),
        3
    ) AS regression_slope,

    ROUND(
        REGR_INTERCEPT(matriculations_count, year_of_matriculation)
        OVER (PARTITION BY field_of_study),
        3
    ) AS regression_intercept,


    ROUND(
    (
        REGR_INTERCEPT(matriculations_count, year_of_matriculation)
        OVER (PARTITION BY field_of_study)
    )
    +
    (
        REGR_SLOPE(matriculations_count, year_of_matriculation)
        OVER (PARTITION BY field_of_study) * year_of_matriculation
    ),
    3
    ) AS regression_trend_value

FROM ewma_calc
ORDER BY
    field_of_study,
    year_of_matriculation