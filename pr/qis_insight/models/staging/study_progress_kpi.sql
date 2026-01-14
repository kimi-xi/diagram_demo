{{ config(materialized='table') }}

WITH fact_base AS (
    SELECT * FROM {{ ref('fact_kpi') }}
),

credits_pro_semester AS (
    SELECT
        f.student_id,
        f.term_counter,
        SUM(f.credits_earned) AS credits_per_term,
        COUNT(DISTINCT f.module_id) AS modules_per_term,
        ROUND(AVG(f.grade_numeric), 2) AS avg_grade_per_term,

        CASE
            WHEN ABS(SUM(f.credits_earned) - 30) > 5 THEN 0
            ELSE 1
        END AS ontrack_flag
    FROM fact_base f
    GROUP BY f.student_id, f.term_counter
)

SELECT
    credits_pro_semester.student_id,
    term_counter,
    credits_per_term,
    modules_per_term,
    avg_grade_per_term,

    SUM(credits_per_term) OVER (
        PARTITION BY credits_pro_semester.student_id
        ORDER BY term_counter
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_ects,

    ROUND(
        avg_grade_per_term
        - LAG(avg_grade_per_term) OVER (
            PARTITION BY credits_pro_semester.student_id
            ORDER BY term_counter
        ),
        2
    ) AS grade_change,

    ROUND(
        credits_per_term
        - LAG(credits_per_term) OVER (
            PARTITION BY credits_pro_semester.student_id
            ORDER BY term_counter
        ),
        2
    ) AS credit_change,

    ontrack_flag,
    s.inactivity_flag

FROM credits_pro_semester
JOIN dim_students s ON s.student_id = credits_pro_semester.student_id
ORDER BY credits_pro_semester.student_id, term_counter