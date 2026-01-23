{{ config(materialized='table') }}

WITH fact_base AS (
    SELECT * FROM {{ ref('fact_kpi') }}
),

cohort_ontrack AS (
    SELECT
        s.year_of_matriculation,
        f.student_id,
        f.term_counter,
        SUM(f.credits_earned) AS credits_per_term
    FROM fact_base f
    JOIN students_final.dim_students s
        ON f.student_id = s.student_id
    GROUP BY
        s.year_of_matriculation,
        f.student_id,
        f.term_counter
),

ontrack_agg AS (
    SELECT
        year_of_matriculation,
        SUM(
            CASE
                WHEN ABS(credits_per_term - 30) <= 5 THEN 1
                ELSE 0
            END
        ) AS ontrack_terms,
        COUNT(*) AS total_terms
    FROM cohort_ontrack
    GROUP BY year_of_matriculation
)

SELECT
    s.year_of_matriculation AS intake_year,

    COUNT(DISTINCT s.student_id) AS enrollments,

    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN m.module_id = 'I707'
                 AND f.passed_flag = 1
            THEN s.student_id
        END)
        / NULLIF(COUNT(DISTINCT s.student_id), 0),
        2
    ) AS graduation_rate,

    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN (
                m.module_id = 'I707'
                AND f.passed_flag = 0
                AND f.tries_count >= 3
            )
            OR s.inactivity_flag = 1
            THEN s.student_id
        END)
        / NULLIF(COUNT(DISTINCT s.student_id), 0),
        2
    ) AS dropout_rate,

    ROUND(
        SUM(
            CASE
                WHEN f.grade_numeric IS NOT NULL
                THEN f.grade_numeric * m.Credits
            END
        )
        /
        NULLIF(
            SUM(
                CASE
                    WHEN f.grade_numeric IS NOT NULL
                    THEN m.Credits
                END
            ),
            0
        ),
        2
    ) AS weighted_avg_grade,

    MEDIAN(f.grade_numeric) AS median_grade,

    (
        SELECT ROUND(AVG(fehlversuche), 2)
        FROM (
            SELECT
                fk.student_id,
                SUM(CASE WHEN fk.passed_flag = 0 THEN 1 ELSE 0 END) AS fehlversuche
            FROM fact_base fk
            GROUP BY fk.student_id
        ) t
        WHERE t.student_id IN (
            SELECT ds.student_id
            FROM students_final.dim_students ds
            WHERE ds.year_of_matriculation = s.year_of_matriculation
        )
    ) AS avg_failed_attempts,

    ROUND(
        100.0 * oa.ontrack_terms
        / NULLIF(oa.total_terms, 0),
        2
    ) AS ontrack_rate

FROM students_final.dim_students s
LEFT JOIN fact_base f
    ON f.student_id = s.student_id
LEFT JOIN students_final.dim_modul m
    ON f.module_id = m.module_id
LEFT JOIN ontrack_agg oa
    ON oa.year_of_matriculation = s.year_of_matriculation

GROUP BY
    s.year_of_matriculation,
    oa.ontrack_terms,
    oa.total_terms

ORDER BY
    intake_year