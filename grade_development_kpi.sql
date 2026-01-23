{{ config(materialized='table') }}

SELECT
    z.year,
    z.semester_type,

    ROUND(SUM(
            CASE
                WHEN f.passed_flag = 1
                    AND f.grade_numeric IS NOT NULL
                THEN f.grade_numeric * f.credits_earned
            END
        )/
        NULLIF(SUM(
                CASE
                    WHEN f.passed_flag = 1
                        AND f.grade_numeric IS NOT NULL
                    THEN f.credits_earned
                END
            ),
            0
        ),
        2
    ) AS average_grade_this_semester

FROM {{ ref('fact_kpi') }} f
INNER JOIN {{ ref('dim_zeit') }} z
    ON f.time_id = z.time_id

GROUP BY
    z.year,
    z.semester_type

ORDER BY
    z.year,
    z.semester_type