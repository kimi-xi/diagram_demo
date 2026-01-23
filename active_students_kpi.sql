{{ config(materialized='table') }}

WITH student_active_semesters AS (
    SELECT DISTINCT
        f.student_id,
        f.year,
        f.semester_type
    FROM {{ ref('stg_students_with_credits') }} f
    WHERE f.exam_status <> 'abgemeldet'
),

student_active_semesters_fos AS (
    SELECT
        a.student_id,
        a.year,
        a.semester_type,
        s.field_of_study
    FROM student_active_semesters a
    INNER JOIN {{ ref('dim_students') }} s
        ON a.student_id = s.student_id
)

SELECT
    year,
    semester_type,
    field_of_study,
    COUNT(student_id) AS students_active_count
FROM student_active_semesters_fos
GROUP BY
    year,
    semester_type,
    field_of_study
ORDER BY
    year