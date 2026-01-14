WITH fact_base AS (
    SELECT * FROM {{ ref('fact_kpi') }}
),

students as (
    select * from {{ ref('stg_students_with_credits') }}
),

modules as (
    select * from {{ ref('dim_modul') }}
),

students_total AS (
    SELECT module_id, COUNT(DISTINCT student_id) as count
    FROM fact_kpi
    GROUP BY module_id
),

students_passed_total AS (
    SELECT module_id, COUNT(DISTINCT student_id) as count
    FROM fact_kpi
    WHERE exam_status = 'bestanden'
    GROUP BY module_id
),

students_failed_total AS (
    SELECT module_id, COUNT(DISTINCT student_id) as count
    FROM fact_kpi
    WHERE exam_status = 'nicht bestanden'
    GROUP BY module_id
),

students_finally_failed_count AS (
    SELECT module_id, COUNT(DISTINCT student_id) AS failed_count
    FROM fact_base
    WHERE exam_status = 'nicht bestanden' AND tries_count = 3
    GROUP BY module_id
),

students_passed_first_try_count AS (
    SELECT module_id, COUNT(DISTINCT student_id) AS succeded_count 
    FROM fact_kpi
    WHERE exam_status = 'bestanden' AND tries_count = 1
    GROUP BY module_id
),

-- TODO
students_succeeded_semester AS (
    SELECT time_id, student_id 
    FROM fact_base
    GROUP BY time_id, student_id
    HAVING SUM(CASE WHEN exam_status <> 'bestanden' THEN 1 ELSE 0 END) = 0
       AND SUM(CASE WHEN tries_count <> 1 THEN 1 ELSE 0 END) = 0
),

bottleneck_value AS (
    SELECT 
        module_id,
        COUNT(*) FILTER (WHERE NOT (tries_count = 1 AND passed_flag = 1)) * 1.0 / COUNT(*) 
            AS bottleneck_value
    FROM fact_base
    GROUP BY module_id
),

avg_exam_delay as (
    SELECT
        module_number,
        AVG(exam_plan_dif) as avg_exam_plan_diff
    FROM students
    GROUP BY module_number
),

acronym as (
    SELECT
        module_id,
        ANY_VALUE(module_acronym) as module_acronym
    FROM modules
    GROUP BY module_id
)

SELECT
    f.module_id,
    a.module_acronym,
    COALESCE(sf.failed_count, 0) AS students_finally_failed_count,
    COALESCE(ssm.succeded_count, 0) AS students_passed_first_try_count,
    ROUND((COALESCE(stft.count, 0) / (COALESCE(stft.count, 1) + COALESCE(stpt.count, 1))) * 100, 2) AS students_failed_percent,
    ROUND((COALESCE(sf.failed_count, 0) / COALESCE(st.count, 1)) * 100, 2) AS students_final_fail_percent,
    ROUND(mb.bottleneck_value, 2) as bottleneck_value,
    ROUND(aed.avg_exam_plan_diff, 2) as avg_exam_plan_diff
FROM (
    SELECT DISTINCT module_id
    FROM fact_base
) f
LEFT JOIN students_total st
    ON f.module_id = st.module_id
LEFT JOIN students_passed_total stpt
    ON f.module_id = stpt.module_id
LEFT JOIN students_failed_total stft
    ON f.module_id = stft.module_id
LEFT JOIN students_finally_failed_count sf
    ON f.module_id = sf.module_id
LEFT JOIN students_passed_first_try_count ssm
    ON f.module_id = ssm.module_id
JOIN bottleneck_value mb
    ON f.module_id = mb.module_id
JOIN avg_exam_delay aed
    ON f.module_id = aed.module_number
JOIN acronym a
    ON f.module_id = a.module_id
ORDER BY f.module_id