WITH fact_base AS (
    SELECT * FROM {{ ref('fact_kpi') }}
)

SELECT student_id, module_id, grade_numeric FROM fact_base
WHERE grade_numeric IS NOT NULL