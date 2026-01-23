WITH fact_base AS (
    SELECT * FROM {{ ref('fact_kpi') }}
)

SELECT student_id, module_id, exam_status FROM fact_base