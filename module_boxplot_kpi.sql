WITH fact_base AS (
    SELECT * FROM {{ ref('fact_kpi') }}
)

SELECT
    module_id,
    QUANTILE_CONT(grade_numeric, 0.25) as first_quartile_grade,
    QUANTILE_CONT(grade_numeric, 0.75) as third_quartile_grade,
    MEDIAN(grade_numeric) as median_grade,
    ROUND(AVG(grade_numeric), 2) as mean_grade
FROM fact_base
GROUP BY module_id