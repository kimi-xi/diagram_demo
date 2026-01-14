WITH fact_base AS (
    SELECT * FROM {{ ref('fact_kpi') }}
),

students_with_master_thesis AS (
    SELECT student_id, grade_numeric AS master_thesis_grade
    FROM fact_base
    WHERE module_id = 'I707' AND grade_numeric IS NOT NULL
),

students_avg AS (
	SELECT f.student_id, AVG(f.grade_numeric) AS avg_final_grade, MIN(s.master_thesis_grade) AS master_thesis_grade
	FROM fact_base f
	JOIN students_with_master_thesis s ON f.student_id = s.student_id
	GROUP BY f.student_id
)

SELECT ROUND(AVG(avg_final_grade), 2) AS students_with_thesis_avg_final_grade, 
    ROUND(AVG(master_thesis_grade), 2) AS students_with_thesis_avg_master_thesis_grade
FROM students_avg