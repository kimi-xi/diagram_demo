WITH offtrack AS (
SELECT
    -- Anzahl aller Semester, die off-track sind
    SUM(ontrack_flag) AS total_offtrack_semesters,

    -- Anzahl aller Semester im Studiengang
    COUNT(*) AS total_semesters
    FROM {{ ref('study_progress_kpi') }}
),

avg_terms_taken AS (
    SELECT AVG(terms_taken_total_count) AS avg_terms_taken
    FROM dim_students
    WHERE graduation_flag = true
),

students_with_degree AS (
    SELECT DISTINCT student_id
    FROM fact_kpi
    WHERE module_id = 'I707'
      AND grade_numeric IS NOT NULL
      AND grade_numeric <> 5
),

students_in_standard_period_of_study AS (
    SELECT DISTINCT d.student_id
    FROM dim_students d
    JOIN students_with_degree s
        ON s.student_id = d.student_id
    WHERE d.terms_taken_total_count <= 4
)

SELECT
'entire_program' AS study_program_label,
-- Gesamter Studiengang

COUNT(DISTINCT s.student_id) AS students_total_count,
-- Gesamtzahl an Studierenden

ROUND(
100.0 * COUNT(DISTINCT CASE
WHEN m.module_id = 'I707'
AND f.passed_flag = 1
THEN s.student_id
END)
/ NULLIF(COUNT(DISTINCT s.student_id), 0), 2
) AS graduation_rate,
-- Anteil der Studierenden, die den Master erfolgreich abgeschlossen haben

ROUND(
100.0 * COUNT(DISTINCT CASE
WHEN (m.module_id = 'I707'
AND f.passed_flag = 0
AND f.tries_count >= 3)
OR s.inactivity_flag = 1
THEN s.student_id
END)
/ NULLIF(COUNT(DISTINCT s.student_id), 0), 2
) AS dropout_rate,
-- Abbruchquote: endgültig durchgefallen oder länger inaktiv

ROUND(
SUM(CASE
WHEN f.passed_flag = 1
AND f.grade_numeric IS NOT NULL
THEN f.grade_numeric * f.credits_earned
END)
/ NULLIF(
SUM(CASE
WHEN f.passed_flag = 1
AND f.grade_numeric IS NOT NULL
THEN f.credits_earned
END),
0
), 2
) AS weighted_avg_program_grade,
-- gewichteter Notendurchschnitt aller bestandenen Leistungen

MEDIAN(f.grade_numeric) AS median_program_grade,
-- Median-Note aller vergebenen Noten

-- Anteil der Semester im gesamten Studiengang, die off track waren
ROUND(
(SELECT total_offtrack_semesters FROM offtrack)
* 100.0 /
NULLIF((SELECT total_semesters FROM offtrack), 0),
2
) AS offtrack_rate,
-- Prozentsatz aller Semester, in denen die Studierenden deutlich vom Sollwert abgewichen sind

-- Anteil der Semester, die on track waren
ROUND(
100 -
((SELECT total_offtrack_semesters FROM offtrack)
* 100.0 /
NULLIF((SELECT total_semesters FROM offtrack), 0)),
2
) AS ontrack_rate,
-- Prozentsatz aller Semester, die im normalen Erwartungsbereich lagen

MAX(avg_terms_taken.avg_terms_taken) AS 'avg_terms_taken',

ROUND((COUNT(s_ontrack.student_id) / NULLIF(COUNT(s_all.student_id), 1) * 100), 2) AS master_on_track_percent

FROM {{ ref('fact_kpi') }} AS f
JOIN {{ ref('dim_students') }} AS s ON f.student_id = s.student_id
JOIN {{ ref('dim_modul') }} AS m ON f.module_id = m.module_id
CROSS JOIN avg_terms_taken
CROSS JOIN students_with_degree s_all
LEFT JOIN students_in_standard_period_of_study s_ontrack ON s_all.student_id = s_ontrack.student_id