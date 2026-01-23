WITH student_kpi AS (
    SELECT
        student_id,
        module_id,
        MAX(
            CASE 
                WHEN passed_flag = 1 THEN 1 
                ELSE 0 
            END
        ) AS module_passed,
        SUM(
          CASE WHEN passed_flag = 0 THEN 1 ELSE 0 END
        ) AS failed_attempts,
        COUNT(*) AS attempts,
        SUM(credits_earned) AS credits_count_temp
    FROM {{ ref('fact_kpi') }}
    GROUP BY student_id, module_id
),
grades_agg AS (
    SELECT
        student_id,
        ROUND(
          SUM(grade_numeric * credits_earned) / NULLIF(SUM(credits_earned), 0),
          2
        ) AS weighted_avg_grade,
        MEDIAN(grade_numeric) AS median_grade
    FROM {{ ref('fact_kpi') }}
    WHERE grade_numeric IS NOT NULL
    GROUP BY student_id
)

SELECT
    m.student_id,

    -- Module gesamt (Anzahl verschiedener Module)
    COUNT(*) AS modules_count,

    -- Anzahl bestandener Module
    SUM(module_passed) AS modules_passed_count,

    -- Anzahl Fehlversuche
    SUM(failed_attempts) AS failed_attempts_count,

    -- Anzahl Wiederholungsmodule (mindestens 2 Versuche)
    SUM(CASE WHEN attempts > 1 THEN 1 ELSE 0 END) AS modules_failed_count,

    -- Summe Credits
    SUM(credits_count_temp) AS credits_total_count,
 -- gewichteter Notenschnitt nach Credits (pro Student aus grades_agg)
    n.weighted_avg_grade,
    
    -- Median-Note pro Student
    n.median_grade,

      -- Bestehensrate
    ROUND(
        SUM(module_passed) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) AS pass_rate,

    -- Nichtbestehensrate
    ROUND(
        100 - (SUM(module_passed) * 100.0 / NULLIF(COUNT(*), 0)),
        2
    ) AS fail_rate,

    -- Wiederholungsrate 
    ROUND(
        SUM(CASE WHEN attempts > 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) AS repetition_rate,

    FIRST(ml.predicted_has_passed_degree) AS predicted_has_passed_degree,
    FIRST(ml.has_passed_degree_confidence) AS has_passed_degree_confidence,
    FIRST(ml.predicted_inactivity) AS predicted_inactivity,
    FIRST(ml.inactivity_confidence) AS inactivity_confidence,
    FIRST(ml.lime_top5_inact) AS lime_top5_inact,
    FIRST(ml.lime_top5_pass) AS lime_top5_pass

FROM student_kpi m
LEFT JOIN grades_agg n
  ON m.student_id = n.student_id
LEFT JOIN {{ source('ml_source', 'student_ml') }} ml
  ON m.student_id = ml.student_id
GROUP BY
    m.student_id,
    weighted_avg_grade,
    median_grade