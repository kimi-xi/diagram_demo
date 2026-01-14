-- Module dimension table
-- One row per module with all module attributes

select
    Modulnummer AS module_id,
    Modul AS module_title,
    Acronym AS module_acronym,
    Credits AS credits,
    AbJahr AS graduation_year,
    AbSemester AS graduation_semester,
    concat(AbJahr, '-', AbSemester, '-', Modulnummer, '-', Studienrichtung) as module_key,
    Semester AS recommended_semester,
    Studienrichtung AS field_of_study,
    Art AS module_type,
    Professor AS professor_name,
    APL AS apl_flag,
    "schriftliche Prüfung" AS written_flag,
    "mündliche Prüfung" AS oral_flag
from {{ ref('module') }}

