-- models/staging/stg_students_clean.sql

{{ config(
    pre_hook=[
        "CREATE TYPE IF NOT EXISTS academic_semester AS ENUM ('WiSe', 'SoSe');",
        "CREATE TYPE IF NOT EXISTS exam_remark AS ENUM ('abgemeldet', 'anerkannt', 'krank', 'nicht zugelassen', 'unentschuldigt gefehlt');",
        "CREATE TYPE IF NOT EXISTS exam_status AS ENUM ('bestanden', 'nicht bestanden', 'abgemeldet');"
    ]
) }}

with source_data as (
    select * from students WHERE 
    -- Filter out everything that is not the main grade
    Modul not like '%APL%' AND 
    Modul not like '%SPL%' AND  
    Modul not like '%PVL%' AND 
    Modul not like '%MP%' AND 
    Modul not like '%schriftlicher Teil%' AND 
    Modul not like '%mündlicher Teil%' AND 
    Modul not like '%mndlicher Teil%' AND 
    Modul not like '%SP%' AND 
    Modul not like '%Semesterarbeit%' AND 
    Modul not like '%BGA%' AND 
    Modul not like '%Beleg%' AND 
    Modul not like '%schriftlich%' AND 
    Modul not like '%Verteidigung%' AND 
    Modul not like '%schriftliche Arbeit%' AND
    Modul not like '%Forschungseminar 2%' AND  Modul not like '%Forschungseminar 1%' AND
    Modul not like '%Forschungseminar Teil 2%' AND  Modul not like '%Forschungseminar Teil 1%' AND
    Modul not like '%Forschungsseminar 2%' AND  Modul not like '%Forschungsseminar 1%' AND
    Modul not like '%Forschungsseminar Teil 2%' AND  Modul not like '%Forschungsseminar Teil 1%' AND
  
    NOT (Modul like '%SP%' AND Modul not like '%Beleg%') AND
    NOT (Modul like '%Beleg%' AND Modul not like '%SP%')
),

module_mapping as (
    select
        *,
        -- Map special module names to module numbers when Modulnummer is NULL TO Be used later when merging in the module data
        case
            when Modulnummer IS NULL and (Modul like '%Wahlpflichtmodul (Ausnahme)%' OR Modul like '%Wahlpflichtmodul (anerkannt)%') then 'XWM'
            when Modulnummer IS NULL and Modul like '%Zusatzmodul%' then 'XZM'
            when Modulnummer IS NULL and Modul like '%Forschungs-/Entwicklungsprojekt%' then 'I705'
            when Modulnummer IS NULL and Modul like '%Wahlpflichtmodul (Ausnahme)%' then 'XWM'
            when Modulnummer IS NULL and Modul like '%Mathematische/Stochastische Modelle%' then 'I886'
            when Modulnummer = 'Zusatzmodul' then 'XZM'
            when Modulnummer = 'Wahlpflichtmodul' then 'XWM'
            else COALESCE(Modulnummer, 'XQQ') -- XQQ is a placeholder for unknown module numbers
        end as Modulnummer_mapped
    from source_data
),

cleaned as (
    select
        studentID,
        Modulnummer_mapped as Modulnummer,
        Studienrichtung,
        Note as Note_original,
        Vermerk as Vermerk_original,
        
        -- Clean and normalize grades (Note)
        case
            -- Rule: Grade 10 -> 1.0
            when Note = '10' then '1,0'
            -- Strip invalid grades
            when Note in ('0,0 mE', '02.01.1900', 'angemeldet') then null
            -- Rule: bestanden -> NULL (will be handled in status)
            when Note = 'bestanden' then null
            -- Clean up whitespace and other weird characters
            when Note is not null then trim(regexp_replace(Note, '[\r\n\t\s]+', '', 'g'))
            -- Normalize decimal format
            else null
        end as Note_cleaned,
        
        -- Convert cleaned note to numeric for calculations
        case
            when Note = '10' then 1.0
            when Note_cleaned IS NULL then null
            -- try to convert the cleaned note to a decimal number
            else try_cast(replace(Note_cleaned, ',', '.') as decimal(3,1))
         
        end as Note_numeric,
        
        -- Determine exam status (using cleaned Note)
        case
            -- Rule: Note: bestanden -> Status: Bestanden 
            when Note = 'bestanden' then 'bestanden'
            -- Rule: Note: NULL, Vermerk: mit Erfolg -> Status: Bestanden
            when (Note_cleaned IS NULL OR Note_cleaned = '') and Vermerk = 'mit Erfolg' then 'bestanden'
            -- Rule: Note: NULL, Vermerk: NULL -> Status: Abgemeldet
            when Note IS NULL and Vermerk is NULL then 'abgemeldet'
            -- Rule: krank -> status abgemeldetm nicht zugelassen -> status abgemeldet
            when Vermerk = 'krank' OR  Vermerk = 'nicht zugelassen' then 'abgemeldet'
            -- Rule: Note: angemeldet, Vermerk: NULL -> status abgemeldet
            when Note = 'angemeldet' and Vermerk is NULL then 'abgemeldet' -- wahrscheinlich Abgemeldet und kein Vermerk

            when Vermerk = 'unentschuldigt gefehlt' then 'nicht bestanden'
            
            -- GRADE-BASED RULES (using Note_cleaned):
            -- Rule: Note 1-4 -> Status: bestanden
            when Note_numeric >= 1.0 and Note_numeric <= 4.0 then 'bestanden'
            -- Rule: Note: 5.0 -> Status: nicht bestanden
            when Note_numeric = 5.0 then 'nicht bestanden'
            -- Rule: Vermerk: abgemeldet -> status abgemeldet
            when Vermerk = 'abgemeldet' then 'abgemeldet'
            -- Rule: Vermerk: abgemeldet, Note: 5.0 -> status nicht bestanden
            when Vermerk = 'abgemeldet' and Note_numeric = 5.0 then 'nicht bestanden'
            -- Default: bestanden if we got here with a note
            when Note_cleaned is not null and Note_cleaned != '' then 'bestanden'
            else null
        end as exam_status_temp,
        
        -- Semester normalization
        case
            -- Semester Cleanup
            when upper(Semester) = 'WISE' then 'WiSe'
            when upper(Semester) = 'SOSE' then 'SoSe'
            else Semester
        end as Semester_cleaned,
        
        -- Jahr handling
        case
            when Jahr = 29 then 19  -- Typo fix
            when Jahr between 11 and 25 then Jahr
            else null
        end as Jahr_cleaned,
        
        
        -- Vermerk cleaning with special rules
        case
            -- Rule: When Note: 5, clear everything except 'unentschuldigt gefehlt'
            when Note = '5' and Vermerk != 'unentschuldigt gefehlt' then null
            -- -- Rule: krank becomes abgemeldet status, so clear vermerk
            -- when Vermerk = 'krank' then NULL
            -- Strip 'mit Erfolg' and 'kein Antrag' (already handled in status)
            when Vermerk in ('mit Erfolg', 'kein Antrag') then null
            -- Keep valid vermerke
            when Vermerk in (
                'abgemeldet',
                'anerkannt', 
                'krank',
                'nicht zugelassen',
                'unentschuldigt gefehlt'
            ) then Vermerk
            else null
        end as Vermerk_cleaned
    
    from module_mapping
),

with_term_counter as (
    select
        *,
        -- Calculate term counter for each student based on chronological semester order
        -- SoSe comes before WiSe in the same year
        DENSE_RANK() OVER (
            PARTITION BY studentID 
            ORDER BY 
                Jahr_cleaned,
                CASE 
                    WHEN Semester_cleaned = 'SoSe' THEN 1
                    ELSE 2
                END
        ) as term_counter,
        -- Get the first year a student has any entry (immatriculation year)
        MIN(Jahr_cleaned) OVER (PARTITION BY studentID) as ImmaJahr,
        -- Get the semester from the first term (immatriculation semester)
        first_value(Semester_cleaned) OVER (
            PARTITION BY studentID
            ORDER BY Jahr_cleaned, CASE WHEN Semester_cleaned = 'SoSe' THEN 1 ELSE 2 END
            rows between unbounded preceding and unbounded following
        ) as ImmaSemester
    from cleaned
    WHERE Jahr_cleaned IS NOT NULL 
        AND Semester_cleaned IS NOT NULL
),

-- Get primary Studienrichtung per student (from non-special modules)
primary_direction as (
    select
        studentID,
        mode() within group (order by Studienrichtung) as primary_studienrichtung
    from with_term_counter
    where Modulnummer not in ('XWM', 'XZM', 'XQQ')
    group by studentID
),

-- Normalize Studienrichtung for XWM modules to student's actual direction
normalized as (
    select
        w.*,
        coalesce(p.primary_studienrichtung, w.Studienrichtung) as primary_studienrichtung
    from with_term_counter w
    left join primary_direction p on w.studentID = p.studentID
)

select
    studentID as student_id,
    Modulnummer as module_number,
    Note_cleaned as grade,
    Note_numeric as grade_numeric,
    cast(exam_status_temp as exam_status) as exam_status,
    cast(Semester_cleaned as academic_semester) as semester_type,
    Jahr_cleaned as year,
    cast(Vermerk_cleaned as exam_remark) as exam_annotation,
    -- For XWM/XZM/XQQ modules, use student's primary Studienrichtung
    case 
        when Modulnummer in ('XWM', 'XZM', 'XQQ') then coalesce(primary_studienrichtung, Studienrichtung)
        else Studienrichtung
    end as field_of_study,
    term_counter,
    ImmaJahr as year_of_matriculation,
    ImmaSemester as semester_of_matriculation
from normalized