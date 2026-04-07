
-- #########################################################
-- # PERSON
-- #########################################################

-- Solo permite causa de muerte si existe la fecha de muerte
UPDATE PERSON 
SET de_cause = NULL 
WHERE de_date IS NULL;


-- #########################################################
-- # SCREENING_ROUND
-- #########################################################

-- En la ronda 1, la asistencia previa debe ser "No Aplicable" (2)
UPDATE SCREENING_ROUND 
SET screening_previous_attendance = 2 
WHERE screening_round_number = 1;

-- Eliminar fecha de invitación si el estado no es "Invitado" (1)
UPDATE SCREENING_ROUND 
SET primary_invitation_date = NULL 
WHERE primary_invited != 1;

-- Si la invitación es No Aplicable (2), la asistencia también debe ser 2
UPDATE SCREENING_ROUND 
SET primary_test_attended = 2 
WHERE primary_invited = 2;

-- Impedir que aparezcan como asistentes (1) si no fueron invitados (0, 2)
UPDATE SCREENING_ROUND 
SET primary_test_attended = 0 
WHERE primary_invited IN (0, 2) AND primary_test_attended = 1;


-- #########################################################
-- # COLON_PRIMARY
-- #########################################################

-- Si la invitación es No Aplicable (2), la asistencia también debe ser 2
UPDATE COLON_PRIMARY 
SET primary_test_attended = 2 
WHERE primary_invited = 2;

-- Borrar fechas de invitación para estados No Invitado, No Aplicable o Desconocido
UPDATE COLON_PRIMARY 
SET primary_invitation_date = NULL 
WHERE primary_invited IN ('0', '2', '99');

-- Solo permitir complicaciones en métodos de tipo endoscopia
UPDATE COLON_PRIMARY 
SET primary_test_complications = NULL 
WHERE primary_test_method NOT LIKE '%endoscopy%' 
  AND primary_test_complications IS NOT NULL;


-- #########################################################
-- # COLON_ASSESSMENT
-- #########################################################

-- Borrar fecha de evaluación si el paciente no asistió (0)
UPDATE COLON_ASSESSMENT 
SET assessment_date = NULL 
WHERE assessment_attended = '0';

-- Borrar el estadio de cáncer si no se ha especificado el tipo de estadiaje
UPDATE COLON_ASSESSMENT 
SET assessment_phase_cancer_stage = NULL 
WHERE assessment_phase_cancer_stage IS NOT NULL 
  AND assessment_phase_stage_type IS NULL;


-- #########################################################
-- # COLON_TREATMENT
-- #########################################################

-- Borrar fecha de tratamiento si el paciente no asistió (0)
UPDATE COLON_TREATMENT 
SET treatment_date = NULL 
WHERE treatment_attended = '0';


-- #########################################################
-- # COLON_INTERVAL
-- #########################################################

-- Si no hay fecha de diagnóstico, se asume que el cáncer de intervalo no existe
UPDATE COLON_INTERVAL 
SET interval_cancer_exists = 'false' 
WHERE interval_cancer_exists = 'true' 
  AND interval_cancer_dg_date IS NULL;