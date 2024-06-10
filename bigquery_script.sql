#1. filter Neuro SICU from icustays
create table recovery-planner.data.icustays2 as (
-- Step 1: Get records where first_careunit is Neuro SICU but last_careunit is not
SELECT count(*)
FROM physionet-data.mimiciv_icu.icustays
WHERE first_careunit = 'Neuro Surgical Intensive Care Unit (Neuro SICU)' 
AND last_careunit != 'Neuro Surgical Intensive Care Unit (Neuro SICU)'
UNION ALL
-- Step 2: Get records where last_careunit is Neuro SICU but first_careunit is not
SELECT count(*)
FROM physionet-data.mimiciv_icu.icustays
WHERE last_careunit = 'Neuro Surgical Intensive Care Unit (Neuro SICU)'
AND first_careunit != 'Neuro Surgical Intensive Care Unit (Neuro SICU)'
UNION ALL
-- Step 3: Get one instance of the intersection (first_careunit and last_careunit both Neuro SICU)
SELECT count(*)
FROM physionet-data.mimiciv_icu.icustays
WHERE first_careunit = 'Neuro Surgical Intensive Care Unit (Neuro SICU)'
AND last_careunit = 'Neuro Surgical Intensive Care Unit (Neuro SICU)'
-- LIMIT 1
-- ) a
)
;

#2. join patients info tables
create table recovery-planner.data.patients2 as (
  select
  a.subject_id, a.hadm_id, p.gender, p.anchor_age, a.race, admission_type, admission_location, discharge_location, hospital_expire_flag, i.stay_id, i.intime, i.outtime, i.los, d.icd_code, dd.long_title, first_careunit, last_careunit
  from physionet-data.mimiciv_hosp.patients AS p
  inner join physionet-data.mimiciv_hosp.admissions AS a
    on p.subject_id = a.subject_id
  inner join recovery-planner.data.icustays2 as i
    on a.subject_id = i.subject_id
    and a.hadm_id = i.hadm_id
  inner join physionet-data.mimiciv_hosp.diagnoses_icd as d
    on i.subject_id = d.subject_id
    and i.hadm_id = d.hadm_id
  inner join physionet-data.mimiciv_hosp.d_icd_diagnoses as dd
    on d.icd_code = dd.icd_code
);

## chartevents numeric columns pivoting
SELECT 
  subject_id, 
  hadm_id,
  stay_id,
  ROUND(AVG(CASE WHEN itemid = 223900 THEN valuenum END), 2) AS GCS_VerbalResponse,
  ROUND(AVG(CASE WHEN itemid = 220739 THEN valuenum END), 2) AS GCS_EyeOpening,
  ROUND(AVG(CASE WHEN itemid = 223901 THEN valuenum END), 2) AS GCS_MotorResponse,
  ROUND(AVG(CASE WHEN itemid = 228096 THEN valuenum END), 2) AS Richmond_RAS_Scale,
  ROUND(AVG(CASE WHEN itemid = 227345 THEN valuenum END), 2) AS Gait_Transferring,
  ROUND(AVG(CASE WHEN itemid = 227346 THEN valuenum END), 2) AS MentalStatus,
  ROUND(AVG(CASE WHEN itemid = 227342 THEN valuenum END), 2) AS SecondaryDiagnosis,
  ROUND(AVG(CASE WHEN itemid = 220045 THEN valuenum END), 2) AS HeartRate,
  ROUND(AVG(CASE WHEN itemid = 220210 THEN valuenum END), 2) AS RespiratoryRate,
  ROUND(AVG(CASE WHEN itemid = 220180 THEN valuenum END), 2) AS BP_diastolic,
  ROUND(AVG(CASE WHEN itemid = 220179 THEN valuenum END), 2) AS BP_systolic,
FROM `physionet-data.mimiciv_icu.chartevents`
WHERE itemid IN (223900, 220739, 223901, 228096, 227345, 227346, 227342,
  220045,220210,220180,220179)
GROUP BY 1,2,3;

#3. chartevents join - use different statistics like MIN, MAX, AVG
CREATE TABLE recovery-planner.data.EHR2 AS (
WITH RankedEvents AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        itemid,
        valuenum,
        charttime,
        ROW_NUMBER() OVER (PARTITION BY subject_id, hadm_id, stay_id, itemid ORDER BY charttime DESC) AS rn
    FROM
        `physionet-data.mimiciv_icu.chartevents`
    WHERE
        itemid IN (227346, 227342) -- MentalStatus and SecondaryDiagnosis
), LatestValues AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        MAX(CASE WHEN itemid = 227346 THEN valuenum END) AS MentalStatus,
        MAX(CASE WHEN itemid = 227342 THEN valuenum END) AS SecondaryDiagnosis
    FROM RankedEvents
    WHERE rn = 1
    GROUP BY subject_id, hadm_id, stay_id
), AggregatedValues AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        MIN(CASE WHEN itemid = 223900 THEN valuenum END) AS GCS_VerbalResponse,
        MIN(CASE WHEN itemid = 220739 THEN valuenum END) AS GCS_EyeOpening,
        MIN(CASE WHEN itemid = 223901 THEN valuenum END) AS GCS_MotorResponse,
        ROUND(AVG(CASE WHEN itemid = 228096 THEN valuenum END), 2) AS Richmond_RAS_Scale,
        ROUND(AVG(CASE WHEN itemid = 220045 THEN valuenum END), 2) AS HeartRate,
        ROUND(AVG(CASE WHEN itemid = 220210 THEN valuenum END), 2) AS RespiratoryRate,
        MAX(CASE WHEN itemid = 220180 THEN valuenum END) AS BP_diastolic,
        MAX(CASE WHEN itemid = 220179 THEN valuenum END) AS BP_systolic
    FROM `physionet-data.mimiciv_icu.chartevents`
    WHERE itemid IN (223900, 220739, 223901, 228096, 220045, 220210, 220180, 220179)
    GROUP BY subject_id, hadm_id, stay_id
)
SELECT
    A.subject_id,
    A.hadm_id,
    A.stay_id,
    A.MentalStatus,
    A.SecondaryDiagnosis,
    B.GCS_VerbalResponse,
    B.GCS_EyeOpening,
    B.GCS_MotorResponse,
    B.Richmond_RAS_Scale,
    B.HeartRate,
    B.RespiratoryRate,
    B.BP_diastolic,
    B.BP_systolic
FROM LatestValues A
INNER JOIN AggregatedValues B
ON A.subject_id = B.subject_id AND A.hadm_id = B.hadm_id AND A.stay_id = B.stay_id
)
;

#4. create readmission column
CREATE TABLE recovery-planner.data.readmission2 AS (
WITH A AS (
  SELECT 
    subject_id,
    intime,
    DENSE_RANK() OVER (PARTITION BY subject_id ORDER BY intime) AS admission_order
FROM recovery-planner.data.patients2
)
SELECT
    subject_id,
    intime, 
    admission_order,
    CASE WHEN admission_order=1 THEN 0 ELSE 1 END AS readmission_flag
FROM A
GROUP BY 1,2,3
ORDER BY 1,2,3
);

#5. create train dataset
create table recovery-planner.data.data4 as (
-- with a as (
select 
    a.subject_id, a.hadm_id, a.stay_id, gender, anchor_age, race, admission_type, admission_location, 
    discharge_location, hospital_expire_flag, a.intime, a.outtime, round(los, 2) as los, 
    long_title as diagnoses_title, 
    GCS_VerbalResponse, GCS_EyeOpening, GCS_MotorResponse, Richmond_RAS_Scale, MentalStatus, SecondaryDiagnosis, 
    HeartRate, RespiratoryRate, BP_diastolic, BP_systolic, 
    admission_order, readmission_flag
from recovery-planner.data.patients2 a
inner join recovery-planner.data.EHR2 b
on a.subject_id = b.subject_id
  and a.hadm_id = b.hadm_id
  and a.stay_id = b.stay_id
inner join recovery-planner.data.readmission2 c
on a.subject_id = c.subject_id
  and a.intime = c.intime
);


## check itemid and label
-- select 
-- distinct value, valuenum
-- from physionet-data.mimiciv_icu.chartevents
-- where itemid  = 220180
-- order by 2


## create all units table for analysis
CREATE TABLE recovery-planner.data.readmission_allunit AS (
WITH A AS (
  SELECT 
    subject_id,
    intime,
    first_careunit,
    last_careunit,
    DENSE_RANK() OVER (PARTITION BY subject_id ORDER BY intime) AS admission_order
FROM physionet-data.mimiciv_icu.icustays
group by 1,2,3,4
)
SELECT
    subject_id,
    intime, 
    first_careunit,
    last_careunit,
    admission_order,
    CASE WHEN admission_order=1 THEN 0 ELSE 1 END AS readmission_flag
FROM A
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
)