-- creates all the tables and produces csv files
.cd data/mimic-iv-0.4

.print "Listing of mimic-iv data files"
.system tree

labels table
.print "Creating labels table"
drop table if exists ld_labels;
create table ld_labels as
  select i.subject_id, i.hadm_id, i.stay_id, i.intime, i.outtime, adm.hospital_expire_flag, i.los
    from 'icu/icustays.csv.gz' as i
    inner join 'core/admissions.csv.gz' as adm
      on adm.hadm_id = i.hadm_id
    inner join 'core/patients.csv.gz' as p
      on p.subject_id = i.subject_id
    where i.los > (5/24)  -- and exclude anyone who doesn't have at least 5 hours of data
      and (extract(year from i.intime) - p.anchor_year + p.anchor_age) > 17;  -- only include adults

-- flat  features table
-- \i MIMIC_preprocessing/flat_features.sql

drop table if exists extra_vars;
create table extra_vars as
SELECT
  ch.stay_id,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'Admission Weight (Kg)') AS weight,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'GCS - Eye Opening') AS eyes,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'GCS - Motor Response') AS motor,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'GCS - Verbal Response') AS verbal,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'Height (cm)') AS height
FROM 'icu/chartevents.csv.gz' AS ch
JOIN 'icu/icustays.csv.gz' AS i ON ch.stay_id = i.stay_id
JOIN 'icu/d_items.csv.gz' AS d ON d.itemid = ch.itemid
WHERE ch.valuenum IS NOT NULL
  AND d.label IN ('Admission Weight (Kg)', 'GCS - Eye Opening', 'GCS - Motor Response', 'GCS - Verbal Response', 'Height (cm)')
  AND ch.valuenum != 0
  AND (strftime('%H', ch.charttime) - strftime('%H', i.intime)) BETWEEN -24 AND 5
GROUP BY ch.stay_id;

  -- select * from crosstab(
  --   'select ch.stay_id, d.label, avg(valuenum) as value
  --     from chartevents as ch
  --       inner join icustays as i
  --         on ch.stay_id = i.stay_id
  --       inner join d_items as d
  --         on d.itemid = ch.itemid
  --       where ch.valuenum is not null
  --         and d.label in (''Admission Weight (Kg)'', ''GCS - Eye Opening'', ''GCS - Motor Response'', ''GCS - Verbal Response'', ''Height (cm)'')
  --         and ch.valuenum != 0
  --         and date_part(''hour'', ch.charttime) - date_part(''hour'', i.intime) between -24 and 5
  --       group by ch.stay_id, d.label'
  --       ) as ct(stay_id integer, weight double precision, eyes double precision, motor double precision, verbal double precision, height double precision);


-- drop materialized view if exists ld_flat cascade;
-- create materialized view ld_flat as
--   select distinct i.stay_id as patientunitstayid, p.gender, (extract(year from i.intime) - p.anchor_year + p.anchor_age) as age,
--     adm.ethnicity, i.first_careunit, adm.admission_location, adm.insurance, ev.height, ev.weight,
--     extract(hour from i.intime) as hour, ev.eyes, ev.motor, ev.verbal
--     from ld_labels as la
--     inner join patients as p on p.subject_id = la.subject_id
--     inner join icustays as i on i.stay_id = la.stay_id
--     inner join admissions as adm on adm.hadm_id = la.hadm_id
--     left join extra_vars as ev on ev.stay_id = la.stay_id;
-- \i MIMIC_preprocessing/timeseries.sql

-- -- we need to make sure that we have at least some form of time series for every patient in diagnoses, flat and labels
-- drop materialized view if exists ld_timeseries_patients cascade;
-- create materialized view ld_timeseries_patients as
--   with repeats as (
--     select distinct patientunitstayid
--       from ld_timeserieslab
--     union
--     select distinct patientunitstayid
--       from ld_timeseries)
--   select distinct patientunitstayid
--     from repeats;

-- -- renaming some of the variables so that they are equivalent to those in eICU
-- \copy (select subject_id as uniquepid, hadm_id as patienthealthsystemstayid, stay_id as patientunitstayid, hospital_expire_flag as actualhospitalmortality, los as actualiculos from ld_labels as l where l.stay_id in (select * from ld_timeseries_patients) order by l.stay_id) to '/Users/emmarocheteau/PycharmProjects/TPC-LoS-prediction/MIMIC_data/labels.csv' with csv header
-- \copy (select * from ld_flat as f where f.patientunitstayid in (select * from ld_timeseries_patients) order by f.patientunitstayid) to '/Users/emmarocheteau/PycharmProjects/TPC-LoS-prediction/MIMIC_data/flat_features.csv' with csv header
-- \copy (select * from ld_timeserieslab as tl order by tl.patientunitstayid, tl.labresultoffset) to '/Users/emmarocheteau/PycharmProjects/TPC-LoS-prediction/MIMIC_data/timeserieslab.csv' with csv header
-- \copy (select * from ld_timeseries as t order by t.patientunitstayid, t.chartoffset) to '/Users/emmarocheteau/PycharmProjects/TPC-LoS-prediction/MIMIC_data/timeseries.csv' with csv header
