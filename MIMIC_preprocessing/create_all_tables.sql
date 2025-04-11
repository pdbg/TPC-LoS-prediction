-- creates all the tables and produces csv files
.cd data

.open mimic-iv-0.4.db

.print "Listing of mimic-iv data files"
.system cd mimic-iv-0.4 && tree

-- core tables

.print "Creating admissions table"
create table if not exists admissions as
select * from read_csv_auto('mimic-iv-0.4/core/admissions.csv.gz', escape='"');

.print "Creating patients table"
create table if not exists patients as
select * from read_csv_auto('mimic-iv-0.4/core/patients.csv.gz', escape='"');

-- hosp tables

.print "Creating labevents table"
create table if not exists labevents as
select * from read_csv_auto('mimic-iv-0.4/hosp/labevents.csv.gz', escape='"');

.print "Creating d_labitems table"
create table if not exists d_labitems as
select * from read_csv_auto('mimic-iv-0.4/hosp/d_labitems.csv.gz', escape='"');

-- icustays tables

.print "Creating d_items table"
create table if not exists d_items as
select * from read_csv_auto('mimic-iv-0.4/icu/d_items.csv.gz', escape='"');

.print "Creating chartevents table"
create table if not exists chartevents as
select * from read_csv_auto('mimic-iv-0.4/icu/chartevents.csv.gz', escape='"');

.print "Creating icustays table"
create table if not exists icustays as
select * from read_csv_auto('mimic-iv-0.4/icu/icustays.csv.gz', escape='"');

-- labels table

.print "Creating ld_labels table"
create table if not exists ld_labels as
  select i.subject_id, i.hadm_id, i.stay_id, i.intime, i.outtime, adm.hospital_expire_flag, i.los
    from 'icustays' as i
    inner join admissions as adm
      on adm.hadm_id = i.hadm_id
    inner join patients as p
      on p.subject_id = i.subject_id
    where i.los > (5/24)  -- and exclude anyone who doesn't have at least 5 hours of data
      and (extract(year from i.intime) - p.anchor_year + p.anchor_age) > 17;  -- only include adults

-- flat  features table

.print "Creating extra_vars table"
create table if not exists extra_vars as
SELECT
  ch.stay_id,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'Admission Weight (Kg)') AS weight,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'GCS - Eye Opening') AS eyes,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'GCS - Motor Response') AS motor,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'GCS - Verbal Response') AS verbal,
  AVG(ch.valuenum) FILTER (WHERE d.label = 'Height (cm)') AS height
FROM chartevents AS ch
JOIN icustays AS i ON ch.stay_id = i.stay_id
JOIN d_items AS d ON d.itemid = ch.itemid
WHERE ch.valuenum IS NOT NULL
  AND d.label IN ('Admission Weight (Kg)', 'GCS - Eye Opening', 'GCS - Motor Response', 'GCS - Verbal Response', 'Height (cm)')
  AND ch.valuenum != 0
  AND (date_part('hour', ch.charttime) - date_part('hour', i.intime)) between -24 and 5
GROUP BY ch.stay_id;

.print "Creating ld_flat table ..."
create table if not exists ld_flat as
  select distinct i.stay_id as patientunitstayid, p.gender, (extract(year from i.intime) - p.anchor_year + p.anchor_age) as age,
    adm.ethnicity, i.first_careunit, adm.admission_location, adm.insurance, ev.height, ev.weight,
    extract(hour from i.intime) as hour, ev.eyes, ev.motor, ev.verbal
    from ld_labels as la
    inner join patients as p on p.subject_id = la.subject_id
    inner join icustays as i on i.stay_id = la.stay_id
    inner join admissions as adm on adm.hadm_id = la.hadm_id
    left join extra_vars as ev on ev.stay_id = la.stay_id;

-- \i MIMIC_preprocessing/timeseries.sql

-- extract the most common lab tests and the corresponding counts of how many patients have values for those labs
.print "Creating ld_commonlabs table..."
create table if not exists ld_commonlabs as
  -- extracting the itemids for all the labevents that occur within the time bounds for our cohort
  with labsstay as (
    select l.itemid, la.stay_id
    from labevents as l
    inner join ld_labels as la
      on la.hadm_id = l.hadm_id
    where l.valuenum is not null  -- stick to the numerical data
      -- epoch extracts the number of seconds since 1970-01-01 00:00:00-00, we want to extract measurements between
      -- admission and the end of the patients' stay
      and (date_part('epoch', l.charttime) - date_part('epoch', la.intime))/(60*60*24) between -1 and la.los),
  -- getting the average number of times each itemid appears in an icustay (filtering only those that are more than 2)
  avg_obs_per_stay as (
    select itemid, avg(count) as avg_obs
    from (select itemid, count(*) as count from labsstay group by itemid, stay_id) as obs_per_stay
    group by itemid
    having avg(count) > 3)  -- we want the features to have at least 3 values entered for the average patient
  select d.label, count(distinct labsstay.stay_id) as count, a.avg_obs
    from labsstay
    inner join d_labitems as d
      on d.itemid = labsstay.itemid
    inner join avg_obs_per_stay as a
      on a.itemid = labsstay.itemid
    group by d.label, a.avg_obs
    -- only keep data that is present at some point for at least 25% of the patients, this gives us 45 lab features
    having count(distinct labsstay.stay_id) > (select count(distinct stay_id) from ld_labels)*0.25
    order by count desc;

.print "Creating ld_timeserieslab table..."
-- get the time series features from the most common lab tests (45 of these)
create table if not exists ld_timeserieslab as
  -- we extract the number of minutes in labresultoffset because this is how the data in eICU is arranged
  select la.stay_id as patientunitstayid, floor((date_part('epoch', l.charttime) - date_part('epoch', la.intime))/60)
  as labresultoffset, d.label as labname, l.valuenum as labresult
    from labevents as l
    inner join d_labitems as d
      on d.itemid = l.itemid
    inner join ld_commonlabs as cl
      on cl.label = d.label  -- only include the common labs
    inner join ld_labels as la
      on la.hadm_id = l.hadm_id  -- only extract data for the cohort
    -- epoch extracts the number of seconds since 1970-01-01 00:00:00-00, we want to extract measurements between
    -- admission and the end of the patients' stay
    where (date_part('epoch', l.charttime) - date_part('epoch', la.intime))/(60*60*24) between -1 and la.los
      and l.valuenum is not null;  -- filter out null values

-- extract the most common chartevents and the corresponding counts of how many patients have values for those chartevents
.print "Creating ld_commonchart table..."
create table if not exists ld_commonchart as
  -- extracting the itemids for all the chartevents that occur within the time bounds for our cohort
  with chartstay as (
      select ch.itemid, la.stay_id
        from chartevents as ch
        inner join ld_labels as la
          on la.stay_id = ch.stay_id
        where ch.valuenum is not null  -- stick to the numerical data
          -- epoch extracts the number of seconds since 1970-01-01 00:00:00-00, we want to extract measurements between
          -- admission and the end of the patients' stay
          and (date_part('epoch', ch.charttime) - date_part('epoch', la.intime))/(60*60*24) between -1 and la.los),
  -- getting the average number of times each itemid appears in an icustay (filtering only those that are more than 5)
  avg_obs_per_stay as (
    select itemid, avg(count) as avg_obs
    from (select itemid, count(*) as count from chartstay group by itemid, stay_id) as obs_per_stay
    group by itemid
    having avg(count) > 5)  -- we want the features to have at least 5 values entered for the average patient
  select d.label, count(distinct chartstay.stay_id) as count, a.avg_obs
    from chartstay
    inner join d_items as d
      on d.itemid = chartstay.itemid
    inner join avg_obs_per_stay as a
      on a.itemid = chartstay.itemid
    group by d.label, a.avg_obs
    -- only keep data that is present at some point for at least 25% of the patients, this gives us 129 chartevents features
    having count(distinct chartstay.stay_id) > (select count(distinct stay_id) from ld_labels)*0.25
    order by count desc;

-- get the time series features from the most common chart features (129 of these)
.print "Creating ld_timeseries table..."
create table if not exists ld_timeseries as
  -- we extract the number of minutes in chartoffset because this is how the data in eICU is arranged
  select la.stay_id as patientunitstayid, floor((date_part('epoch', ch.charttime) - date_part('epoch', la.intime))/60)
  as chartoffset, d.label as chartvaluelabel, ch.valuenum as chartvalue
    from chartevents as ch
    inner join d_items as d
      on d.itemid = ch.itemid
    inner join ld_commonchart as cch
      on cch.label = d.label  -- only include the common chart features
    inner join ld_labels as la
      on la.stay_id = ch.stay_id  -- only extract data for the cohort
    where (date_part('epoch', ch.charttime) - date_part('epoch', la.intime))/(60*60*24) between -1 and la.los
      and ch.valuenum is not null;  -- filter out null values


-- we need to make sure that we have at least some form of time series for every patient in diagnoses, flat and labels
.print "Creating ld_timeseries_patients table..."
create table if not exists ld_timeseries_patients as
  with repeats as (
    select distinct patientunitstayid
      from ld_timeserieslab
    union
    select distinct patientunitstayid
      from ld_timeseries)
  select distinct patientunitstayid
    from repeats;

-- -- renaming some of the variables so that they are equivalent to those in eICU

.cd ..
.system rm -rf MIMIC_data && mkdir MIMIC_data

.print "Exporting data in CSV files..."
copy (select subject_id as uniquepid, hadm_id as patienthealthsystemstayid, stay_id as patientunitstayid, hospital_expire_flag as actualhospitalmortality, los as actualiculos from ld_labels as l where l.stay_id in (select * from ld_timeseries_patients) order by l.stay_id) to './MIMIC_data/labels.csv' (HEADER, DELIMITER ',');
copy (select * from ld_flat as f where f.patientunitstayid in (select * from ld_timeseries_patients) order by f.patientunitstayid) to './MIMIC_data/flat_features.csv' (HEADER, DELIMITER ',');
copy (select * from ld_timeserieslab as tl order by tl.patientunitstayid, tl.labresultoffset) to './MIMIC_data/timeserieslab.csv' (HEADER, DELIMITER ',');
copy (select * from ld_timeseries as t order by t.patientunitstayid, t.chartoffset) to './MIMIC_data/timeseries.csv' (HEADER, DELIMITER ',');
