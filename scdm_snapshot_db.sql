SET memory_limit = '96GB';
SET VARIABLE DPID = "MKSCNR";
SET VARIABLE DPMaxDate = DATE '2023-06-30';

-- Create a temporary table from the Parquet file
CREATE OR REPLACE TEMP TABLE tmp_enr_d AS
SELECT * FROM '/apps/socprojects/dmqa/etl25-parquet/enrollment-snappy.parquet'
WHERE lower(drugcov) = 'y';

CREATE OR REPLACE TEMP TABLE tmp_enr_md AS
SELECT * FROM tmp_enr_d
WHERE lower(medcov) = 'y';

CREATE OR REPLACE TEMP TABLE tmp_enr_m AS
SELECT * FROM '/apps/socprojects/dmqa/etl25-parquet/enrollment-snappy.parquet'
WHERE lower(medcov) = 'y';

-- Bridge Enrollments: Medical Only
CREATE OR REPLACE TEMP TABLE enr_final_m AS
WITH LaggedEnroll AS (
    SELECT
        patid,
        enr_start,
        enr_end,
        LAG(enr_end) OVER (PARTITION BY patid ORDER BY enr_start) AS lag_end
    FROM tmp_enr_m
), SpanPeriod1 AS (
    SELECT *,
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY patid ORDER BY enr_start) = 1 THEN 1
            WHEN enr_start > (lag_end + 46) THEN
                SUM(CASE
                        WHEN enr_start > (lag_end + 46) THEN 1
                        ELSE 0
                    END) OVER (PARTITION BY patid ORDER BY enr_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) + 1
            ELSE NULL
        END AS span_period
    FROM LaggedEnroll
), SpanPeriod2 AS (
    SELECT *,
        FIRST_VALUE(span_period) OVER (PARTITION BY patid ORDER BY enr_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_span_period
    FROM SpanPeriod1
), SpanPeriod3 AS (
    SELECT
        patid,
        enr_start,
        enr_end,
        lag_end,
        COALESCE(span_period, first_span_period) AS span_period
    FROM SpanPeriod2
)
SELECT
    patid,
    MIN(enr_start) AS _enr_start,
    MAX(enr_end) AS _enr_end,
    span_period
FROM SpanPeriod3
GROUP BY patid, span_period
ORDER BY patid, _enr_start, span_period;

CREATE OR REPLACE TEMP TABLE enr_patid_m AS
SELECT DISTINCT patid FROM enr_final_m;

-- Bridge Enrollments: Medical and Drug
CREATE OR REPLACE TEMP TABLE enr_final_md AS
WITH LaggedEnroll AS (
    SELECT
        patid,
        enr_start,
        enr_end,
        LAG(enr_end) OVER (PARTITION BY patid ORDER BY enr_start) AS lag_end
    FROM tmp_enr_md
), SpanPeriod1 AS (
    SELECT *,
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY patid ORDER BY enr_start) = 1 THEN 1
            WHEN enr_start > (lag_end + 46) THEN
                SUM(CASE
                        WHEN enr_start > (lag_end + 46) THEN 1
                        ELSE 0
                    END) OVER (PARTITION BY patid ORDER BY enr_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) + 1
            ELSE NULL
        END AS span_period
    FROM LaggedEnroll
), SpanPeriod2 AS (
    SELECT *,
        FIRST_VALUE(span_period) OVER (PARTITION BY patid ORDER BY enr_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_span_period
    FROM SpanPeriod1
), SpanPeriod3 AS (
    SELECT
        patid,
        enr_start,
        enr_end,
        lag_end,
        COALESCE(span_period, first_span_period) AS span_period
    FROM SpanPeriod2
)
SELECT
    patid,
    MIN(enr_start) AS _enr_start,
    MAX(enr_end) AS _enr_end,
    span_period
FROM SpanPeriod3
GROUP BY patid, span_period
ORDER BY patid, _enr_start, span_period;

CREATE OR REPLACE TEMP TABLE enr_patid_md AS
SELECT DISTINCT patid FROM enr_final_md;

-- Bridge Enrollments: Drug Only
CREATE OR REPLACE TEMP TABLE enr_final_d AS
WITH LaggedEnroll AS (
    SELECT
        patid,
        enr_start,
        enr_end,
        LAG(enr_end) OVER (PARTITION BY patid ORDER BY enr_start) AS lag_end
    FROM tmp_enr_d
), SpanPeriod1 AS (
    SELECT *,
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY patid ORDER BY enr_start) = 1 THEN 1
            WHEN enr_start > (lag_end + 46) THEN
                SUM(CASE
                        WHEN enr_start > (lag_end + 46) THEN 1
                        ELSE 0
                    END) OVER (PARTITION BY patid ORDER BY enr_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) + 1
            ELSE NULL
        END AS span_period
    FROM LaggedEnroll
), SpanPeriod2 AS (
    SELECT *,
        FIRST_VALUE(span_period) OVER (PARTITION BY patid ORDER BY enr_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_span_period
    FROM SpanPeriod1
), SpanPeriod3 AS (
    SELECT
        patid,
        enr_start,
        enr_end,
        lag_end,
        COALESCE(span_period, first_span_period) AS span_period
    FROM SpanPeriod2
)
SELECT
    patid,
    MIN(enr_start) AS _enr_start,
    MAX(enr_end) AS _enr_end,
    span_period
FROM SpanPeriod3
GROUP BY patid, span_period
ORDER BY patid, _enr_start, span_period;

CREATE OR REPLACE TEMP TABLE enr_patid_d AS
SELECT DISTINCT patid FROM enr_final_d;

-- Calculate length of enrollment per PatID
CREATE OR REPLACE TEMP TABLE LengthOfEnrollment AS
  SELECT *
    , DATEDIFF('day', _enr_start, _enr_end) AS LOE
  FROM enr_final_md;

CREATE OR REPLACE TEMP TABLE enr_pat_covlength_md AS
WITH TotalLengthOfEnrollment AS (
  SELECT patid
    , SUM(LOE) AS TLOE
  FROM LengthOfEnrollment
  GROUP BY patid
) SELECT getvariable('DPID') as DP
        , TLOE
        , COUNT(*) as count
  FROM TotalLengthOfEnrollment
  GROUP BY DP, TLOE
  ORDER BY DP, TLOE;

COPY enr_pat_covlength_md TO 'enr_pat_covlength_md.parquet' (FORMAT 'parquet');

-- Number of PatIDs with at least one day of coverage
CREATE OR REPLACE TEMP TABLE enr_patid_ct_md as
SELECT getvariable('DPID') as DP
    , COUNT(*) as count
  FROM enr_patid_md;

COPY enr_patid_ct_md TO 'enr_patid_ct_md.parquet' (FORMAT 'parquet');

-- Number of PatIDs with at Least 1 day of coverage by Year
CREATE OR REPLACE TEMP TABLE enr_pat_covyears_md AS
WITH EnrollmentYears AS (
  SELECT *
       , EXTRACT(YEAR FROM _enr_start) AS t_start
       , EXTRACT(YEAR FROM _enr_end) AS t_end
  FROM LengthOfEnrollment
), CoverageYears AS (
  SELECT PatID
        , GENERATE_SERIES(t_start, t_end, 1) AS Year
  FROM EnrollmentYears
), CoverageByYear AS (
  SELECT PatID
      ,  unnest(Year) as Year
  FROM CoverageYears
) SELECT getvariable('DPID') as DP
        , Year
        , COUNT(*) as count
  FROM CoverageByYear
  GROUP BY DP, Year
  ORDER BY DP, Year;

COPY enr_pat_covyears_md TO 'enr_pat_covyears_md.parquet' (FORMAT 'parquet');

-- Enrollments Count
CREATE OR REPLACE TEMP TABLE enr_pat_enrcount_md AS
WITH _enr_startend_md AS (
  SELECT DISTINCT patid
     , _enr_start
     , _enr_end
  FROM enr_final_md
), _enr_pat_start_md AS (
  SELECT patid
     , _enr_start
     , count(*) as _count
  FROM _enr_startend_md
  GROUP BY patid, _enr_start
), enr_pat_count_md AS (
  SELECT patid
     , sum(_count) as enr_count
  FROM _enr_pat_start_md
  GROUP BY patid
)
SELECT getvariable('DPID') as DP
    , enr_count
    , count(*) as count
  FROM enr_pat_count_md
  GROUP BY enr_count
  ORDER BY enr_count;

COPY enr_pat_enrcount_md TO 'enr_pat_enrcount_md.parquet' (FORMAT 'parquet');

-- enr_active_patid_ct_md dataset
CREATE OR REPLACE TEMP TABLE enr_active_patid_ct_md AS
WITH ActivePatids AS (
  SELECT last_value(patid) OVER (PARTITION BY patid ORDER BY span_period) AS patid
  FROM LengthOfEnrollment where _enr_end >= getvariable('DPMaxDate')
) SELECT getvariable('DPID') as DP
   ,  getvariable('DPMaxDate') as DPMaxDate
   ,  COUNT(*) as Count
  FROM ActivePatids
  GROUP BY DP, DPMaxDate;

COPY enr_active_patid_ct_md TO 'enr_active_patid_ct_md.parquet' (FORMAT 'parquet');

-- Demographics
CREATE OR REPLACE TEMP TABLE dem_age_md AS
WITH LastEnrollment AS (
  SELECT patid
       , _enr_start
       , CASE
            WHEN last_value(patid) OVER (PARTITION BY patid ORDER BY _enr_start) IS NOT NULL THEN
              CASE
                WHEN _enr_end >= getvariable('DPMaxDate') THEN 1
                ELSE 0
              END
            ELSE NULL
          END AS _dpMaxenroll
  FROM enr_final_md
  ORDER BY PatID, _enr_start
), EnrolledDemPatIDs AS (
  SELECT a.patid,
          a._dpMaxenroll,
          a._enr_start,
          b.birth_date
  FROM LastEnrollment AS a INNER JOIN '/apps/socprojects/dmqa/etl25-parquet/demographic-snappy.parquet' AS b
  ON (a.patid = b.patid)
  WHERE b.birth_date IS NOT NULL
) SELECT *
       , (CAST(FLOOR(CAST(DATESUB('month', birth_date, _enr_start) AS INTEGER) / 12) AS INTEGER)) as age
       , CASE WHEN age IS NULL THEN 'MISSING'
           WHEN age < 0 THEN 'NEGATIVE'
           WHEN AGE BETWEEN 0 AND 1 THEN '0-1 yrs'
           WHEN AGE BETWEEN 2 AND 4 THEN '2-4 yrs'
           WHEN AGE BETWEEN 5 AND 9 THEN '5-9 yrs'
           WHEN AGE BETWEEN 10 AND 14 THEN '10-14 yrs'
           WHEN AGE BETWEEN 15 AND 19 THEN '15-19 yrs'
            WHEN AGE BETWEEN 20 AND 24 THEN '20-24 yrs'
            WHEN AGE BETWEEN 25 AND 29 THEN '25-29 yrs'
            WHEN AGE BETWEEN 30 AND 34 THEN '30-34 yrs'
            WHEN AGE BETWEEN 35 AND 39 THEN '35-39 yrs'
            WHEN AGE BETWEEN 40 AND 44 THEN '40-44 yrs'
            WHEN AGE BETWEEN 45 AND 49 THEN '45-49 yrs'
            WHEN AGE BETWEEN 50 AND 54 THEN '50-54 yrs'
            WHEN AGE BETWEEN 55 AND 59 THEN '55-59 yrs'
            WHEN AGE BETWEEN 60 AND 64 THEN '60-64 yrs'
            WHEN AGE BETWEEN 65 AND 69 THEN '65-69 yrs'
            WHEN AGE BETWEEN 70 AND 74 THEN '70-74 yrs'
            WHEN AGE BETWEEN 75 AND 79 THEN '75-79 yrs'
            WHEN AGE > 80 THEN '80+ yrs'
            ELSE NULL END AS age_category
  FROM EnrolledDemPatIDs
  ORDER BY patid;

CREATE OR REPLACE TEMP TABLE dem_pat_lstagecount_md AS
SELECT getvariable('DPID') as DP
    , age_category
    , COUNT(*) as count
  FROM dem_age_md
  GROUP BY DP, age_category
  ORDER BY DP, age_category;

COPY dem_pat_lstagecount_md TO 'dem_pat_lstagecount_md.parquet' (FORMAT 'parquet');

CREATE OR REPLACE TEMP TABLE dem_pat_actagect_md AS
SELECT getvariable('DPID') as DP
    , age_category
    , COUNT(*) as count
  FROM dem_age_md WHERE _dpMaxenroll = 1
  GROUP BY DP, age_category
  ORDER BY DP, age_category;

COPY dem_pat_actagect_md TO 'dem_pat_actagect_md.parquet' (FORMAT 'parquet');

CREATE OR REPLACE TEMP TABLE dem_catvars_md AS
WITH EnrolledDemPatids AS (
  SELECT a.patid
        , a.Sex
        , a.race
        , a.hispanic
        FROM '/apps/socprojects/dmqa/etl25-parquet/demographic-snappy.parquet' AS a
      JOIN
      enr_patid_md AS b ON a.patid = b.patid
), EnrolledDemPatIDsLong AS (
      UNPIVOT EnrolledDemPatids
        ON COLUMNS (* EXCLUDE (patid))
        INTO
          NAME variable
          VALUE value
) SELECT getvariable('DPID') as DP
        , variable
        , value
        , COUNT(*) as count
  FROM EnrolledDemPatIDsLong
  GROUP BY DP, variable, value
  ORDER BY DP, variable, value;

COPY dem_catvars_md TO 'dem_catvars_md.parquet' (FORMAT 'parquet');

-- Dispensing Table --

CREATE OR REPLACE TEMP TABLE dis_pat_rx_ct AS
  SELECT patid
       , rxdate
       , count(*) as _count
  FROM '/apps/socprojects/dmqa/etl25-parquet/dispensing-snappy.parquet'
  WHERE rxdate IS NOT NULL
  GROUP BY patid, rxdate;

CREATE OR REPLACE TEMP TABLE dis_pat_rx_md AS
WITH EnrolledPatIDs AS (
   SELECT a.*
    FROM dis_pat_rx_ct AS a, enr_final_md AS b
    WHERE (a.patid = b.patid) AND (a.rxdate BETWEEN b._enr_start AND b._enr_end)
), DispensingCount AS (
  SELECT patid
      ,  sum(_count) as rx_count
  FROM EnrolledPatIDs
  GROUP BY patid
) SELECT getvariable('DPID') as DP
       , rx_count
       , count(*) as count
  FROM DispensingCount
  GROUP BY DP, rx_count
  ORDER BY DP, rx_count;

CREATE OR REPLACE TEMP TABLE dis_pat_rx_d AS
WITH EnrolledPatIDs AS (
   SELECT a.*
    FROM dis_pat_rx_ct AS a, enr_final_d AS b
    WHERE (a.patid = b.patid) AND (a.rxdate BETWEEN b._enr_start AND b._enr_end)
), DispensingCount AS (
  SELECT patid
      ,  sum(_count) as rx_count
  FROM EnrolledPatIDs
  GROUP BY patid
) SELECT getvariable('DPID') as DP
       , rx_count
       , count(*) as count
  FROM DispensingCount
  GROUP BY DP, rx_count
  ORDER BY DP, rx_count;

COPY dis_pat_rx_md TO 'dis_pat_rx_md.parquet' (FORMAT 'parquet');
COPY dis_pat_rx_d TO 'dis_pat_rx_d.parquet' (FORMAT 'parquet');

-- Encounter Table --

CREATE OR REPLACE TEMP TABLE enc_pat_enccount_md AS
WITH EncPatIDs AS (
  SELECT patid
      ,  adate
      ,  count(*) as _count
    FROM '/apps/socprojects/dmqa/etl25-parquet/encounter-snappy.parquet'
    WHERE adate IS NOT NULL
    GROUP BY patid, adate
), EnrolledEncPatIDs AS (
  Select a.*
    FROM EncPatIDs AS a, enr_final_md AS b
    WHERE (a.patid = b.patid) AND (a.adate BETWEEN b._enr_start AND b._enr_end)
), EncounterCount AS (
  SELECT patid
      ,  sum(_count) as enc_count
    FROM EnrolledEncPatIDs
    GROUP BY patid
) SELECT getvariable('DPID') as DP
       , enc_count
       , count(*) as count
    FROM EncounterCount
    GROUP BY DP, enc_count
    ORDER BY DP, enc_count;

COPY enc_pat_enccount_md TO 'enc_pat_enccount_md.parquet' (FORMAT 'parquet');

-- Lab Results --
CREATE OR REPLACE TEMP TABLE lab_pat_testct_md AS
WITH TestDates AS (
  SELECT patid
      , COALESCE(lab_dt,
                 CAST(result_dt AS DATE),
                 CAST(order_dt AS DATE)) AS test_dt
    FROM '/apps/socprojects/dmqa/etl25-parquet/lab-snappy.parquet'
    WHERE test_dt IS NOT NULL
), LabCtByDate AS (
    SELECT patid,
           test_dt,
           count(*) as _count
      FROM TestDates
      GROUP BY patid, test_dt
), EnrolledLabPatIDs AS (
  SELECT a.*
    FROM LabCtByDate AS a, enr_final_md AS b
    WHERE (a.patid = b.patid) AND (a.test_dt BETWEEN b._enr_start AND b._enr_end)
  ), LabTestCount AS (
    SELECT patid
        ,  sum(_count) as Lab_Count
      FROM EnrolledLabPatIDs
      GROUP BY patid
) SELECT getvariable('DPID') as DP
       , Lab_Count
       , count(*) as count
    FROM LabTestCount
    GROUP BY DP, Lab_Count
    ORDER BY DP, Lab_Count;

COPY lab_pat_testct_md TO 'lab_pat_testct_md.parquet' (FORMAT 'parquet');

-- Vitals Signs --
/*
CREATE OR REPLACE TEMP TABLE vit_pat_vitct_md AS
WITH VitalsWide AS (
  SELECT patid
      ,  measure_date as measure_date
      ,  ht as HGT
      ,  wt as WGT
      ,  diastolic as DIA
      ,  systolic as SYS
   FROM '/apps/socprojects/dmqa/etl25-parquet/vitals-snappy.parquet'
), VitalsLong AS (
  UNPIVOT VitalsWide
  ON COLUMNS (* EXCLUDE (patid, measure_date))
  INTO NAME measure,
       VALUE value
), EnrolledVitPatIDs AS (
  SELECT a.*
    FROM VitalsLong AS a, enr_final_md AS b
    WHERE (a.patid = b.patid) AND (a.measure_date BETWEEN b._enr_start AND b._enr_end)
) SELECT getvariable('DPID')
        , measure AS VS_Type
        , CASE WHEN measure = 'HGT' THEN 1
               WHEN measure = 'WGT' THEN 2
               WHEN measure = 'DIA' THEN 3
               WHEN measure = 'SYS' THEN 4
               ELSE NULL END AS sortOrder
        , count(*) as count
      FROM EnrolledVitPatIDs
      GROUP BY DP, measure, sortOrder
      ORDER BY sortOrder;

ALTER TABLE vit_pat_vitct_md DROP sortOrder;
*/

-- Death Table --
CREATE OR REPLACE TEMP TABLE dth_dthct_md AS
SELECT getvariable('DPID') as DP,
       COUNT(*) as count
FROM '/apps/socprojects/dmqa/etl25-parquet/death-snappy.parquet' AS a
JOIN enr_patid_md AS b ON a.patid = b.patid
GROUP BY DP;

CREATE OR REPLACE TEMP TABLE dth_dthct_m AS
SELECT getvariable('DPID') as DP,
       COUNT(*) as count
FROM '/apps/socprojects/dmqa/etl25-parquet/death-snappy.parquet' AS a
JOIN enr_patid_m AS b ON a.patid = b.patid
GROUP BY DP;

COPY dth_dthct_md TO 'dth_dthct_md.parquet' (FORMAT 'parquet');
COPY dth_dthct_m TO 'dth_dthct_m.parquet' (FORMAT 'parquet');

-- MIL Table --
CREATE OR REPLACE TEMP TABLE mil_linkage_rates AS
WITH milLinkedCPatID AS (
  SELECT MPatID,
         EncounterID,
         EncType,
         birth_type,
         CASE
           WHEN AGE BETWEEN 10 AND 14 THEN '10-14 yrs'
           WHEN AGE BETWEEN 15 AND 19 THEN '15-19 yrs'
           WHEN AGE BETWEEN 20 AND 24 THEN '20-24 yrs'
           WHEN AGE BETWEEN 25 AND 29 THEN '25-29 yrs'
           WHEN AGE BETWEEN 30 AND 34 THEN '30-34 yrs'
           WHEN AGE BETWEEN 35 AND 39 THEN '35-39 yrs'
           WHEN AGE BETWEEN 40 AND 44 THEN '40-44 yrs'
           WHEN AGE BETWEEN 45 AND 49 THEN '45-49 yrs'
           WHEN AGE BETWEEN 50 AND 54 THEN '50-54 yrs'
          ELSE NULL
         END AS age_category,
         EXTRACT(YEAR FROM adate) AS year,
         COUNT(DISTINCT cpatid) AS InfantsLinked
    FROM '/apps/socprojects/dmqa/etl25-parquet/mil-snappy.parquet'
    WHERE mpatid IS NOT NULL
    GROUP BY MPatID, EncounterID, EncType, birth_type, age_category, year
),
Aggregated AS (
  SELECT 1 as sortOrder,
         'overall' AS variable,
         'overall' AS value,
         COUNT(*) AS deliveries,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) AS InfantsLinked,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) / COUNT(*) AS LinkageRate
  FROM milLinkedCPatID
  UNION ALL
  SELECT 2 as sortOrder,
        'age_category' AS variable,
         age_category AS value,
         COUNT(*) AS deliveries,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) AS InfantsLinked,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) / COUNT(*) AS LinkageRate
    FROM milLinkedCPatID
   GROUP BY age_category
  UNION ALL
  SELECT 3 as sortOrder,
         'EncType' AS variable,
         EncType AS value,
         COUNT(*) AS deliveries,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) AS InfantsLinked,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) / COUNT(*) AS LinkageRate
    FROM milLinkedCPatID
   GROUP BY EncType
  UNION ALL
  SELECT 4 as sortOrder,
        'year' AS variable,
         year AS value,
         COUNT(*) AS deliveries,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) AS InfantsLinked,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) / COUNT(*) AS LinkageRate
    FROM milLinkedCPatID
   GROUP BY year
  UNION ALL
  SELECT 5 as sortOrder,
        'birth_type' AS variable,
         birth_type AS value,
         COUNT(*) AS deliveries,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) AS InfantsLinked,
         SUM(CASE WHEN InfantsLinked > 0 THEN 1 ELSE 0 END) / COUNT(*) AS LinkageRate
    FROM milLinkedCPatID
   GROUP BY birth_type
)
SELECT getvariable('DPID') AS DP,
       variable AS Variable,
       value AS Value,
       FORMAT('{:,}', deliveries) AS Deliveries,
       FORMAT('{:,}', InfantsLinked) AS InfantsLinked,
       CAST(LinkageRate * 100 AS DECIMAL(5,2)) || '%' AS LinkageRate
  FROM Aggregated
 ORDER BY sortOrder, value;

COPY mil_linkage_rates TO 'mil_linkage_rates.parquet' (FORMAT 'parquet');

-- Reset memory limit --
RESET memory_limit;
