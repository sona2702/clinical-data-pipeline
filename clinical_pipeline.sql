CREATE WAREHOUSE clinical_wh;
CREATE DATABASE clinical_db;
CREATE SCHEMA raw_schema;

USE WAREHOUSE clinical_wh;
USE DATABASE clinical_db;
USE SCHEMA raw_schema;

/// Raw table 

CREATE OR REPLACE TABLE raw_clinical_data(
 Patient_id STRING UNIQUE,
 Weight float,
 Height float
);


select * from  raw_clinical_data;

// stage table 

CREATE OR REPLACE TABLE STAGE_CLINICAL_DATA (
  Patient_id STRING,
  Weight FLOAT,
  Height FLOAT,
  BMI FLOAT,
  Risk_Level STRING
);


select * from stage_clinical_data;


/// Report table 

CREATE OR REPLACE TABLE REPORT_CLINICAL_DATA (
  Patient_id STRING,
  Weight FLOAT,
  Height FLOAT,
  BMI FLOAT,
  Risk_Level STRING,
  Load_Date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


select * from report_clinical_data;


/// inserting into report table without task


INSERT INTO REPORT_CLINICAL_DATA (Patient_id, Weight, Height, BMI, Risk_Level)
SELECT Patient_id, Height, Weight, BMI, Risk_Level
FROM STAGE_CLINICAL_DATA;


///  stream for raw to stage


CREATE OR REPLACE STREAM raw_clinical_stream ON TABLE raw_clinical_data;

/// task for raw to stage 

CREATE OR REPLACE TASK stage_clinical_task
WAREHOUSE = clinical_wh
SCHEDULE = '3 MINUTE'  -- runs every 5 minutes
WHEN SYSTEM$STREAM_HAS_DATA('raw_clinical_stream')
AS
INSERT INTO stage_clinical_data (Patient_id, Weight, Height, BMI, Risk_Level)
SELECT
  Patient_id,
  Weight,
  Height,
  Weight / POWER(Height / 100, 2) AS BMI,
  CASE
    WHEN Weight / POWER(Height / 100, 2) < 18.5 THEN 'Underweight'
    WHEN Weight / POWER(Height / 100, 2) < 25 THEN 'Normal'
    WHEN Weight / POWER(Height / 100, 2) < 30 THEN 'Overweight'
    ELSE 'Obese'
  END AS Risk_Level
FROM raw_clinical_stream;


ALTER TASK stage_clinical_task RESUME;


// stream for stage to report 


CREATE OR REPLACE STREAM stage_clinical_stream
ON TABLE stage_clinical_data;

// task for stage to report 

CREATE OR REPLACE TASK report_clinical_task
WAREHOUSE = clinical_wh
SCHEDULE = '3 MINUTE'  -- or any interval you prefer
WHEN SYSTEM$STREAM_HAS_DATA('stage_clinical_stream')
AS
INSERT INTO report_clinical_data (Patient_id, Weight, Height, BMI, Risk_Level)
SELECT Patient_id, Weight, Height, BMI, Risk_Level
FROM stage_clinical_stream;

ALTER TASK report_clinical_task RESUME;


// manual task trigger for testing

EXECUTE TASK stage_clinical_task;
EXECUTE TASK report_clinical_task;

//  check status and history

SELECT * FROM INFORMATION_SCHEMA.TASK_HISTORYWHERE TASK_NAME = 'report_clinical_task';
