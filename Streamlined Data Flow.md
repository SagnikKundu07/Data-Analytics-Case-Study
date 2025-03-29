# Streamlining On-Prem Data Flow into Snowflake

## 1.0 Overview

### 1.1 Business Task
The project objective is to migrate and restructure the data pipeline for the company's incident management portal to generate essential metrics for monthly reporting.

### 1.1 Business Deliverables
The key business deliverables are summarized as follows:
1. Identification of the top 5 process failures each month
2. Determination of the business area with the highest number of issues
3. Calculation of the average business time taken to resolve issues
4. Formatting of closing notes and work notes for improved readability
5. Generation of web hyperlinks to directly access specific issues

---

## 2.0 Entity Relationship Diagram

![Data Model](https://github.com/user-attachments/assets/b3b19697-2f4a-4851-8caf-1d35aa315275)

---

## 3.0 Data Exploration

### 3.1 Data Validation

#### 3.1.1 Records in the source table are stored as a VARIANT datatype
**Solution:**
- String columns:  
  `REGEXP_REPLACE("NUMBER_PK_ID", '[^ -~]+') AS NUMBER_PK_ID`
- Number columns:  
  `TRY_TO_NUMBER(REGEXP_REPLACE("STATE_VALUE", '[^ -~]+')) AS STATE_VALUE`
- Timestamp columns:  
  `TRY_TO_TIMESTAMP_NTZ(REGEXP_REPLACE("OPENED_AT", '[^ -~]+')) AS OPENED_AT`

#### 3.1.2 Timestamp columns (e.g., OPENED_AT, RESOLVED_AT, CLOSED_AT) contain NULL values in the fact table, causing datatype conversion errors.

#### 3.1.3 Timestamp values stored in the source table are in UTC format
**Solution:**
```sql
CASE WHEN OPENED_AT IS NOT NULL
     THEN CONVERT_TIMEZONE('UTC', 'America', OPENED_AT::TIMESTAMP_NTZ)
     ELSE NULL
END AS OPENED_AT;
```

#### 3.1.4 The ASSIGNED_TO_DETAILS table and CALLER_DETAILS_TABLE are consolidated into a single reference table containing all user details.

#### 3.1.5 State and Priority values are stored as numeric columns
**Solution:** Created two reference tables (`REF_STATE` and `REF_PRIORITY`) mapping numeric values to corresponding category names.

#### 3.1.6 Primary key columns in different dimension tables are long numeric columns of fixed length mapped into the fact table.
**Solution:**
```sql
SELECT DISTINCT(LENGTH(<primary_key_columns>)) FROM table_names;
```
**Result:** `64`

---

## 3.2 Joining Tables

```sql
SELECT * FROM SERVICENOW_INCIDENT_TABLE SIT
LEFT JOIN (SELECT req_columns FROM ASSIGNED_TO_DETAILS_TABLE) ADT
ON SIT.ASSIGNED_TO_FK_ID = ADT.USER_PK_ID
LEFT JOIN
.
.
.
LEFT JOIN (SELECT req_columns FROM FILE_ATTACHMENT_DETAILS_TABLE) FDT
ON SIT.NUMBER_PK_ID = FDT.ATTACHMENT_TABLE_FK_ID;
```

---

## 4.0 Extracting Key Metrics

### 4.1 Monthly Top 5 Process Failures
**4.1.1 Extract process name from Short Description:**
```sql
SPLIT_PART(
CASE 
    WHEN SHORT_DESCRIPTION ILIKE 'Process ' THEN TRIM(REGEXP_REPLACE(SHORT_DESCRIPTION, '^Process ', ' '))
    WHEN SHORT_DESCRIPTION ILIKE 'PROCESS: ' THEN TRIM(REGEXP_REPLACE(SHORT_DESCRIPTION, '^PROCESS: ', ' '))
    ELSE TRIM(SHORT_DESCRIPTION)
END, ' ',1) AS PROCESS_NAME
```

**4.1.2 Find the top 5 process failures in a month:**
```sql
WITH PROCESS_FAILURE_RANK AS (
    SELECT PROCESS_NAME, COUNT(NUMBER) AS FAILURE_COUNT, 
    ROW_NUMBER() OVER (ORDER BY COUNT(NUMBER) DESC) AS RANK
    FROM SERVICENOW_INCIDENT_DETAILS_TABLE
    GROUP BY PROCESS_NAME
    HAVING MONTH(OPENED_AT) BETWEEN <month_1> AND <month_2>
)
SELECT PROCESS_NAME, FAILURE_COUNT
FROM PROCESS_FAILURE_RANK
WHERE RANK <= 5;
```

Using Snowflake's `QUALIFY` clause, we can filter the results of window functions and optimize the query as::
```sql
SELECT PROCESS_NAME, COUNT(NUMBER) AS FAILURE_COUNT, 
ROW_NUMBER() OVER (PARTITION BY FAILURE_COUNT ORDER BY COUNT(NUMBER) DESC) AS RANK
FROM SERVICENOW_INCIDENT_DETAILS_TABLE
GROUP BY PROCESS_NAME
HAVING MONTH(OPENED_AT) BETWEEN <month_1> AND <month_2>
QUALIFY RANK <= 5;
```

### 4.2 Business Area with the Most Number of Issues
```sql
SELECT SUBJECT_AREA_NAME, COUNT(NUMBER) AS ISSUE_COUNT
FROM SERVICENOW_INCIDENT_DETAILS_TABLE
GROUP BY SUBJECT_AREA_NAME
HAVING COUNT(NUMBER) = (
    SELECT MAX(COUNT(NUMBER))
    FROM SERVICENOW_INCIDENT_DETAILS_TABLE
    GROUP BY SUBJECT_AREA_NAME
);
```

### 4.3 Average Business Time Elapsed to Resolve Issues
**4.3.1 Calculate time to resolve:**
```sql
ROUND(DATEDIFF('MINUTE', OPENED_AT, RESOLVED_AT),2) AS CALC_TIME_TO_RESOLVE
```
**4.3.2 Find the average time taken to resolve issues:**
```sql
SELECT AVG(CALC_TIME_TO_RESOLVE/(24*60)) AS AVERAGE_TIME_TO_RESOLVE
FROM SERVICENOW_INCIDENT_DETAILS_TABLE
WHERE MONTH(OPENED_AT) BETWEEN <month_1> AND <month_2>;
```

### 4.4 Concatenating Work Notes
```sql
LISTAGG(WORK_NOTE_VALUE, '\n\n') WITHIN GROUP(ORDER BY <create_timestamp_value>) AS WORK_NOTE_LIST
```

### 4.5 Web Hyperlink to Redirect to the Actual Issue
```sql
CONCAT('https://domain.com//incident.do?sys_id=', NUMBER_PK_ID) AS WEB_HYPERLINK
FROM SERVICENOW_INCIDENT_DETAILS_TABLE;
```

---

## 5.0 Conclusion
This project demonstrates a fictional data flow from an on-prem database into Snowflake by cleansing, transforming, and structuring incident management data to create efficient reporting and analysis of key business metrics.
