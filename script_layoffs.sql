CREATE TABLE companies_layoffs.layoffs (
company TEXT,
location TEXT,
industry TEXT,
total_laid_off TEXT,
percentage_laid_off TEXT,
date TEXT,
stage TEXT,
country TEXT,
funds_raised_millions TEXT
);

SELECT * FROM companies_layoffs.layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns

CREATE TABLE companies_layoffs.layoffs_stg
(LIKE companies_layoffs.layoffs INCLUDING ALL);

SELECT * FROM companies_layoffs.layoffs_stg;

INSERT INTO companies_layoffs.layoffs_stg 
SELECT * FROM companies_layoffs.layoffs;


SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, total_laid_off, percentage_laid_off, 'date') AS rn
FROM companies_layoffs.layoffs_stg;


-- 1. Remove duplicates
WITH duplicate_cte as (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date,
stage, country, funds_raised_millions) AS rn
FROM companies_layoffs.layoffs_stg
) 
SELECT * FROM duplicate_cte WHERE rn > 1;

SELECT * FROM companies_layoffs.layoffs where company = 'Oda';

CREATE TABLE IF NOT EXISTS companies_layoffs.layoffs_stg2
(
    company text COLLATE pg_catalog."default",
    location text COLLATE pg_catalog."default",
    industry text COLLATE pg_catalog."default",
    total_laid_off text COLLATE pg_catalog."default",
    percentage_laid_off text COLLATE pg_catalog."default",
    date text COLLATE pg_catalog."default",
    stage text COLLATE pg_catalog."default",
    country text COLLATE pg_catalog."default",
    funds_raised_millions text COLLATE pg_catalog."default",
	row_num int
)

TABLESPACE pg_default;
GRANT ALL ON TABLE companies_layoffs.layoffs_stg2 TO root WITH GRANT OPTION;

INSERT INTO companies_layoffs.layoffs_stg2
SELECT *, 
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date,
stage, country, funds_raised_millions) AS row_num
FROM companies_layoffs.layoffs_stg;

SELECT * FROM companies_layoffs.layoffs_stg2 where row_num > 1;

DELETE FROM companies_layoffs.layoffs_stg2 where row_num > 1;

-- 2. Standardize the data

SELECT date, to_date(date, 'MM/DD/YYYY')
FROM companies_layoffs.layoffs_stg2
WHERE "date" NOT IN ('NULL', 'NU', 'N/A', '');

UPDATE companies_layoffs.layoffs_stg2
SET date = to_date(date, 'MM/DD/YYYY')
WHERE "date" NOT IN ('NULL', 'NU', 'N/A', '');

-- I verified here that the date is still in text format.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'layoffs_stg2';

-- but can't change the column type to date because of the 'NULL' that is in text.
SELECT *
FROM companies_layoffs.layoffs_stg2
WHERE date IN ('NULL', 'NU', 'N/A', '');

-- so i changed the 'NULL' to a real NULL value.
UPDATE companies_layoffs.layoffs_stg2
SET date = NULL
WHERE date = 'NULL';

-- and changed column type to date.
ALTER TABLE companies_layoffs.layoffs_stg2
ALTER COLUMN date TYPE date
USING date::date;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'layoffs_stg2';

-- FIXING NULL VALUES IN total_laid_off and percentage_laid_off

SELECT *
FROM companies_layoffs.layoffs_stg2
WHERE industry IS NULL
OR industry IN ('NULL','');

SELECT *
FROM companies_layoffs.layoffs_stg2
WHERE company = 'Airbnb';

SELECT tbl1.company, tbl1.industry, tbl2.company, tbl2.industry
FROM companies_layoffs.layoffs_stg2 tbl1
JOIN companies_layoffs.layoffs_stg2 tbl2
	ON tbl1.company = tbl2.company
WHERE (tbl1.industry is NULL OR tbl1.industry ='NULL')
AND tbl2.industry is not NULL;

UPDATE companies_layoffs.layoffs_stg2 AS tbl1
SET industry = tbl2.industry
FROM companies_layoffs.layoffs_stg2 AS tbl2
WHERE tbl1.company = tbl2.company
AND (tbl1.industry is NULL)
AND tbl2.industry is not NULL;

UPDATE companies_layoffs.layoffs_stg2
SET industry = NULL
WHERE industry = 'NULL';

SELECT * 
FROM companies_layoffs.layoffs_stg2
WHERE total_laid_off IN ('NULL','NU','')
AND percentage_laid_off IN ('NULL','NU','');

DELETE 
FROM companies_layoffs.layoffs_stg2
WHERE total_laid_off IN ('NULL','NU','')
AND percentage_laid_off IN ('NULL','NU','');



SELECT * 
FROM companies_layoffs.layoffs_stg2;

ALTER TABLE companies_layoffs.layoffs_stg2
DROP column row_num;
