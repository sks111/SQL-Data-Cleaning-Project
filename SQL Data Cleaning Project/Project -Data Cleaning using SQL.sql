-- SQL Project - Data Cleaning

USE world_layoffs;

SELECT * 
FROM world_layoffs.layoffs;

-- Create a staging table to preserve the raw data

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;



-- Step 1: Check for and remove duplicate rows

-- Identify potential duplicates using ROW_NUMBER()

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
	       ROW_NUMBER() OVER (
	           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
	       ) AS row_num
	FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Add row numbers to a new staging table for easier duplicate removal

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

-- Create a new staging table with row numbers for duplicates

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT,
    row_num INT
);

-- Populate the new staging table with data and calculated row numbers

INSERT INTO `world_layoffs`.`layoffs_staging2`
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Delete true duplicate records (keep only the first occurrence)

SET SQL_SAFE_UPDATES = 0;

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- Step 2: Standardize and clean the data

-- Replace blank industry values with NULL for consistency

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Fill in missing industry values using other rows from the same company

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Standardize industry names (e.g., unify variations of "Crypto")

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Remove trailing periods from country names (e.g., "United States.")

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Convert date column from text to proper DATE format

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter the column to use the DATE data type

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 3: Handle NULL values

-- Identify rows where both layoff metrics are missing

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Remove rows that contain no meaningful layoff data

DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Final cleanup: drop temporary row_num column

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final check: review cleaned data

SELECT * 
FROM world_layoffs.layoffs_staging2;


