-- Portfolio Project : Layoff Data Cleaning and Analysis

-- Step 1 : Initial Data Selection.
-- Selecting all records from the layoffs table for data review.

SELECT * 
FROM layoffs;

-- Step 2 : Remove Duplicates
-- Creating a staging table (Layoffs_staging) without duplicates records.
-- This step is to ensure the integrity of the data.

-- Inserting distinct records from the layoffs table into the staging table to remove duplicates.

--SELECT *
--INTO layoffs_staging
--FROM layoffs
--WHERE 1 = 0;  

--Removing duplicate rows.


--INSERT INTO layoffs_staging
--SELECT distinct * 
--FROM layoffs;

-- Step 3 : Looking for duplicate values
-- Using ROW_NUMBER() to find and list duplicate entries based on key columns.

WITH duplicates_cte AS(
	SELECT *,
		ROW_NUMBER()OVER(
			PARTITION BY company,[location],industry,total_laid_off,percentage_laid_off,[date],stage,country,funds_raised_millions 
			ORDER BY company
		) row_num
	FROM layoffs_staging
)
SELECT * 
FROM duplicates_cte
WHERE row_num > 1;


-- Step 4 : Standardizing data.
-- Trimming spaces from company names to maintain consistency.


SELECT company,TRIM(company) 
FROM layoffs_staging;


UPDATE layoffs_staging
set company = TRIM(company);


-- Step 5: Standardizing Industry Data
-- Correcting variations in idustry names (Observed Some inconsistencies like "Crypto" , "Crypto Currency")


SELECT * 
FROM layoffs_staging
WHERE industry like 'Crypto%';


UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry like 'Crypto%';


SELECT DISTINCT industry 
FROM layoffs_staging
ORDER BY industry;


-- Step 6: Standardizing Country Data
-- Removing unwanted characters from country names.

SELECT DISTINCT country , 
TRIM(TRAILING '.' FROM country)
FROM layoffs_staging
ORDER BY 1;


UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


--Step 7: Handling NULL and invalid date values

SELECT *, 
    CASE 
	    WHEN [date] = 'NULL' THEN null 
		ELSE [date]
		END
FROM layoffs_staging;


UPDATE layoffs_staging
SET [date] = 
    CASE 
		WHEN [date] = 'NULL' 
		THEN null 
		ELSE [date] 
	END;


--Converting date column to proper date format(It was nvarchar in dataset)

UPDATE layoffs_staging
SET [date] = CONVERT(date,[date],101);


ALTER TABLE layoffs_staging
ALTER COLUMN [date] date;


--Step 8: Handling NULL Values in industry Column
--Replacing empty or 'NULL' strings with NULL in the industry column.

SELECT *
FROM layoffs_staging
WHERE industry = ''
OR industry = 'NULL';


UPDATE layoffs_staging
SET industry = CASE 
                   WHEN industry = '' OR industry = 'NULL' THEN null 
				   ELSE industry END
WHERE industry = '' OR industry = 'NULL';


-- Step 9: Imputing Industry data using a common value.
-- Using LEAD function to fill missing industry based on the company.

WITH CTE AS (
    SELECT company,
           location,
           ISNULL(industry, LEAD(industry) OVER (PARTITION BY company ORDER BY company)) AS corrected_industry
    FROM layoffs_staging
    WHERE company IN (
        SELECT company 
        FROM layoffs_staging
        WHERE industry IS NULL
    )
)
UPDATE layoffs_staging 
SET industry = CTE.corrected_industry
FROM CTE
WHERE layoffs_staging.company = CTE.company 
  AND layoffs_staging.location = CTE.location;


-- Step 11: Replacing 'NULL' with actual NULL values for future analysis.


UPDATE layoffs_staging
SET total_laid_off =
    CASE
	    WHEN total_laid_off = 'NULL' THEN null 
		ELSE total_laid_off 
	END;


UPDATE layoffs_staging
SET percentage_laid_off = 
	CASE 
		WHEN percentage_laid_off = 'NULL' THEN null 
		ELSE percentage_laid_off 
	END


UPDATE layoffs_staging
SET funds_raised_millions = 
	CASE 
	WHEN funds_raised_millions = 'NULL' THEN null 
	ELSE funds_raised_millions 
END;


-- Step 11: Adjusting Column Data Types
-- Changing the data type of numeric columns to the appropriate types.

ALTER TABLE layoffs_staging
ALTER COLUMN  total_laid_off INT;


ALTER TABLE layoffs_staging
ALTER COLUMN percentage_laid_off FLOAT;


ALTER TABLE layoffs_staging
ALTER COLUMN  funds_raised_millions FLOAT;


-- Step 12: Removing Rows with missing Data
-- Deleting rows where both total_laid_off and percentage_laid_off are NULL as they are incomplete.

SELECT * FROM layoffs_staging
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


DELETE 
FROM layoffs_staging
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


-- Step 13: Final data check
-- varifying the cleaned by selecting all records from the table.

SELECT * 
FROM layoffs_staging;


-- Exploratory Data Analysis

-- Step 1 : Maximum Layoffs
-- Finding the Maximum total laid off and percentage laid off across all records.

SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging;


-- Step 2: High Layofff Percentage.
-- Listing companies with a 100% layoff rate,sorted by funds raised.


SELECT * 
FROM layoffs_staging
WHERE percentage_laid_off = 1 
ORDER BY funds_raised_millions DESC;


-- Step 3: Total Layoffs by company
-- Summing up total layoffs by compnay and sorting the results to show the compnaies with most layoffs.

SELECT company, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;


-- Step 4: Getting the range of dates in the dataset.

SELECT MIN(date), MAX(date)
FROM layoffs_staging;


-- Step 5: Layoffs by Year.
-- Aggregating total layoffs by year to understand trends over time.

SELECT year([date]), SUM(total_laid_off)
FROM layoffs_staging
GROUP BY year([date])
ORDER BY 1 DESC;


-- Step 6: Layoffs by company stage
-- Summing layoffs by the companies stage to understand which stage is most effected.


SELECT stage, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;

--Step 7 : Finding the rate of layyoffs over the months

SELECT SUBSTRING(cast(date as varchar),1,7) AS 'Month' , SUM(total_laid_off)
FROM layoffs_staging
WHERE SUBSTRING(cast(date as varchar),1,7) is not null
GROUP BY SUBSTRING(cast(date as varchar),1,7)
ORDER BY 1;


--Step 8 : Finding the rolling sum of layyoffs over the months


WITH Rolling_Total AS(
SELECT SUBSTRING(cast(date as varchar),1,7) AS 'Month' , SUM(total_laid_off) AS sum_over_Month
FROM layoffs_staging
where SUBSTRING(cast(date as varchar),1,7) is not null
GROUP BY SUBSTRING(cast(date as varchar),1,7)
)
SELECT [MONTH],sum_over_Month, SUM(sum_over_Month)OVER(ORDER BY [Month]) AS rolling_total
FROM Rolling_Total;


-- Step 9 : Yearly rate of layingoffs as per companies in every year

WITH Company_Year AS
(
SELECT company,YEAR([date])'year', SUM(total_laid_off) AS 'laid_off'
FROM layoffs_staging
GROUP BY company, YEAR([date])
),
Company_Year_Rank AS
(
SELECT company,[year],laid_off,DENSE_RANK()OVER(PARTITION BY [year] ORDER BY laid_off DESC) AS Ranking
FROM Company_Year
WHERE [year] is not null
)
SELECT * FROM Company_Year_Rank
WHERE Ranking <= 5
ORDER BY [year];

-- Step 10 : top 10 industries with highest layoffs per year

SELECT 
    industry,
    YEAR(date) AS date,
    SUM(total_laid_off) AS total_laid_off
FROM 
    layoffs_staging
WHERE 
    industry IN (
        -- Subquery to get top 10 industries by layoffs
        SELECT 
            industry
        FROM (
            SELECT 
                industry,
                ROW_NUMBER() OVER (ORDER BY SUM(total_laid_off) DESC) AS ranking_per_layoffs
            FROM 
                layoffs_staging
            GROUP BY 
                industry
        ) AS ranking
        WHERE ranking_per_layoffs <= 10 )
GROUP BY 
    industry,
    YEAR(date);










