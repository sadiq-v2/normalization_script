select * from companies
select * from companies_bfr

SELECT country_raw, LENGTH(country_raw) AS length
FROM companies
ORDER BY length DESC

SELECT DISTINCT country_raw
FROM companies;


SELECT DISTINCT iso2 AS country FROM worldcities;
SELECT DISTINCT country_raw AS country FROM companies;

SELECT DISTINCT admin_name_ascii as state FROM worldcities;
SELECT DISTINCT region as state from companies

SELECT DISTINCT city_ascii as state FROM worldcities;
SELECT DISTINCT city as state from companies

---------------------alter table -------------------
ALTER TABLE companies
RENAME COLUMN country TO country_raw;
 
ALTER TABLE companies
RENAME COLUMN city TO city_raw;
 
ALTER TABLE companies
RENAME COLUMN region TO region_raw;
 
ALTER TABLE companies
ADD COLUMN country VARCHAR(50);
 
ALTER TABLE companies
ADD COLUMN city VARCHAR(255);
 
ALTER TABLE companies
ADD COLUMN region VARCHAR(255);
 
-----------------------------------Normalize Country---------------------------------------------------------
WITH distinct_worldcities AS (
    SELECT DISTINCT iso2
    FROM worldcities
),
closest_country_match AS (
    SELECT 
        c.id,
        c.country_raw,
        wc.iso2 AS normalized_country,
        levenshtein(c.country_raw, wc.iso2) AS distance,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY levenshtein(c.country_raw, wc.iso2)) AS rn
    FROM 
        companies c
    CROSS JOIN 
        distinct_worldcities wc  -- Use the distinct values subquery here
) 
UPDATE companies
SET country = closest_country_match.normalized_country
FROM closest_country_match
WHERE 
    companies.id = closest_country_match.id
    AND closest_country_match.rn = 1
    AND closest_country_match.distance <= 2;

---------------------------------------Normalized state/region--------------------------------------------------------------
WITH distinct_worldcities AS (
    SELECT DISTINCT admin_name_ascii
    FROM worldcities
),
closest_state_match AS (
    SELECT 
        c.id,
        c.region_raw,
        wc.admin_name_ascii AS normalized_state,
        levenshtein(c.region_raw, wc.admin_name_ascii) AS distance,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY levenshtein(c.region_raw, wc.admin_name_ascii)) AS rn
    FROM 
        companies c
    CROSS JOIN --do join on country to reduce comparisions
        distinct_worldcities wc
) 
UPDATE companies
SET region = closest_state_match.normalized_state
FROM closest_state_match
WHERE 
    companies.id = closest_state_match.id
    AND closest_state_match.rn = 1
    AND closest_state_match.distance <= 2;
---------------------------------------------City----------------------------------------------------
WITH closest_match AS (
    SELECT 
        c.id AS company_id,
        c.city_raw AS company_city,
        wc.city_ascii AS normalized_city,
        ROW_NUMBER() OVER (
            PARTITION BY c.id 
            ORDER BY 
                levenshtein(lower(c.city_raw), lower(wc.city_ascii))
        ) AS rn
    FROM 
        companies c
    JOIN 
        worldcities wc 
    ON 
        c.country_raw = wc.iso2 -- use state/region for this join
    -- AND
        -- LEFT(lower(c.city_raw), 2) = LEFT(lower(wc.city_ascii), 2)  -- Corrected usage of LOWER and LEFT
)
UPDATE companies
SET city = COALESCE(closest_match.normalized_city, companies.city_raw)
FROM closest_match
WHERE 
    companies.id = closest_match.company_id
    AND closest_match.rn = 1
    AND levenshtein(lower(companies.city_raw), lower(closest_match.normalized_city)) <= 2;  -- Optional: add distance filter

-------------------------------------------------------------------------------------------
SELECT
    SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS null_region,
	SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country
    -- Add more columns as needed
FROM companies;

SELECT
    SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS null_percentage_city,
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS null_percentage_region,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS null_percentage_countries
FROM 
    companies;

-------------------------------test-------------------------------
SELECT 
    c1.city as before_city,
    c2.city as after_city,
    levenshtein(c1.city, c2.city) AS levenshtein_distance
FROM 
    companies_bfr c1
inner JOIN 
    companies c2
	on c1.id = c2.id
order by levenshtein_distance ASC

SELECT 
    c1.region as before_region,
    c2.region as after_region,
    levenshtein(c1.region, c2.region) AS levenshtein_distance
FROM 
    companies_bfr c1
inner JOIN 
    companies c2
	on c1.id = c2.id
order by levenshtein_distance ASC

SELECT 
    c1.country as before_country,
    c2.country as after_country,
    levenshtein(c1.country, c2.country) AS levenshtein_distance
FROM 
    companies_bfr c1
inner JOIN 
    companies c2
	on c1.id = c2.id
order by levenshtein_distance desc


select  country_raw = country from  companies   
select country_raw = country , count(*) from  companies group by country_raw = country
select count(distinct companies.country )  from companies inner join worldcities on companies.country = worldcities.iso2
select region_raw = region , count(*) from  companies group by region_raw = region 
