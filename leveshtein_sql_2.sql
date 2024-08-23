
---------------------   Alter table ------------------------------------------------------
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
 
-----------------------------------Normalize Country-----------------------------------------
EXPLAIN ANALYZE
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

---------------------------------------Normalize state/region----------------------------------------
EXPLAIN ANALYZE
WITH distinct_worldcities AS (
    SELECT DISTINCT admin_name_ascii,iso2
    FROM worldcities WHERE admin_name_ascii IS NOT NULL AND iso2 IS NOT NULL
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
    inner JOIN 
        distinct_worldcities wc
		on  c.country = wc.iso2
) 
UPDATE companies
	SET region =  closest_state_match.normalized_state 
FROM closest_state_match
WHERE 
    companies.id = closest_state_match.id
    AND closest_state_match.rn = 1
    AND closest_state_match.distance <= 2;

------------------------------------------Normalize City----------------------------------------------------
EXPLAIN ANALYZE
WITH distinct_worldcities AS (
    SELECT DISTINCT city_ascii,admin_name_ascii
    FROM worldcities
	
),
closest_city_match AS (
    SELECT 
        c.id,
        c.city_raw,
        wc.city_ascii AS normalized_city,
        levenshtein(c.city_raw, wc.city_ascii) AS distance,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY levenshtein(c.city_raw, wc.city_ascii)) AS rn
    FROM 
        companies c
    inner JOIN 
        distinct_worldcities wc
		on  c.region = wc.admin_name_ascii
) 
UPDATE companies
SET city = closest_city_match.normalized_city
FROM closest_city_match
WHERE 
    companies.id = closest_city_match.id
    AND closest_city_match.rn = 1
    AND closest_city_match.distance <= 2;

---------------------analysis on Normalization ----------------------------------
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

----------city-----------------------
SELECT 
    city_raw AS before_city,
    city AS after_city,
    levenshtein(city_raw, city) AS levenshtein_distance
FROM 
    companies
WHERE 
    levenshtein(city_raw, city) BETWEEN 1 AND 3  -- Filter to only distances of 1 or 2
ORDER BY 
    levenshtein_distance ASC;

SELECT 
    city_raw AS before_city,
    city AS after_city,
	levenshtein(city_raw, city) AS levenshtein_distance
FROM 
    companies
WHERE 
    city_raw != city  -- Compare city_raw and city
ORDER BY 
    levenshtein_distance ASC;  -- Order by Levenshtein distance


SELECT 
    levenshtein(city_raw, city) AS levenshtein_distance,
    COUNT(*) AS count
FROM 
    companies
WHERE 
    levenshtein(city_raw, city) IN (1, 2)
GROUP BY 
    levenshtein_distance
ORDER BY 
    levenshtein_distance ASC;


SELECT 
    region_raw AS before_region,
    region AS after_region,
	levenshtein(region_raw, region) AS levenshtein_distance
FROM 
    companies
WHERE 
    region_raw != region  -- Compare city_raw and city
ORDER BY 
    levenshtein_distance ASC;  -- Order by Levenshtein distance
	
------------region ---------------------------------------------
SELECT 
    region_raw AS before_region,
    region AS after_region,
    levenshtein(region_raw, region) AS levenshtein_distance
FROM 
    companies
WHERE 
    levenshtein(region_raw, region) BETWEEN 1 AND 3  -- Filter to only distances of 1 or 2
ORDER BY 
    levenshtein_distance ASC;


SELECT 
    levenshtein(region_raw, region) AS levenshtein_distance,
    COUNT(*) AS count
FROM 
    companies
WHERE 
    levenshtein(region_raw, region) IN (1, 2)
GROUP BY 
    levenshtein_distance
ORDER BY 
    levenshtein_distance ASC;

------------------------------------21/08/2024------------------------------------------
select  country_raw = country from  companies   
select country_raw = country , count(*) from  companies group by country_raw = country
select count(distinct companies.country )  from companies inner join worldcities on companies.country = worldcities.iso2

select region_raw = region , count(*) from  companies group by region_raw = region 

select city_raw = city , count(*) from  companies group by city_raw = city

SELECT 
    CASE 
        WHEN city_raw = city THEN 'TRUE'
        WHEN city_raw IS NULL OR city IS NULL THEN 'NULL'
        ELSE 'FALSE'
    END AS city_status,
    COUNT(*) AS count,
    (COUNT(*) * 100.0 / 1000) AS percentage  
FROM 
    companies
GROUP BY 
    CASE 
        WHEN city_raw = city THEN 'TRUE'
        WHEN city_raw IS NULL OR city IS NULL THEN 'NULL'
        ELSE 'FALSE'
    END
ORDER BY 
    city_status;


SELECT 
    CASE 
        WHEN region_raw = region THEN 'TRUE'
        WHEN region_raw IS NULL OR region IS NULL THEN 'NULL'
        ELSE 'FALSE'
    END AS region_status,
    COUNT(*) AS count,
    (COUNT(*) * 100.0 / 1000) AS percentage  
FROM 
    companies
GROUP BY 
    CASE 
        WHEN region_raw = region THEN 'TRUE'
        WHEN region_raw IS NULL OR region IS NULL THEN 'NULL'
        ELSE 'FALSE'
    END
ORDER BY 
    region_status;

---------------------------------------------------
 
----------Table initial analysis ---------------------------------------------
select * from companies

SELECT country_raw, LENGTH(country_raw) AS length 
FROM companies
ORDER BY length DESC

SELECT DISTINCT country_raw
FROM companies;

SELECT DISTINCT iso2 AS country FROM worldcities;
SELECT DISTINCT country_raw AS country FROM companies;

SELECT DISTINCT admin_name_ascii as state FROM worldcities;
SELECT DISTINCT region as state from companies

SELECT DISTINCT city_ascii as city FROM worldcities;
SELECT DISTINCT city_raw as city_raw from companies

select city_ascii,iso2 from worldcities where city_ascii ='Quarry Bay'
select city,country from companies_bfr where city ='Quarry Bay'
select city_raw from companies where city_raw ='England'
select admin_name_ascii,iso2 from worldcities where admin_name_ascii ='England'
select admin_name_ascii,iso2 from worldcities where admin_name_ascii ='Haifa'

SELECT 
    CASE 
        WHEN c.region_raw = wc.admin_name_ascii THEN 'Match'
        ELSE 'No Match'
    END AS match_region_status,
    COUNT(*) AS count
FROM 
    companies c
LEFT JOIN 
    worldcities wc
ON 
    c.country = wc.iso2  -- Ensure the country matches
    AND LOWER(c.region_raw) = LOWER(wc.admin_name_ascii)  -- Compare region/state names
GROUP BY 
    match_region_status;


SELECT 
    CASE 
        WHEN c.city_raw = wc.city_ascii THEN 'Match'
        ELSE 'No Match'
    END AS match_city_status,
    COUNT(*) AS count
FROM 
    companies c
LEFT JOIN 
    worldcities wc
ON 
    c.region_raw = wc.admin_name_ascii  -- Ensure the country matches
    AND LOWER(c.city_raw) = LOWER(wc.city_ascii)  -- Compare region/state names
GROUP BY 
    match_city_status;

SELECT 
    CASE 
        WHEN c.city_raw = wc.city_ascii THEN 'Match'
        ELSE 'No Match'
    END AS match_city_status,
    COUNT(*) AS count
FROM 
    companies c
LEFT JOIN 
    worldcities wc
ON 
    c.city_raw = wc.city_ascii  -- Ensure the country matches
    AND LOWER(c.city_raw) = LOWER(wc.city_ascii)  -- Compare region/state names
GROUP BY 
    match_city_status;


SELECT 
    c.city_raw AS non_matching_city,
    COUNT(*) AS count
FROM 
    companies c
LEFT JOIN 
    worldcities wc
ON 
    c.region_raw = wc.admin_name_ascii  -- Ensure the region matches
    AND LOWER(c.city_raw) = LOWER(wc.city_ascii)  -- Compare city names
WHERE 
    c.city_raw IS NOT NULL 
    AND (wc.city_ascii IS NULL OR c.city_raw != wc.city_ascii)  -- Filter non-matching cities
GROUP BY 
    c.city_raw;


 SELECT 
    c.region_raw AS non_matching_region,
    COUNT(*) AS count
FROM 
    companies c
LEFT JOIN 
    worldcities wc
ON 
    c.country_raw = wc.iso2  -- Ensure the region matches
    
WHERE 
    c.region_raw IS NOT NULL 
    AND (wc.admin_name_ascii IS NULL OR c.region_raw != wc.admin_name_ascii)  -- Filter non-matching cities
GROUP BY 
    c.region_raw;

--------------------------------------------------------------------------------------
