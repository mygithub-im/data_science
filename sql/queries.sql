-- Query that returns the top 3 spenders(users) by country: 


SELECT *
FROM (
			SELECT  *, rank() over (partition by country order by total desc) as rk
			FROM (
							SELECT country, user_id, sum(total_amount_spent) as total
							FROM userdatabase
							WHERE install_source='ua'
							GROUP BY user_id, country
						) a
		) b

WHERE rk <=3
ORDER BY country, rk



-- Query that gives the daily average revenue per game with 

-- daily average revenue = total_amount_spent / total unique players


SELECT install_date, game, (total_amount_spent/total_unique_players) as daily_average_revenue`
FROM (
			SELECT install_date, game, COUNT(DISTINCT user_id )as total_unique_players, SUM(total_amount_spent) as total_amount_spent`
			FROM gamers
			GROUP BY install_date, game
			) a


-- Get last 3 months records only


SELECT *
FROM TABLE_NAME
WHERE Date_Column >= DATEADD(MONTH, -3, GETDATE())


USE DATABASE
USE SCHEMA

-- Create an Ingestion Table for the NESTED JSON Data
				CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON 
				(
				  "RAW_NESTED_BOOK" VARIANT
				);

-- CREATE A FILE FORMAT FOR THE FILE TO BE INGESTED
				CREATE OR REPLACE FILE FORMAT LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT 
				TYPE = 'JSON' 
				COMPRESSION = 'AUTO' 
				ENABLE_OCTAL = FALSE
				ALLOW_DUPLICATE = FALSE
				STRIP_OUTER_ARRAY = TRUE
				STRIP_NULL_VALUES = FALSE
				IGNORE_UTF8_ERRORS = FALSE;

-- Load the Nested JSON File
-- Creating a stage to read file from external storage
				create stage library_card_catalog.public.like_a_window_into_an_s3_bucket
				 url = 's3://uni-lab-files';

				list @like_a_window_into_an_s3_bucket; -- list of the files in the bucket

-- Ingest the file in the created table
				copy into NESTED_INGEST_JSON
				from @like_a_window_into_an_s3_bucket
				files = ( 'json_book_author_nested.json')
				file_format = ( format_name=JSON_FILE_FORMAT);
-- QUERY
				select * from NESTED_INGEST_JSON
				SELECT RAW_NESTED_BOOK FROM NESTED_INGEST_JSON;
				SELECT RAW_NESTED_BOOK:year_published FROM NESTED_INGEST_JSON;
				SELECT RAW_NESTED_BOOK:authors[0].first_name FROM NESTED_INGEST_JSON;
				SELECT value:first_name::VARCHAR, value:last_name::VARCHAR FROM NESTED_INGEST_JSON ,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);
				SELECT RAW_STATUS:entities:hashtags[0].text FROM TWEET_INGEST WHERE RAW_STATUS:entities:hashtags[0].text is not null;