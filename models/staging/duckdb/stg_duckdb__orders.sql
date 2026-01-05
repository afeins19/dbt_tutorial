-- stg_duckdb__customers.sql

WITH orders AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
),

customers AS (
    SELECT 
        id AS customer_id
        
        -- split first and last name
        ,[name] AS customer_full_name
        --,split_part([name], ' ', 1) AS customer_first_name
        --,split_part([name], ' ', 2) AS customer_last_name 
    FROM {{ source('raw', 'raw_customers') }}
),

stores as (
    SELECT 
        id AS store_id 
        ,[name] AS store_name
        ,opened_at AS store_opened_at
        ,tax_rate AS store_tax_rate
    FROM {{ source('raw', 'raw_stores') }}

)


SELECT 
    o.id AS order_id    
    ,s.*
    ,c.*

    -- time intelligence on ordered at date
    ,EXTRACT(WEEKDAY FROM o.ordered_at) AS ordered_at_weekday
    ,EXTRACT(MONTH FROM o.ordered_at) AS ordered_at_month
    ,EXTRACT(YEAR FROM o.ordered_at) AS ordered_at_year

FROM orders o 
INNER JOIN stores s 
ON s.store_id = o.store_id

LEFT OUTER JOIN customers c
ON o.customer = c.customer_id 
