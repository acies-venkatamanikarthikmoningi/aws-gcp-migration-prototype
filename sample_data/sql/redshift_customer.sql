-- Customer warehouse objects (Amazon Redshift)
CREATE TABLE IF NOT EXISTS analytics.customer (
    customer_id     BIGINT IDENTITY(1,1) ENCODE az64,
    customer_name   VARCHAR(256) ENCODE lzo,
    country         CHAR(2),
    revenue         DECIMAL(18,2),
    created_at      TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (customer_id)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (country, created_at);

CREATE TABLE analytics.orders (
    order_id     BIGINT NOT NULL,
    customer_id  BIGINT NOT NULL,
    order_date   DATE,
    status       VARCHAR(20),
    amount       NUMERIC(18,2),
    updated_at   TIMESTAMP
)
DISTKEY (customer_id)
SORTKEY (order_date);

SELECT TOP 10
       c.customer_id,
       NVL(c.customer_name, 'UNKNOWN') AS customer_name,
       DATEDIFF(day, c.created_at, GETDATE()) AS tenure_days,
       DATEADD(month, -3, GETDATE()) AS window_start,
       LISTAGG(o.status, ',') WITHIN GROUP (ORDER BY o.order_date) AS status_path,
       SUM(o.amount)::DECIMAL(18,2) AS total_amount
FROM analytics.customer c
JOIN analytics.orders o ON o.customer_id = c.customer_id
WHERE c.country ILIKE 'u%'
  AND o.order_date >= DATEADD(month, -3, GETDATE())
GROUP BY 1, 2, 3, 4
ORDER BY total_amount DESC;

SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', updated_at) AS updated_local,
       APPROXIMATE COUNT(DISTINCT customer_id) AS approx_customers
FROM analytics.orders
GROUP BY 1;

COPY analytics.orders FROM 's3://customer-raw/orders/'
IAM_ROLE 'arn:aws:iam::111122223333:role/customer-etl-role'
FORMAT AS PARQUET;

UNLOAD ('SELECT * FROM analytics.customer')
TO 's3://customer-raw/exports/customer_'
IAM_ROLE 'arn:aws:iam::111122223333:role/customer-etl-role'
PARQUET;

VACUUM analytics.orders;
ANALYZE analytics.orders;
