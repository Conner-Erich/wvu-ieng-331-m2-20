With data_type as (
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'main'
ORDER BY ordinal_position
)
select (*)
from data_type

With orph_customer as(
SELECT
  o.order_id AS orphan_id,
  o.customer_id AS missing
FROM
  orders as o
  LEFT JOIN customers as c ON o.customer_id = c.customer_id
WHERE
  c.customer_id IS NULL
  AND o.customer_id IS NOT NULL
  )
  select (*)
  from orph_customer
  
  With orph_order_item as(
SELECT
  'order_items → orders' AS relationship,
  oi.order_item_id AS orphan_id,
  oi.order_id AS missing
FROM
  order_items as oi
  LEFT JOIN orders as o ON oi.order_id = o.order_id
  LEFT JOIN order_reviews as ore ON ore.order_id = o.order_id
  LEFT JOIN order_payments as op ON op.order_id = o.order_id
WHERE
  o.order_id IS NULL
  AND oi.order_id IS NOT NULL
  )
  select (*)
  from orph_order_item
  
  With orph_product_id as(
SELECT
  'order_items → products' AS relationship,
  oi.order_item_id AS orphan_id,
  oi.product_id AS missing
FROM
  order_items as oi
  LEFT JOIN products as p ON oi.product_id = p.product_id
WHERE
  p.product_id IS NULL
  AND oi.product_id IS NOT NULL
    )
  select (*)
  from orph_product_id
  
  With orph_seller_id as(
SELECT
  oi.order_id AS orphan_id,
  se.seller_id AS missing
FROM
  sellers as se
  LEFT JOIN order_items as oi ON se.seller_id = oi.seller_id
WHERE
  oi.seller_id IS NULL
  AND se.seller_id IS NOT NULL
      )
  select (*)
  from orph_seller_id
  
  with order_timeline as (
SELECT
  order_id AS order_id,
  order_purchase_timestamp AS purchased_date,
  order_approved_at AS approved_date,
  order_delivered_customer_date AS delivered_date,
  COUNT(DISTINCT DATE(order_purchase_timestamp)) AS distinct_days,
  SUM(
    CASE
      WHEN order_purchase_timestamp > CURRENT_TIMESTAMP THEN 1
      ELSE 0
    END
  ) AS future_dated_rows
from
  orders
GROUP BY
  order_id,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_customer_date
  )
  SELECT (*)
  FROM order_timeline
  
  WITH
  row_counts AS (
    SELECT
      'customers' AS table_name,
      COUNT(*) AS row_count
    FROM
      customers
    UNION ALL
    SELECT
      'orders',
      COUNT(*)
    FROM
      orders
    UNION ALL
    SELECT
      'order_items',
      COUNT(*)
    FROM
      order_items
    UNION ALL
    SELECT
      'products',
      COUNT(*)
    FROM
      products
  )
SELECT
  table_name,
  row_count,
FROM
  row_counts
ORDER BY
  table_name;
  
  with
  null_customers as (
    SELECT
      customer_id,
      customer_unique_id,
      customer_zip_code_prefix,
      customer_city,
      customer_state,
      SUM(
        CASE
          WHEN customer_id IS NULL THEN 1
          WHEN customer_unique_id IS NULL THEN 1
          WHEN customer_zip_code_prefix IS NULL THEN 1
          WHEN customer_city IS NULL THEN 1
          WHEN customer_state IS NULL THEN 1
          ELSE 0
        END
      ) AS null_count,
    FROM
      customers
    group by
      customer_id,
      customer_unique_id,
      customer_zip_code_prefix,
      customer_city,
      customer_state,
  )
select
  sum(null_count)
from
  null_customers
  
  with
  null_geolocation as (
    SELECT
      geolocation_zip_code_prefix,
      geolocation_lat,
      geolocation_lng,
      geolocation_city,
      geolocation_state,
      SUM(
        CASE
          WHEN geolocation_zip_code_prefix IS NULL THEN 1
          WHEN geolocation_lat IS NULL THEN 1
          WHEN geolocation_lng IS NULL THEN 1
          WHEN geolocation_city IS NULL THEN 1
          WHEN geolocation_state IS NULL THEN 1
          ELSE 0
        END
      ) AS null_count,
    FROM
      geolocation
    group by
      geolocation_zip_code_prefix,
      geolocation_lat,
      geolocation_lng,
      geolocation_city,
      geolocation_state,
  )
select
  sum(null_count)
from
  null_geolocation
  
  with
  null_order_items as (
    SELECT
      order_id,
      order_item_id,
      product_id,
      seller_id,
      shipping_limit_date,
      price,
      freight_value,
      SUM(
        CASE
          WHEN order_id IS NULL THEN 1
          WHEN order_item_id IS NULL THEN 1
          WHEN product_id IS NULL THEN 1
          WHEN seller_id IS NULL THEN 1
          WHEN shipping_limit_date IS NULL THEN 1
          WHEN price IS NULL THEN 1
          WHEN freight_value IS NULL THEN 1
          ELSE 0
        END
      ) AS null_count,
    FROM
      order_items
    group by
      order_id,
      order_item_id,
      product_id,
      seller_id,
      shipping_limit_date,
      price,
      freight_value,
  )
select
  sum(null_count)
from
  null_order_items
  
  with
  null_order_payments as (
    SELECT
      order_id,
      payment_sequential,
      payment_type,
      payment_installments,
      payment_value,
      SUM(
        CASE
          WHEN order_id IS NULL THEN 1
          WHEN payment_sequential IS NULL THEN 1
          WHEN payment_type IS NULL THEN 1
          WHEN payment_installments IS NULL THEN 1
          WHEN payment_value IS NULL THEN 1
          ELSE 0
        END
      ) AS null_count,
    FROM
      order_payments
    group by
      order_id,
      payment_sequential,
      payment_type,
      payment_installments,
      payment_value,
  )
select
  sum(null_count)
from
  null_order_payments
  
  with
  null_order_reviews as (
    SELECT
      review_id,
      order_id,
      review_score,
      review_comment_title,
      review_comment_message,
      review_creation_date,
      review_answer_timestamp,
      SUM(
        CASE
          WHEN review_id IS NULL THEN 1
          WHEN order_id IS NULL THEN 1
          WHEN review_score IS NULL THEN 1
          WHEN review_comment_title IS NULL THEN 1
          WHEN review_comment_message IS NULL THEN 1
          WHEN review_creation_date IS NULL THEN 1
          WHEN review_answer_timestamp IS NULL THEN 1
          ELSE 0
        END
      ) AS null_count,
    FROM
      order_reviews
    group by
      review_id,
      order_id,
      review_score,
      review_comment_title,
      review_comment_message,
      review_creation_date,
      review_answer_timestamp,
  )
select
  sum(null_count)
from
  null_order_reviews
  
  with
  null_orders as (
    SELECT
      customer_id,
      order_id,
      order_status,
      order_purchase_timestamp,
      order_approved_at,
      order_delivered_carrier_date,
      order_delivered_customer_date,
      order_estimated_delivery_date,
      SUM(
        CASE
          WHEN customer_id IS NULL THEN 1
          WHEN order_id IS NULL THEN 1
          WHEN order_status IS NULL THEN 1
          WHEN order_purchase_timestamp IS NULL THEN 1
          WHEN order_approved_at IS NULL THEN 1
          WHEN order_delivered_carrier_date IS NULL THEN 1
          WHEN order_delivered_customer_date IS NULL THEN 1
          WHEN order_estimated_delivery_date IS NULL THEN 1
          ELSE 0
        END
      ) AS null_count,
    FROM
      orders
    group by
      customer_id,
      order_id,
      order_status,
      order_purchase_timestamp,
      order_approved_at,
      order_delivered_carrier_date,
      order_delivered_customer_date,
      order_estimated_delivery_date,
  )
select
  sum(null_count)
from
  null_orders
  
  with
  null_products as (
    SELECT
      product_id,
      product_category_name,
      product_name_lenght,
      product_description_lenght,
      product_photos_qty,
      product_weight_g,
      product_length_cm,
      product_height_cm,
      product_width_cm,
      SUM(
        CASE
          WHEN product_id IS NULL THEN 1
          WHEN product_category_name IS NULL THEN 1
          WHEN product_name_lenght IS NULL THEN 1
          WHEN product_description_lenght IS NULL THEN 1
          WHEN product_photos_qty IS NULL THEN 1
          WHEN product_weight_g IS NULL THEN 1
          WHEN product_length_cm IS NULL THEN 1
          WHEN product_height_cm IS NULL THEN 1
          WHEN product_width_cm IS NULL THEN 1
          ELSE 0
        END
      ) AS null_count,
    FROM
      products
    group by
      product_id,
      product_category_name,
      product_name_lenght,
      product_description_lenght,
      product_photos_qty,
      product_weight_g,
      product_length_cm,
      product_height_cm,
      product_width_cm,
  )
select
  sum(null_count)
from
  null_products
  
  with
  null_sellers as (
    SELECT
      seller_id,
      seller_zip_code_prefix,
      seller_city,
      seller_state,
      SUM(
        CASE
          WHEN seller_id IS NULL THEN 1
          WHEN seller_zip_code_prefix IS NULL THEN 1
          WHEN seller_city IS NULL THEN 1
          WHEN seller_state IS NULL THEN 1
          ELSE 0
        END
      ) AS null_count,
    FROM
      sellers
    group by
      seller_id,
      seller_zip_code_prefix,
      seller_city,
      seller_state,
  )
select
  sum(null_count)
from
  null_sellers
  
  with duplicate_customers as(
SELECT
  *,
  COUNT(*) AS occurrences
FROM
  customers
GROUP BY ALL
HAVING
  COUNT(*) > 1
)
select (*)
from duplicate_customers

with duplicate_geolocations as(
SELECT
  *,
  COUNT(*) AS occurrences
FROM
  geolocation
GROUP BY ALL
HAVING
  COUNT(*) > 1
)
select (*)
from duplicate_geolocations

with duplicate_order_items as(
SELECT
  *,
  COUNT(*) AS occurrences
FROM
  order_items
GROUP BY ALL
HAVING
  COUNT(*) > 1
)
select (*)
from duplicate_order_items

with duplicate_order_payments as(
SELECT
  *,
  COUNT(*) AS occurrences
FROM
  order_payments
GROUP BY ALL
HAVING
  COUNT(*) > 1
)
select (*)
from duplicate_order_payments

with duplicate_order_reviews as(
SELECT
  *,
  COUNT(*) AS occurrences
FROM
  order_reviews
GROUP BY ALL
HAVING
  COUNT(*) > 1
)
select (*)
from duplicate_order_reviews

with duplicate_orders as(
SELECT
  *,
  COUNT(*) AS occurrences
FROM
  orders
GROUP BY ALL
HAVING
  COUNT(*) > 1
)
select (*)
from duplicate_orders

with duplicate_products as(
 SELECT
  *,
  COUNT(*) AS occurrences
FROM
  products
GROUP BY ALL
HAVING
  COUNT(*) > 1
)
select (*)
from duplicate_products

with duplicate_sellers as(
SELECT
  *,
  COUNT(*) AS occurrences
FROM
  sellers
GROUP BY ALL
HAVING
  COUNT(*) > 1
)
select (*)
from duplicate_sellers

