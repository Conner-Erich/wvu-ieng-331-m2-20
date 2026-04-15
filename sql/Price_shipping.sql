-- This CTE Finds the price to ship the prodcut based on it dimensions to find what prodcuts are most ecinomical to ship.
with product_shipping_values as (
select
  (p.product_length_cm * p.product_height_cm * p.product_width_cm) as product_volume,
  product_volume / oi.freight_value as price_per_cm3,
  p.product_weight_g / product_volume as price_per_density,
  op.payment_value,
  oi.freight_value,
  oi.price
from
  products as p
  join order_items as oi on oi.product_id = p.product_id
  join order_payments as op on op.order_id = oi.order_id
  )
  select *
  from product_shipping_values