-- This CTE finds what products use what payment type and what customers also use it

with payment_information as (
select
payment_type,
payment_installments,
customer_id,
product_id,
case
  when payment_installments = 1 then 'onetime purchase'
  when payment_installments < 1 then 'multiple installments'
  else 'not paid for or error'
  end as 'type of installment'
from
order_payments as op
join order_items as oi on op.order_id = oi.order_id
join orders as o on oi.order_id = o.order_id
order by payment_type
)
select *
from payment_information
