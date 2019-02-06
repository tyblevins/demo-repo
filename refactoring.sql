with orders as (
    select *
    from {{ ref('stg_orders') }}
    where state != 'canceled'
      and extract(year from completed_at) < '2018'
      and email not like '%company.com'
),

order_items as (
  select * from {{ ref('stg_order_items') }}
),

order_totals as (
  select id
    , number
    , completed_at
    , completed_at::date as completed_at_date
    , sum(total) as net_rev
    , sum(item_total) as gross_rev
    , count(id) as order_count
  from orders
  group by completed_at_date
),

orders_complete as (
  select order_items.order_id
    , orders.completed_at::date as completed_at_date
    , sum(order_items.quantity) as qty
  from order_items
  left join source_data.orders
    on order_items.order_id = orders.id
  where (orders.is_cancelled_order = false
    or orders.is_pending_order != true)
  group by 1,2
),

joined as (
  select orders_complete.completed_at_date
    , orders_complete.gross_rev
    , orders_complete.net_rev
    , orders_complete.qty
    , orders_complete.order_count as orders
    , orders_complete.qty/a.distinct_orders as avg_unit_per_order
    , orders_complete.Gross_Rev/a.distinct_orders as aov_gross
    , orders_complete.Net_Rev/a.distinct_orders as aov_net
  from order_totals
  join orders_complete using (completed_at_date)
  where orders_complete.net_rev >= 150000
  order by completed_at_date desc
)

select * from joined
