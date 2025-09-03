MODEL (
  name dar.uss.northwind__customer_lifetime_value,
  kind FULL
  --kind INCREMENTAL_BY_TIME_RANGE(
  --  time_column _record__updated_at
  --)
);

WITH cte__order_value AS (
  SELECT
    _bridge__northwind__order_details._pit_hook__customer__id,
    _bridge__northwind__order_details._pit_hook__order__id,
    order_date__northwind__orders,
    SUM(
      unit_price__northwind__order_details * quantity__northwind__order_details * (
        1 - COALESCE(discount__northwind__order_details, discount__v_double__northwind__order_details)
      )
    ) AS revenue,
    _bridge__northwind__order_details._record__updated_at,
    _bridge__northwind__order_details._record__valid_from,
    _bridge__northwind__order_details._record__valid_to,
    _bridge__northwind__order_details._record__is_current
  FROM dar.uss__staging._bridge__northwind__order_details
  LEFT JOIN dar.uss.northwind__order_details
    ON _bridge__northwind__order_details._pit_hook__order__product = northwind__order_details._pit_hook__order__product
  LEFT JOIN dar.uss.northwind__orders
    ON _bridge__northwind__order_details._pit_hook__order__id = northwind__orders._pit_hook__order__id
  GROUP BY
    _bridge__northwind__order_details._pit_hook__customer__id,
    _bridge__northwind__order_details._pit_hook__order__id,
    order_date__northwind__orders,
    _bridge__northwind__order_details._record__updated_at,
    _bridge__northwind__order_details._record__valid_from,
    _bridge__northwind__order_details._record__valid_to,
    _bridge__northwind__order_details._record__is_current
), cte__per_customer AS (
  SELECT
    _pit_hook__customer__id,
    MIN(order_date__northwind__orders::DATE) AS first_order_date,
    MAX(order_date__northwind__orders::DATE) AS last_order_date,
    COUNT(DISTINCT DATEADD(month, DATEDIFF(month, 0, order_date__northwind__orders::DATE), 0)) AS active_months,
    MAX(revenue) AS historical_revenue,
    _record__updated_at,
    _record__valid_from,
    _record__valid_to,
    _record__is_current
  FROM cte__order_value
  GROUP BY
    _pit_hook__customer__id,
    _record__updated_at,
    _record__valid_from,
    _record__valid_to,
    _record__is_current
)
SELECT
  _pit_hook__customer__id,
  first_order_date,
  active_months,
  historical_revenue,
  CASE
    WHEN active_months >= 6
    THEN historical_revenue * 2.5
    WHEN active_months >= 3
    THEN historical_revenue * 2.0
    ELSE historical_revenue * 1.5
  END AS lifetime_value,
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__is_current
FROM cte__per_customer
--WHERE 1 = 1 AND _record__updated_at::TIMESTAMP BETWEEN @start_ts AND @end_ts