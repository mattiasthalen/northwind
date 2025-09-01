MODEL (
  name das.scd.@source,
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _record__hash
  ),
  blueprints (
    (source := northwind__categories, @unique_key := category_id),
    (source := northwind__customers, @unique_key := customer_id),
    (source := northwind__employees, @unique_key := employee_id),
    (
      source := northwind__employee_territories,
      @unique_key := CONCAT(employee_id::TEXT, '|', territory_id::TEXT)
    ),
    (
      source := northwind__order_details,
      @unique_key := CONCAT(order_id::TEXT, '|', product_id::TEXT)
    ),
    (source := northwind__orders, @unique_key := order_id),
    (source := northwind__products, @unique_key := product_id),
    (source := northwind__regions, @unique_key := region_id),
    (source := northwind__shippers, @unique_key := shipper_id),
    (source := northwind__suppliers, @unique_key := supplier_id),
    (source := northwind__territories, @unique_key := territory_id)
  )
);

WITH cte__source AS (
  SELECT
    *,
    @unique_key AS _unique_key
  FROM das.raw.@source
), cte__changed_ids AS (
  SELECT DISTINCT
    _unique_key
  FROM cte__source
  WHERE
    _record__loaded_at BETWEEN @start_ts AND @end_ts
), cte__changed_data AS (
  SELECT
    *
  FROM cte__source
  WHERE
    EXISTS(
      SELECT
        1
      FROM cte__changed_ids
      WHERE
        cte__changed_ids._unique_key = cte__source._unique_key
    )
), cte__previous_hash AS (
  SELECT
    _record__hash
  FROM cte__changed_data
  WHERE
    _record__loaded_at < @start_ts
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _unique_key ORDER BY _record__loaded_at DESC) = 1
), cte__current_hashes AS (
  SELECT
    _record__hash
  FROM cte__changed_data
  WHERE
    _record__loaded_at BETWEEN @start_ts AND @end_ts
), cte__next_hash AS (
  SELECT
    _record__hash
  FROM cte__changed_data
  WHERE
    _record__loaded_at > @end_ts
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _unique_key ORDER BY _record__loaded_at ASC) = 1
), cte__all_hashes AS (
  SELECT
    _record__hash
  FROM cte__previous_hash
  UNION ALL
  SELECT
    _record__hash
  FROM cte__current_hashes
  UNION ALL
  SELECT
    _record__hash
  FROM cte__next_hash
), cte__final AS (
  SELECT
    *,
    COALESCE(
      LEAD(_record__loaded_at) OVER (PARTITION BY _unique_key ORDER BY _record__loaded_at),
      _record__loaded_at
    ) AS _record__updated_at,
    COALESCE(
      LAG(_record__loaded_at) OVER (PARTITION BY _unique_key ORDER BY _record__loaded_at),
      '1970-01-01 00:00:00'::TIMESTAMP
    ) AS _record__valid_from,
    COALESCE(
      LEAD(_record__loaded_at) OVER (PARTITION BY _unique_key ORDER BY _record__loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS _record__valid_to,
    ROW_NUMBER() OVER (PARTITION BY _unique_key ORDER BY _record__loaded_at DESC) AS _record__version,
    CASE
      WHEN LEAD(_record__loaded_at) OVER (PARTITION BY _unique_key ORDER BY _record__loaded_at) IS NULL
      THEN 1
      ELSE 0
    END AS _record__is_current
  FROM cte__changed_data
  WHERE
    1 = 1
    AND EXISTS(
      SELECT
        1
      FROM cte__all_hashes
      WHERE
        cte__all_hashes._record__hash = cte__changed_data._record__hash
    )
)
SELECT
  @STAR__LIST(table_name := das.raw.@source),
  _record__updated_at,
  _record__valid_from,
  _record__valid_to,
  _record__version,
  _record__is_current
FROM cte__final
WHERE
  1 = 1 AND _record__updated_at BETWEEN @start_ts AND @end_ts