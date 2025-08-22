MODEL (
  name @target_table,
  enabled TRUE,
  kind VIEW,
  blueprints (
    (
      source_table := dab.hook.frame__northwind__categories,
      target_table := dar.uss.northwind__categories,
      pit_hook := _pit_hook__category__id,
      suffix := '__northwind__categories'
    ),
    (
      source_table := dab.hook.frame__northwind__customers,
      target_table := dar.uss.northwind__customers,
      pit_hook := _pit_hook__customer__id,
      suffix := '__northwind__customers'
    ),
    (
      source_table := dab.hook.frame__northwind__employees,
      target_table := dar.uss.northwind__employees,
      pit_hook := _pit_hook__employee__id,
      suffix := '__northwind__employees'
    ),
    (
      source_table := dab.hook.frame__northwind__employee_territories,
      target_table := dar.uss.northwind__employee_territories,
      pit_hook := _pit_hook__employee__territory,
      suffix := '__northwind__employee_territories'
    ),
    (
      source_table := dab.hook.frame__northwind__order_details,
      target_table := dar.uss.northwind__order_details,
      pit_hook := _pit_hook__order__product,
      suffix := '__northwind__order_details'
    ),
    (
      source_table := dab.hook.frame__northwind__orders,
      target_table := dar.uss.northwind__orders,
      pit_hook := _pit_hook__order__id,
      suffix := '__northwind__orders'
    ),
    (
      source_table := dab.hook.frame__northwind__products,
      target_table := dar.uss.northwind__products,
      pit_hook := _pit_hook__product__id,
      suffix := '__northwind__products'
    ),
    (
      source_table := dab.hook.frame__northwind__regions,
      target_table := dar.uss.northwind__regions,
      pit_hook := _pit_hook__region__id,
      suffix := '__northwind__regions'
    ),
    (
      source_table := dab.hook.frame__northwind__shippers,
      target_table := dar.uss.northwind__shippers,
      pit_hook := _pit_hook__shipper__id,
      suffix := '__northwind__shippers'
    ),
    (
      source_table := dab.hook.frame__northwind__suppliers,
      target_table := dar.uss.northwind__suppliers,
      pit_hook := _pit_hook__supplier__id,
      suffix := '__northwind__suppliers'
    ),
    (
      source_table := dab.hook.frame__northwind__territories,
      target_table := dar.uss.northwind__territories,
      pit_hook := _pit_hook__territory__id,
      suffix := '__northwind__territories'
    )
  )
);

SELECT
  @pit_hook,
  @STAR(relation := @source_table, exclude := @pit_hook, suffix := @suffix)
FROM @source_table