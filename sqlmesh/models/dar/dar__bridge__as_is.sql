MODEL (
  name dar.uss._bridge__as_is,
  enabled TRUE,
  kind VIEW
);

SELECT
  *
FROM dar.uss._bridge__as_of
WHERE
  1 = 1 AND _record__is_current = 1