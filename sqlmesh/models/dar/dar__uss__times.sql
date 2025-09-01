MODEL (
  name dar.uss.times,
  enabled TRUE,
  kind VIEW
);

SELECT
  time_24h::TIME AS event_occurred_at,
  second_of_day::INT,
  time_24h::TIME,
  time_12h::TEXT,
  hour_24::INT,
  hour_12::INT,
  minute::INT,
  second::INT,
  am_pm::TEXT,
  hour_minute_24h::TEXT,
  hour_minute_12h::TEXT,
  quarter_hour::TEXT,
  half_hour::TEXT,
  time_of_day::TEXT,
  part_of_day::TEXT,
  business_hours::TEXT,
  shift::TEXT
FROM das.raw.times
;