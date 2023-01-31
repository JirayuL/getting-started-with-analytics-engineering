with order_counts_in_each_hour AS (
  SELECT
      date_trunc('hour', created_at),
      COUNT(order_id) AS order_count
    FROM
      public.orders
    GROUP BY
      1
)
SELECT
  AVG(order_count)
FROM order_counts_in_each_hour