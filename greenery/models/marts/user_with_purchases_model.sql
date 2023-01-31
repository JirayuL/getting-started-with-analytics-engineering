with users_with_purchase AS (
  SELECT
    user_id,
    CASE
      WHEN COUNT(order_id) = 1 THEN 'one_order'
      WHEN COUNT(order_id) = 2 THEN 'two_order'
      WHEN COUNT(order_id) >= 3 THEN 'three_order'
    END AS purchase
  FROM
    {{ ref('stg_greenery__orders')}}
  GROUP BY
    user_id
)
SELECT
  purchase,
  COUNT(purchase) AS user_count
FROM
  users_with_purchase
GROUP BY
  purchase