
orders = LOAD 'orders/part-m-00000' using PigStorage(',') AS (order_id:int, order_date: chararray, order_customer:int, order_status:chararray);
customer_orders  = GROUP orders  BY order_customer;
customer_orders_no = FOREACH customer_orders GENERATE group,COUNT($1) AS total;
customer_orders_no = ORDER customer_orders_no BY total DESC;

-- Almacenamos los resultados
STORE customer_orders_no INTO 'pig_out/' USING PigStorage (',');