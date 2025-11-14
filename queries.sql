SELECT
    u.full_name,
    u.email,
    o.order_id,
    o.order_date,
    p.product_name,
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    o.total_amount,
    pay.payment_method,
    pay.payment_status,
    pay.payment_date
FROM orders AS o
JOIN users AS u ON o.user_id = u.user_id
JOIN order_items AS oi ON oi.order_id = o.order_id
JOIN products AS p ON oi.product_id = p.product_id
JOIN payments AS pay ON pay.order_id = o.order_id
WHERE pay.payment_status = 'successful'
ORDER BY o.order_date DESC;

