WITH orders AS (
    SELECT *
    FROM {{ source('public', 'order') }}
),

products AS (
    SELECT *
    FROM {{ source('public', 'product') }}
),

customers AS (
    SELECT *
    FROM {{ source('public', 'customer') }}
)

SELECT
    o.OrderID,
    o.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email,
    c.Phone,
    o.OrderDate,
    o.TotalAmount,
    p.ProductID,
    p.ProductName,
    p.ProductDescription,
    p.ProductPrice,
    p.ProductInventory
FROM orders o
JOIN customers c ON o.CustomerID = c.CustomerID
JOIN products p ON 1=1  -- This line is joining products without any specific condition
