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
),

categories AS (
    SELECT *
    FROM {{ source('public', 'category') }}
),

cities AS (
    SELECT *
    FROM {{ source('public', 'city') }}
)

SELECT
    o.OrderID,
    o.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email,
    c.Phone,
    ci.CityName,
    ci.State,
    o.OrderDate,
    o.TotalAmount,
    p.ProductID,
    cat.CategoryName,
    p.ProductName,
    p.ProductDescription,
    p.ProductPrice,
    p.ProductInventory
FROM orders o
JOIN customers c ON o.CustomerID = c.CustomerID
JOIN cities ci ON c.CityID = ci.CityID
JOIN products p ON 1=1
JOIN categories cat ON p.CategoryID = cat.CategoryID
