-- Daily and Monthly Sales by Store

SELECT d.Month, s.StoreKey, SUM(f.SalesAmount) AS sales 
FROM factsales f
JOIN dimstore s ON s.StoreKey = f.StoreKey
JOIN dimdate d ON d.DateKey = f.DateKey
GROUP BY d.Month, s.StoreKey;

SELECT d.Day, s.StoreKey, SUM(f.SalesAmount) AS sales 
FROM factsales f
JOIN dimstore s ON s.StoreKey = f.StoreKey
JOIN dimdate d ON d.DateKey = f.DateKey
GROUP BY d.Day, s.StoreKey;


-- Sales by Product Category
SELECT pro.Category, SUM(f.SalesAmount) AS sales
FROM factsales f
JOIN dimproduct pro ON f.ProductKey = pro.ProductKey
GROUP BY pro.Category

-- Top-Selling Products and Suppliers
-- Top Products
SELECT RANK() OVER (ORDER BY SUM(f.SalesAmount) ASC) AS product_rank, pro.ProductName, SUM(f.SalesAmount) AS sales
FROM factsales f
JOIN dimproduct pro ON f.ProductKey = pro.ProductKey
GROUP BY pro.ProductName
ORDER BY product_rank;

-- Top Suppliers
SELECT RANK() OVER (ORDER BY SUM(f.SalesAmount) DESC) AS supplier_rank, sup.SupplierName, SUM(f.SalesAmount) AS sales
FROM factsales f
JOIN dimsupplier sup ON sup.SupplierKey = f.SupplierKey
GROUP BY sup.SupplierName
ORDER BY supplier_rank;

-- Average Basket Size (number of products per Purchase)
SELECT cus.CustomerKey, AVG(f.Quantity) as avg_quantity
FROM factsales f
JOIN dimcustomer cus ON cus.CustomerKey = f.CustomerKey
GROUP BY cus.CustomerKey
ORDER BY cus.CustomerKey;
