-- 1 Unpaid purchases (no payment recorded)
SELECT p.PurchaseID, p.PurchaseDateTime, c.FirstName, c.LastName, s.StoreName
FROM Purchases p
JOIN Customers c ON c.CustomerID = p.CustomerID
JOIN Stores s ON s.StoreID = p.StoreID
LEFT JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
WHERE pay.PurchaseID IS NULL
ORDER BY p.PurchaseDateTime DESC;

-- 2 Customer’s last 5 purchases with item count and paid amount
SELECT c.CustomerID, c.FirstName, c.LastName, p.PurchaseID, SUM(pay.Amount), SUM(pitem.Quantity)
FROM Purchases p
JOIN Customers c ON c.CustomerID = p.CustomerID
JOIN Purchase_Items pitem ON p.PurchaseID = pitem.PurchaseID
LEFT JOIN Payments pay ON p.PurchaseID = pay.purchaseID
GROUP BY c.CustomerID, p.PurchaseID, pay.Amount
ORDER BY p.PurchaseDateTime DESC 
LIMIT 5;
-- 3 Revenue & transaction count per store in a date range
SELECT s.StoreID, SUM(pay.amount) AS total_amount, COUNT(p.PurchaseID) AS transaction_count
FROM Stores s
JOIN Purchases p ON s.StoreID = p.StoreID
JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
WHERE p.PurchaseDateTime BETWEEN '2025-08-10 09:30:00' AND '2025-09-30 09:30:00'
GROUP BY s.StoreID;

-- 4 Top 5 products by revenue in the last N days
SELECT pro.ProductID, pro.ProductName, SUM(pro.Price * pitem.Quantity) AS product_revenue
From Products pro
JOIN Purchase_Items pitem ON pitem.ProductID = pro.ProductID
JOIN Purchases p ON p.purchaseID = pitem.purchaseID
WHERE p.PurchaseDateTime >= CURRENT_TIMESTAMP - INTERVAL '5' DAY
GROUP BY pro.ProductID
ORDER BY product_revenue DESC
limit 5;

--5 Suppliers for a given product
SELECT STRING_AGG(sup.SupplierName, ', ') AS sup_names
FROM Suppliers sup
JOIN Product_Suppliers ps ON ps.SupplierID = sup.SupplierID
JOIN Products pro ON pro.ProductID = ps.ProductID
WHERE pro.ProductID = 5004;

--6 Monthly revenue by category and by store's location a. Can you add the year-over-year growth
SELECT  'Category' AS type, c.CategoryName, DATE_TRUNC('month', p.PurchaseDateTime) AS month, SUM(pro.Price*pitem.Quantity) AS category_revenue
FROM Categories c
JOIN Products pro ON pro.CategoryID = c.CategoryID
JOIN Purchase_Items pitem ON pro.ProductID = pitem.ProductID
JOIN Purchases p ON pitem.PurchaseID = p.PurchaseID
GROUP BY c.CategoryName, DATE_TRUNC('month', p.PurchaseDateTime)
UNION ALL
SELECT 'Store' AS type, s.Location, DATE_TRUNC('month', p.PurchaseDateTime) AS month, SUM(pro.Price * pitem.Quantity) AS store_revenue
FROM Stores s
JOIN Purchases p ON s.StoreID = p.StoreID
JOIN Purchase_Items pitem ON pitem.PurchaseID = p.PurchaseID
JOIN Products pro ON pro.ProductID = pitem.ProductID
GROUP BY s.Location, DATE_TRUNC('month', p.PurchaseDateTime);

--7 Monthly customer retention
WITH customer_list AS (
	SELECT CustomerID, DATE_TRUNC('month', p.PurchaseDateTime) AS month
	FROM Purchases p
	GROUP BY CustomerID, DATE_TRUNC('month', p.PurchaseDateTime)
) SELECT c.month, COUNT(DISTINCT c.CustomerID) AS retained_customers
FROM customer_list c
JOIN customer_list c1 
ON c1.CustomerID = c.CustomerID AND c1.month = c.month - INTERVAL '1 month'
GROUP BY c.month
ORDER BY c.month;

-- Task 6:
-- 1 Update a customer's email
UPDATE Customers
SET Email = $2
WHERE CustomerID = $1
AND NOT EXISTS (SELECT 1 FROM Customers WHERE Email = $2)
RETURNING CustomerID, FirstName, LastName, Email

-- 2 Move a purchase to a different store
UPDATE Purchase
SET StoreID = $2
WHERE PurchaseID = $1
RETURN PurchaseID, CustomerID, StoreID, PurchaseDateTime

--3 Change payment method and amount for a purchase (idempotent on PurchaseID)
INSERT INTO Payments (PurchaseID, PaymentMethod, Amount)
VALUES ($1, $2, $3)
ON CONFLICT (PurchaseID)
DO UPDATE SET PaymentMethod = $2, Amount = $3;

--4 Reassign a product to a category (create category if it doesn’t exist)
WITH create_cat AS (
	INSERT INTO Categories (CategoryName)
	VALUES ($1)
	ON CONFLICT (CategoryID) DO NOTHING
	RETURNING CategoryID
)
UPDATE Products
SET COALESCE CategoryID = COALESCE(
	(SELECT CategoryID FROM cat),
	(SELECT CategoryID FROM Categories WHERE CategoryName = $1)	
)
WHERE ProductID = $2
RETURNING ProductID, ProductName, CategoryID;

--5 Optimistic price change (only if current price matches expected old price)
UPDATE Products
SET Price = $new_price
WHERE ProductID = $product_id
AND Price = $expected_old_price
RETURNING ProductID, ProductName, Price;



















