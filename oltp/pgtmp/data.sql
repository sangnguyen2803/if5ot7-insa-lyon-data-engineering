BEGIN;

-- ID ranges:
-- Categories: 100s
-- Suppliers : 2000s
-- Stores    : 3000s
-- Customers : 1000s
-- Products  : 5000s
-- Purchases : 7000s
-- Payments  : 8000s

-- === Categories ===
INSERT INTO Categories (CategoryID, CategoryName) VALUES
  (101, 'Beverages'),
  (102, 'Snacks'),
  (103, 'Electronics'),
  (104, 'Household');

-- === Suppliers ===
INSERT INTO Suppliers (SupplierID, SupplierName) VALUES
  (2001, 'Acme Foods'),
  (2002, 'Fresh Farm'),
  (2003, 'TechSource');

-- === Products ===
INSERT INTO Products (ProductID, ProductName, Price, CategoryID) VALUES
  (5001, 'Coffee',     6.50, 101),
  (5002, 'Tea',        4.00, 101),
  (5003, 'Chips',      2.50, 102),
  (5004, 'Soda',       1.75, 101),
  (5005, 'Headphones',49.99, 103),
  (5006, 'Detergent',  8.99, 104);

-- === Product_Suppliers (many-to-many) ===
INSERT INTO Product_Suppliers (ProductID, SupplierID) VALUES
  (5001,2001), (5001,2002),   -- Coffee from Acme, Fresh Farm
  (5002,2002),                -- Tea from Fresh Farm
  (5003,2001),                -- Chips from Acme
  (5004,2001), (5004,2002),   -- Soda from Acme, Fresh Farm
  (5005,2003),                -- Headphones from TechSource
  (5006,2001);                -- Detergent from Acme

-- === Stores ===
INSERT INTO Stores (StoreID, StoreName, Location) VALUES
  (3001, 'Central Market', 'Paris'),
  (3002, 'Westside Outlet', 'Lyon');

-- === Customers ===
INSERT INTO Customers (CustomerID, FirstName, LastName, Email) VALUES
  (1001, 'Ava',   'Ng',     'ava@example.com'),
  (1002, 'Liam',  'Chen',   'liam@example.com'),
  (1003, 'Sofia', 'Rossi',  'sofia@example.com'),
  (1004, 'Marco', 'Dubois', 'marco@example.com');

-- === Purchases (mix of paid & unpaid; spread across dates) ===
INSERT INTO Purchases (PurchaseID, PurchaseDateTime, CustomerID, StoreID) VALUES
  (7001, '2025-09-27 10:15:00', 1001, 3001), -- paid
  (7002, '2025-09-27 18:40:00', 1002, 3002), -- UNPAID
  (7003, '2025-09-26 12:05:00', 1001, 3001), -- paid
  (7004, '2025-09-20 09:30:00', 1003, 3001), -- paid
  (7005, '2025-08-15 14:20:00', 1004, 3002), -- paid (older)
  (7006, '2025-09-29 16:50:00', 1002, 3001), -- paid
  (7007, '2025-09-30 08:10:00', 1003, 3002); -- UNPAID (today)

-- === Purchase_Items ===
-- P7001: Coffee x2, Chips x3 => 20.50
-- P7002: Headphones x1, Soda x2 => 53.49 (UNPAID)
-- P7003: Tea x1, Detergent x2 => 21.98
-- P7004: Coffee x1, Soda x4 => 13.50
-- P7005: Headphones x2 => 99.98
-- P7006: Chips x5, Detergent x1 => 21.49
-- P7007: Tea x3, Soda x1 => 13.75 (UNPAID)
INSERT INTO Purchase_Items (PurchaseID, ProductID, Quantity) VALUES
  (7001, 5001, 2), (7001, 5003, 3),
  (7002, 5005, 1), (7002, 5004, 2),
  (7003, 5002, 1), (7003, 5006, 2),
  (7004, 5001, 1), (7004, 5004, 4),
  (7005, 5005, 2),
  (7006, 5003, 5), (7006, 5006, 1),
  (7007, 5002, 3), (7007, 5004, 1);

-- === Payments (omit for unpaid purchases 7002 & 7007) ===
INSERT INTO Payments (PaymentID, PurchaseID, PaymentMethod, Amount) VALUES
  (8001, 7001, 'Card',    20.50),
  (8002, 7003, 'Cash',    21.98),
  (8003, 7004, 'Card',    13.50),
  (8004, 7005, 'Card',    99.98),
  (8005, 7006, 'Voucher', 21.49);

-- === Fix sequences to max IDs (PostgreSQL) ===
SELECT setval('categories_categoryid_seq', 104,  true);
SELECT setval('suppliers_supplierid_seq',  2003, true);
SELECT setval('products_productid_seq',    5006, true);
SELECT setval('stores_storeid_seq',        3002, true);
SELECT setval('customers_customerid_seq',  1004, true);
SELECT setval('purchases_purchaseid_seq',  7007, true);
SELECT setval('payments_paymentid_seq',    8005, true);

COMMIT;
