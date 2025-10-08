
-- Table for Product Categories
CREATE TABLE IF NOT EXISTS Categories (
    CategoryID SERIAL PRIMARY KEY,
    CategoryName VARCHAR(255) NOT NULL UNIQUE
);

-- Table for Suppliers
CREATE TABLE IF NOT EXISTS Suppliers (
    SupplierID SERIAL PRIMARY KEY,
    SupplierName VARCHAR(255) NOT NULL
);

-- Table for Products
CREATE TABLE IF NOT EXISTS Products (
    ProductID SERIAL PRIMARY KEY,
    ProductName VARCHAR(255) NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    CategoryID INT,
    CONSTRAINT fk_category
        FOREIGN KEY(CategoryID)
        REFERENCES Categories(CategoryID)
);

-- JUNCTION TABLE: Product_Suppliers
-- Resolves the many-to-many relationship between Products and Suppliers.
CREATE TABLE IF NOT EXISTS Product_Suppliers (
    ProductID INT,
    SupplierID INT,
    PRIMARY KEY (ProductID, SupplierID),
    CONSTRAINT fk_product
        FOREIGN KEY(ProductID)
        REFERENCES Products(ProductID),
    CONSTRAINT fk_supplier
        FOREIGN KEY(SupplierID)
        REFERENCES Suppliers(SupplierID)
);

-- Table for Stores
CREATE TABLE IF NOT EXISTS Stores (
    StoreID SERIAL PRIMARY KEY,
    StoreName VARCHAR(255) NOT NULL,
    Location VARCHAR(255)
);

-- Table for Customers
CREATE TABLE IF NOT EXISTS Customers (
    CustomerID SERIAL PRIMARY KEY,
    FirstName VARCHAR(255) NOT NULL,
    LastName VARCHAR(255),
    Email VARCHAR(255) UNIQUE
);

-- Table for Purchases (Transactions)
CREATE TABLE IF NOT EXISTS Purchases (
    PurchaseID SERIAL PRIMARY KEY,
    PurchaseDateTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CustomerID INT,
    StoreID INT,
    CONSTRAINT fk_customer
        FOREIGN KEY(CustomerID)
        REFERENCES Customers(CustomerID),
    CONSTRAINT fk_store
        FOREIGN KEY(StoreID)
        REFERENCES Stores(StoreID)
);

-- JUNCTION TABLE: Purchase_Items
-- Resolves the many-to-many relationship between Purchases and Products.
CREATE TABLE IF NOT EXISTS Purchase_Items (
    PurchaseID INT,
    ProductID INT,
    Quantity INT NOT NULL CHECK (Quantity > 0),
    PRIMARY KEY (PurchaseID, ProductID),
    CONSTRAINT fk_purchase
        FOREIGN KEY(PurchaseID)
        REFERENCES Purchases(PurchaseID),
    CONSTRAINT fk_product
        FOREIGN KEY(ProductID)
        REFERENCES Products(ProductID)
);

-- Table for Payments
CREATE TABLE IF NOT EXISTS Payments (
    PaymentID SERIAL PRIMARY KEY,
    PurchaseID INT NOT NULL UNIQUE,
    PaymentMethod VARCHAR(50) NOT NULL CHECK (PaymentMethod IN ('Cash', 'Card', 'Voucher')),
    Amount DECIMAL(10, 2) NOT NULL,
    CONSTRAINT fk_purchase
        FOREIGN KEY(PurchaseID)
        REFERENCES Purchases(PurchaseID)
);

-- Notification to confirm script completion
SELECT 'All tables created successfully!' as status;