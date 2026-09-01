CREATE TABLE IF NOT EXISTS raw_orders (
    orderID INTEGER,
    customerID TEXT,
    employeeID INTEGER,
    orderDate TIMESTAMP,
    requiredDate TIMESTAMP,
    shippedDate TIMESTAMP,
    shipVia INTEGER,
    freight NUMERIC(10,2),
    shipName TEXT,
    shipAddress TEXT,
    shipCity TEXT,
    shipRegion TEXT,
    shipPostalCode TEXT,
    shipCountry TEXT,
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_customers (
    customerID TEXT,
    companyName TEXT,
    contactName TEXT,
    contactTitle TEXT,
    address TEXT,
    city TEXT,
    region TEXT,
    postalCode TEXT,
    country TEXT,
    phone TEXT,
    fax TEXT,
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_products (
    productID INTEGER,
    productName TEXT,
    supplierID INTEGER,
    categoryID INTEGER,
    quantityPerUnit TEXT,
    unitPrice NUMERIC(10,2),
    unitsInStock INTEGER,
    unitsOnOrder INTEGER,
    reorderLevel INTEGER,
    discontinued INTEGER,
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_categories (
    categoryID INTEGER,
    categoryName TEXT,
    description TEXT,
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_order_details (
    orderID INTEGER,
    productID INTEGER,
    unitPrice NUMERIC(10,2),
    quantity INTEGER,
    discount NUMERIC(10,2),
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_orders (
    orderID INTEGER,
    customerID TEXT,
    employeeID INTEGER,
    orderDate TIMESTAMP,
    requiredDate TIMESTAMP,
    shippedDate TIMESTAMP,
    shipVia INTEGER,
    freight NUMERIC(10,2),
    shipName TEXT,
    shipAddress TEXT,
    shipCity TEXT,
    shipRegion TEXT,
    shipPostalCode TEXT,
    shipCountry TEXT,
    customer_country TEXT,
    productID INTEGER,
    productName TEXT,
    categoryID INTEGER,
    categoryName TEXT,
    quantity INTEGER,
    unitPrice NUMERIC(10,2),
    discount NUMERIC(10,2),
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agg_sales_by_country (
    shipCountry TEXT,
    total_orders INTEGER,
    total_revenue NUMERIC(18,2),
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agg_sales_by_category (
    categoryName TEXT,
    total_orders INTEGER,
    total_quantity INTEGER,
    total_revenue NUMERIC(18,2),
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit_invalid_rows (
    table_name TEXT,
    row_index INTEGER,
    reason TEXT,
    raw_row JSONB,
    batch_id TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
