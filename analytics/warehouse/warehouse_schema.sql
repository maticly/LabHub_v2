--
--Main Warehouse Schema
--
-- Create schema
CREATE SCHEMA IF NOT EXISTS dw;
SET schema 'dw';

-- Sequences for auto-incrementing keys
CREATE SEQUENCE IF NOT EXISTS dw.seq_product_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_location_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_user_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_stock_event_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_status_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_storage_conditions_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_vendor_key;

-- =========================
-- Dimension: Dim_Date
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Date (
    DateKey INT PRIMARY KEY,              -- YYYYMMDD
    FullDate DATE NOT NULL,
    Day INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(32) NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL,
    DayOfWeek VARCHAR(32) NOT NULL,
    IsDayOff BIT DEFAULT 0,
    IsAfterHours BIT DEFAULT 0
);

-- =========================
-- Dimension: Dim_Product
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Product (
    ProductKey BIGINT PRIMARY KEY DEFAULT nextval('dw.seq_product_key'),
    ProductID BIGINT NOT NULL,
    ProductName VARCHAR(128) NOT NULL,
    CategoryName VARCHAR(64) NOT NULL,
    UnitOfMeasure VARCHAR(64) NOT NULL,
    Description TEXT,
    IsHazardous BIT NOT NULL,
    unit_cost DECIMAL(10,2),
    StorageConditionID BIGINT NOT NULL,
    EffectiveDate TIMESTAMP NOT NULL,
    EndDate TIMESTAMP,
    IsCurrent BIT NOT NULL
);

-- =========================
-- Dimension: Dim_Location
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Location (
    LocationKey BIGINT PRIMARY KEY DEFAULT nextval('dw.seq_location_key'),
    LocationID BIGINT NOT NULL,
    SiteName VARCHAR(256),
    Building VARCHAR(256),
    RoomNumber VARCHAR(256),
    StorageType VARCHAR(32),
    EffectiveDate TIMESTAMP NOT NULL,
    EndDate TIMESTAMP,
    IsCurrent BIT NOT NULL
);

-- =========================
-- Dimension: Dim_User
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_User (
    UserKey BIGINT PRIMARY KEY DEFAULT nextval('dw.seq_user_key'),
    UserID BIGINT NOT NULL,
    UserName VARCHAR(128) NOT NULL,
    UserRole VARCHAR(64) NOT NULL,
    DepartmentName VARCHAR(255) NOT NULL,
    EffectiveDate TIMESTAMP NOT NULL,
    EndDate TIMESTAMP,
    IsCurrent BIT NOT NULL
);

-- =========================
-- Dimension: Dim_Status
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Status (
    StatusKey BIGINT PRIMARY KEY DEFAULT nextval('dw.seq_status_key'),
    StatusID BIGINT NOT NULL,
    StatusName VARCHAR(64) NOT NULL,
    StatusCategory VARCHAR(64) NOT NULL,
    EffectiveDate TIMESTAMP NOT NULL,
    EndDate TIMESTAMP,
    IsCurrent BIT NOT NULL
);

-- =========================
-- Dimension: Dim_Stock_Event
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Stock_Event (
    StockEventKey BIGINT PRIMARY KEY DEFAULT nextval('dw.seq_stock_event_key'),
    StockEventID BIGINT NOT NULL,
    StockEventType VARCHAR(32) NOT NULL,
    StockEventReason VARCHAR(64) NOT NULL,
    EventDescription VARCHAR(512) NOT NULL,
    LastUpdated TIMESTAMP NOT NULL
);

-- =========================
-- Dimension: Dim_Storage_Conditions
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Storage_Conditions (
    StorageConditionKey BIGINT PRIMARY KEY DEFAULT nextval('dw.seq_storage_conditions_key'),
    StorageConditionID BIGINT NOT NULL,
    MaxTemp INT,
    MinTemp INT,
    ConditionDescription VARCHAR(512),
    LastUpdated TIMESTAMP NOT NULL,
    ConditionName VARCHAR(64) UNIQUE
);

-- =========================
-- Dimension: Dim_Vendor
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Vendor (
    VendorKey BIGINT PRIMARY KEY DEFAULT nextval('dw.seq_vendor_key'),
    VendorID BIGINT NOT NULL,
    VendorName VARCHAR(128) NOT NULL,
    VendorPreviousName VARCHAR(128) DEFAULT '',
    VendorStatus VARCHAR(16) NOT NULL,
    VendorPreviousStatus VARCHAR(16) NOT NULL,
    EffectiveDate TIMESTAMP NOT NULL
);

-- =========================
-- Fact: Fact_Inventory_Transactions
-- =========================
CREATE TABLE IF NOT EXISTS dw.Fact_Inventory_Transactions (
    TransactionID BIGINT PRIMARY KEY,     -- From inventory.StockEvent.StockEventID
    ProductKey BIGINT NOT NULL,
    EventDateKey INT NOT NULL,
    DeliveryDateKey INT NOT NULL,
    ExpirationDateKey INT NOT NULL,
    LocationKey BIGINT NOT NULL,
    UserKey BIGINT NOT NULL,
    LotNumber VARCHAR(64) NOT NULL,
    StockEventKey BIGINT NOT NULL,
    StorageConditionKey BIGINT NOT NULL,

    QuantityDelta DECIMAL(12,2) NOT NULL,
    AbsoluteQuantity DECIMAL(12,2) NOT NULL,
    CurrentStockSnapshot DECIMAL(12,2) NOT NULL,

    CONSTRAINT fk_fact_inventory_delivery_date
        FOREIGN KEY (DeliveryDateKey) REFERENCES dw.Dim_Date(DateKey),

    CONSTRAINT fk_fact_inventory_expiration_date
        FOREIGN KEY (ExpirationDateKey) REFERENCES dw.Dim_Date(DateKey),

    CONSTRAINT fk_fact_inventory_expiration_product
        FOREIGN KEY (ProductKey) REFERENCES dw.Dim_Product(ProductKey),

    CONSTRAINT fk_fact_inventory_expiration_location
        FOREIGN KEY (LocationKey) REFERENCES dw.Dim_Location(LocationKey),

    CONSTRAINT fk_fact_inventory_expiration_user
        FOREIGN KEY (UserKey) REFERENCES dw.Dim_User(UserKey),

    CONSTRAINT fk_fact_inventory_stock_event
        FOREIGN KEY (StockEventKey) REFERENCES dw.Dim_Stock_Event(StockEventKey),

    CONSTRAINT fk_fact_inventory_storage_conditions
        FOREIGN KEY (StorageConditionKey) REFERENCES dw.Dim_Storage_Conditions(StorageConditionKey)
);


-- =========================
-- Fact: Fact_Purchase_Orders
-- =========================
CREATE TABLE IF NOT EXISTS dw.Fact_Purchase_Orders (
    PurchaseOrderID BIGINT PRIMARY KEY,     -- From OrderHistory
    ProductKey BIGINT NOT NULL,
    OrderDateKey INT NOT NULL,
    DeliveryDateKey INT NOT NULL,
    VendorKey BIGINT,
    StatusKey BIGINT NOT NULL,
    RequestedByKey BIGINT NOT NULL,
    StorageConditionKey BIGINT NOT NULL,

    QuantityOrdered DECIMAL(12,2) NOT NULL,
    TotalCost DECIMAL(16,2) NOT NULL,
    VendorLeadTimeDays INT NOT NULL,

    CONSTRAINT fk_fact_purchase_Order_date
        FOREIGN KEY (OrderDateKey) REFERENCES dw.Dim_Date(DateKey),

    CONSTRAINT fk_fact_purchase_delivery_date
        FOREIGN KEY (DeliveryDateKey) REFERENCES dw.Dim_Date(DateKey),

    CONSTRAINT fk_fact_purchase_product
        FOREIGN KEY (ProductKey) REFERENCES dw.Dim_Product(ProductKey),

    CONSTRAINT fk_fact_purchase_vendor
        FOREIGN KEY (VendorKey) REFERENCES dw.Dim_Vendor(VendorKey),

    CONSTRAINT fk_fact_purchase_status
        FOREIGN KEY (StatusKey) REFERENCES dw.Dim_Status(StatusKey),

    CONSTRAINT fk_fact_purchase_requested_by
        FOREIGN KEY (RequestedByKey) REFERENCES dw.Dim_User(UserKey),

    CONSTRAINT fk_fact_purchase_orders
        FOREIGN KEY (StorageConditionKey) REFERENCES dw.Dim_Storage_Conditions(StorageConditionKey)
);