USE LabHub_v2;
GO

BEGIN TRAN;
GO
CREATE SCHEMA inventory;
GO

CREATE TABLE inventory.Location
(
    LocationID BIGINT IDENTITY PRIMARY KEY,
    SiteName VARCHAR(256),
    Building VARCHAR(256),
    RoomNumber VARCHAR(256),
    StorageType VARCHAR(32)
);

CREATE TABLE inventory.LocationClosure
(
    AncestorID BIGINT NOT NULL,
    DescendantID BIGINT NOT NULL,
    Depth INT,
    PRIMARY KEY (AncestorID, DescendantID)
);

SELECT DISTINCT o.ProductID
FROM supply.[Order] o
LEFT JOIN core.Product p ON o.ProductID = p.ProductID
WHERE p.ProductID IS NULL;


SELECT COUNT(*) AS MissingInventory
FROM supply.[Order] o
LEFT JOIN inventory.InventoryItem i ON o.OrderID = i.OrderID
WHERE i.OrderID IS NULL;



CREATE TABLE inventory.InventoryItem
(
    InventoryItemID BIGINT IDENTITY PRIMARY KEY,
    ProductID BIGINT,
    LocationID BIGINT,
    OrderID BIGINT,
    ExpirationDate DATE,
    AddedAt DATETIME2 DEFAULT SYSDATETIME(),
    LotNumber VARCHAR(64)
);
select * from inventory.StockEvent
CREATE TABLE inventory.Chemical
(
    InventoryItemID BIGINT PRIMARY KEY,
    RegulationStatus VARCHAR(32)
);

CREATE TABLE inventory.EquipmentItem
(
    InventoryItemID BIGINT PRIMARY KEY,
    SerialNumber VARCHAR(64)
);

CREATE TABLE inventory.Consumable
(
    InventoryItemID BIGINT PRIMARY KEY
);

CREATE TABLE inventory.EventReason
(
    EventReasonID BIGINT IDENTITY PRIMARY KEY,
    Reason VARCHAR(64) NOT NULL
    -- Example: ('Expire', 'Loan', 'Use')
);

CREATE TABLE inventory.StockEvent
(
    StockEventID BIGINT IDENTITY PRIMARY KEY,
    InventoryItemID BIGINT,
    LocationID BIGINT,
    [UserID] BIGINT,
    OldQuantity DECIMAL(12,2),
    NewQuantity DECIMAL(12,2),
    EventReasonID BIGINT,
    EventDescription TEXT,
    EventDate DATETIME2 DEFAULT SYSDATETIME()
);


COMMIT;
GO