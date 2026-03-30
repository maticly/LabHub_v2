USE CS779_LabHub_final;
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

CREATE TABLE inventory.InventoryItem
(
    InventoryItemID BIGINT IDENTITY PRIMARY KEY,
    ProductID BIGINT,
    LocationID BIGINT,
    PurchaseID BIGINT,
    Quantity DECIMAL(12,2),
    ExpirationDate DATE,
    AddedAt DATETIME2 DEFAULT SYSDATETIME(),
    LotNumber VARCHAR(64),
    IsHazardous BIT
);

CREATE TABLE inventory.Chemical
(
    InventoryItemID BIGINT PRIMARY KEY,
    RegulationStatus VARCHAR(32),
    SerialNumber VARCHAR(64)
);

CREATE TABLE inventory.EquipmentItem
(
    InventoryItemID BIGINT PRIMARY KEY,
    SerialNumber VARCHAR(64)
);

CREATE TABLE inventory.Consumable
(
    InventoryItemID BIGINT PRIMARY KEY,
    SerialNumber VARCHAR(64)
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
    EventType VARCHAR(32),
    EventReasonID BIGINT,
    EventDescription VARCHAR(512),
    EventDate DATETIME2 DEFAULT SYSDATETIME()
);

SELECT * FROM inventory.StockEvent

COMMIT;
GO