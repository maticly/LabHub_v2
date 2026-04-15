USE LabHub_v2;
GO
SELECT name FROM sys.tables where schema_id = SCHEMA_ID('link');
BEGIN TRAN;
GO
CREATE SCHEMA link;
GO

CREATE TABLE link.UserNotification
(
    ID BIGINT IDENTITY PRIMARY KEY,
    UserID BIGINT,
    NotificationID BIGINT,
    IsRead BIT DEFAULT 0
);

CREATE TABLE link.SupplyRequestProduct
(
    ID BIGINT IDENTITY PRIMARY KEY,
    SupplyRequestID BIGINT,
    ProductID BIGINT,
    Quantity DECIMAL(8,2),
    Priority VARCHAR(16)
);    

CREATE TABLE link.OrderProduct
(
    ID BIGINT IDENTITY PRIMARY KEY,
    OrderID BIGINT,
    ProductID BIGINT
);

CREATE TABLE link.RFBProduct
(
    ID BIGINT IDENTITY PRIMARY KEY,
    RequestForBidID BIGINT,
    ProductID BIGINT
);

CREATE TABLE link.UserSupplyRequest
(
    ID BIGINT IDENTITY PRIMARY KEY,
    UserID BIGINT,
    SupplyRequestID BIGINT
);

CREATE TABLE link.OrderHistoryProduct
(
    ID BIGINT IDENTITY PRIMARY KEY,
    OrderHistoryID BIGINT,
    ProductID BIGINT
);

CREATE TABLE link.OrderHistoryInventoryItem
(
    ID BIGINT IDENTITY PRIMARY KEY,
    OrderHistoryID BIGINT,
    InventoryItemID BIGINT
);

CREATE TABLE link.VendorProductLink
(
    ID BIGINT IDENTITY PRIMARY KEY,
    VendorID BIGINT,
    ProductID BIGINT,
    ListingPrice DECIMAL(12,2)
);
COMMIT;
GO
