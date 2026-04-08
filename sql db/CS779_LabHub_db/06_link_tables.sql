USE CS779_LabHub_final;
GO

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

SELECT * FROM link.VendorProduct

CREATE TABLE link.VendorProduct
(
    ID BIGINT IDENTITY PRIMARY KEY,
    VendorID BIGINT,
    ProductID BIGINT,
    ProductPrice DECIMAL(12,2),
    LeadTimeDays INT,
    UpdatedAt DATETIME2 DEFAULT SYSDATETIME()
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

COMMIT;
GO
