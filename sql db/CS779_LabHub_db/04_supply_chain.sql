USE LabHub_v2;
GO

BEGIN TRAN;
GO
CREATE SCHEMA supply;
GO

-- Supply Requests


CREATE TABLE supply.SupplyRequest
(
    SupplyRequestID BIGINT IDENTITY PRIMARY KEY,
    NeededBy DATE,
    Status VARCHAR(16) CHECK (Status IN ('Draft','Submitted','Approved','Fulfilled','Cancelled')),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME()
);

-- Order
CREATE TABLE supply.[Order]
(
    OrderID BIGINT IDENTITY PRIMARY KEY,
    UserID BIGINT NOT NULL,
    SupplyRequestID BIGINT,
    OrderStatusID BIGINT,
    Quantity DECIMAL(12,2),
    UnitPrice DECIMAL(16,2),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    UpdatedAt DATETIME2 DEFAULT SYSDATETIME(),
    TotalAmount DECIMAL(16,2),
    ProductID BIGINT NOT NULL
);

-- OrderStatus
CREATE TABLE supply.OrderStatus
(
    StatusID BIGINT IDENTITY PRIMARY KEY,
    StatusName VARCHAR(64) NOT NULL
);


-- RFB / Bidding

CREATE TABLE supply.RequestForBid
(
    RequestForBidID BIGINT IDENTITY PRIMARY KEY,
    SupplyRequestID BIGINT,
    InitiatorID BIGINT,
    Status VARCHAR(16),
    Deadline DATETIME2
);

CREATE TABLE supply.Bid
(
    BidID BIGINT IDENTITY PRIMARY KEY,
    VendorID BIGINT,
    RequestForBidID BIGINT,
    BidPrice DECIMAL(12,2),
    BidStatus VARCHAR(16),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME()
);

CREATE INDEX idx_supplyrequest_status ON supply.SupplyRequest(Status);
CREATE INDEX idx_order_user ON supply.[Order](UserID);
CREATE INDEX idx_bid_vendor ON supply.Bid(VendorID);

COMMIT;
GO
