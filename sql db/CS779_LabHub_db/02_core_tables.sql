USE CS779_LabHub_final;
GO

BEGIN TRANSACTION;

GO
CREATE SCHEMA core;
GO


---------------------------------------------------------
--2. USER ENTITY ---
---------------------------------------------------------
CREATE TABLE core.UserRole
(
    UserRoleID BIGINT IDENTITY PRIMARY KEY,
    UserRoleName VARCHAR(64) NOT NULL
    -- Example: ('LabManager', 'Researcher', 'Admin', 'VendorRep')
);

CREATE TABLE core.Department
(
    DepartmentID BIGINT IDENTITY PRIMARY KEY,
    DepartmentName VARCHAR(255) NOT NULL
);

CREATE TABLE core.[User]
(
    UserID BIGINT IDENTITY PRIMARY KEY,
    FirstName VARCHAR(64) NOT NULL,
    LastName VARCHAR(64) NOT NULL,
    Email VARCHAR(128) NOT NULL UNIQUE,
    UserRoleID BIGINT NOT NULL,
    UserCreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    DepartmentID BIGINT NOT NULL
);

CREATE TABLE core.LabManager
(
    LabManagerID BIGINT IDENTITY PRIMARY KEY,
    ManageField VARCHAR(64),
    UserID BIGINT NOT NULL
);

CREATE TABLE core.Researcher
(
    ResearcherID BIGINT IDENTITY PRIMARY KEY,
    ResearcherEmail VARCHAR(64),
    UserID BIGINT NOT NULL
);

CREATE TABLE core.VendorRep
(
    VendorRepID BIGINT IDENTITY PRIMARY KEY,
    VendorID BIGINT NOT NULL,
    UserID BIGINT NOT NULL,
    Company VARCHAR(64)
);

CREATE TABLE core.Admin
(
    AdminID BIGINT IDENTITY PRIMARY KEY,
    AdminField VARCHAR(64),
    UserID BIGINT NOT NULL
);

CREATE TABLE core.Vendor
(
    VendorID BIGINT IDENTITY PRIMARY KEY NOT NULL,
    VendorName VARCHAR(128) NOT NULL,
    Email VARCHAR(150) NOT NULL UNIQUE CHECK (Email NOT LIKE '%,%' AND Email NOT LIKE '%;%' AND Email NOT LIKE '% %'),
    Timestamp DATETIME2 DEFAULT SYSDATETIME(),
    VendorStatus VARCHAR(16) NOT NULL CHECK (VendorStatus IN ('Active', 'Paused', 'Inactive')),
    LeadTimeDays INT,
);


CREATE TABLE core.ProductCategory
(
    CategoryID BIGINT IDENTITY PRIMARY KEY NOT NULL,
    CategoryName VARCHAR(64) NOT NULL UNIQUE
);

CREATE TABLE core.UnitOfMeasure
(
    UnitID BIGINT IDENTITY PRIMARY KEY NOT NULL,
    UnitName VARCHAR(64) NOT NULL UNIQUE
);

CREATE TABLE core.Product
(
    ProductID BIGINT IDENTITY PRIMARY KEY NOT NULL,
    ProductName VARCHAR(128) NOT NULL,
    ProductCategoryID BIGINT NOT NULL,
    UnitID BIGINT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    LastUpdatedAt DATETIME2 DEFAULT SYSDATETIME(),
    StorageConditionID BIGINT
);

CREATE TABLE core.StorageConditions
(
    StorageConditionID BIGINT IDENTITY PRIMARY KEY,
    IsHazardous BIT,
    MaxTemp INT,
    MinTemp INT,
    ConditionDescription VARCHAR(512) NOT NULL
);

CREATE INDEX idx_user_role ON core.[User](UserRoleID);
CREATE INDEX idx_user_department ON core.[User](DepartmentID);
CREATE INDEX idx_product_category ON core.Product(ProductCategoryID);
CREATE INDEX idx_product_unit ON core.Product(UnitID);

COMMIT;
GO