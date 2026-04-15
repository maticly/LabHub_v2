USE LabHub_v2;
GO

BEGIN TRAN;
GO
CREATE SCHEMA workflow;
GO

-- Notifications

CREATE TABLE workflow.Notification
(
    NotificationID BIGINT IDENTITY PRIMARY KEY,
    EntityID BIGINT NOT NULL,
    Message VARCHAR(MAX),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    NotificationType VARCHAR(32) CHECK (NotificationType IN ('Approval','Reminder','SystemAlert','DeliveryUpdate')),
    ApprovalEntryID BIGINT NULL
);

-- Approval

CREATE TABLE workflow.ApprovalRequest
(
    ApprovalRequestID BIGINT IDENTITY PRIMARY KEY,
    OrderID BIGINT NOT NULL,
    RequestorID BIGINT NOT NULL,
    ApproverID BIGINT NOT NULL,
    ApprovalDate DATETIME2 DEFAULT SYSDATETIME()
);


CREATE TABLE workflow.ApprovalType
(
    ApprovalTypeID BIGINT IDENTITY PRIMARY KEY,
    ApprovalTypeName VARCHAR(64) UNIQUE,
    Description VARCHAR(255)
);

CREATE TABLE workflow.ApprovalEntry
(
    ApprovalEntryID BIGINT IDENTITY PRIMARY KEY,
    ApprovalRequestID BIGINT NOT NULL,
    ApprovalTypeID BIGINT NOT NULL,
    ApprovalStatus VARCHAR(16) CHECK (ApprovalStatus IN ('Pending','Approved','Rejected')),
    ApprovalDate DATETIME2 DEFAULT SYSDATETIME()
);


--Audit

CREATE TABLE workflow.AuditLog
(
    AuditLogID BIGINT IDENTITY PRIMARY KEY,
    AuditTime DATETIME2 DEFAULT SYSDATETIME(),
    UserID BIGINT NOT NULL,
    AuditStatus VARCHAR(16) CHECK (AuditStatus IN ('Success','Failure','Pending'))
);

CREATE TABLE workflow.AuditAssociation
(
    AuditAssociationID BIGINT IDENTITY PRIMARY KEY,
    AuditLogID BIGINT NOT NULL,
    AuditEntityTypeID BIGINT NOT NULL,
    AuditActionTypeID BIGINT NOT NULL,
    LinkedEntityID BIGINT NULL
);

CREATE TABLE workflow.AuditEntityType
(
    AuditEntityTypeID BIGINT IDENTITY PRIMARY KEY,
    ActionEntityTypeName VARCHAR(64) UNIQUE
);

CREATE TABLE workflow.AuditActionType
(
    AuditActionTypeID BIGINT IDENTITY PRIMARY KEY,
    ActionName VARCHAR(64)
);



-- Documents

CREATE TABLE workflow.Document
(
    DocumentID BIGINT IDENTITY PRIMARY KEY,
    FileName VARCHAR(128),
    FilePath VARCHAR(255),
    UploadedAt DATETIME2 DEFAULT SYSDATETIME()
);

CREATE TABLE workflow.DocumentEntityType
(
    DocumentEntityTypeID BIGINT IDENTITY PRIMARY KEY,
    Name VARCHAR(64) UNIQUE
);

CREATE TABLE workflow.DocumentAssociation
(
    DocumentAssociationID BIGINT IDENTITY PRIMARY KEY,
    DocumentID BIGINT NOT NULL,
    DocumentEntityTypeID BIGINT NOT NULL,
    LinkedEntityID BIGINT
);

COMMIT;
GO