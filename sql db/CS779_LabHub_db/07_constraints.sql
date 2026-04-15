USE LabHub_v2;
BEGIN TRAN;
-- Core
ALTER TABLE core.[User]
ADD CONSTRAINT fk_user_role FOREIGN KEY(UserRoleID) REFERENCES core.UserRole(UserRoleID);
ALTER TABLE core.[User]
ADD CONSTRAINT fk_user_department FOREIGN KEY(DepartmentID) REFERENCES core.Department(DepartmentID);
ALTER TABLE core.Product
ADD CONSTRAINT fk_product_category FOREIGN KEY(ProductCategoryID) REFERENCES core.ProductCategory(CategoryID);
ALTER TABLE core.Product
ADD CONSTRAINT fk_product_unit FOREIGN KEY(UnitID) REFERENCES core.UnitOfMeasure(UnitID);
ALTER TABLE core.Product
ADD CONSTRAINT FK_Product_StorageCondition FOREIGN KEY (StorageConditionID) REFERENCES core.StorageConditions(StorageConditionID);
ALTER TABLE core.[User]
ADD CONSTRAINT check_EmailAtomic CHECK (
        Email NOT LIKE '%,%'
        AND Email NOT LIKE '%;%'
        AND Email NOT LIKE '% %'
    ) -- Supply
ALTER TABLE supply.[Order]
ADD CONSTRAINT fk_order_user FOREIGN KEY(UserID) REFERENCES core.[User](UserID);
ALTER TABLE supply.[Order]
ADD CONSTRAINT fk_order_vendor FOREIGN KEY(VendorID) REFERENCES core.Vendor(VendorID);
ALTER TABLE supply.[Order]
ADD CONSTRAINT fk_order_supplyrequest FOREIGN KEY(SupplyRequestID) REFERENCES supply.SupplyRequest(SupplyRequestID);
ALTER TABLE supply.[Order]
ADD CONSTRAINT fk_order_status FOREIGN KEY(OrderStatusID) REFERENCES supply.OrderStatus(StatusID);
-- Inventory
ALTER TABLE inventory.InventoryItem
ADD CONSTRAINT fk_inventory_product FOREIGN KEY(ProductID) REFERENCES core.Product(ProductID);
ALTER TABLE inventory.InventoryItem
ADD CONSTRAINT fk_inventory_location FOREIGN KEY(LocationID) REFERENCES inventory.Location(LocationID);

ALTER TABLE inventory.StockEvent
ADD CONSTRAINT fk_stock_inventory FOREIGN KEY(InventoryItemID) REFERENCES inventory.InventoryItem(InventoryItemID);
ALTER TABLE inventory.StockEvent
ADD CONSTRAINT fk_stock_user FOREIGN KEY([UserID]) REFERENCES core.[User](UserID);
ALTER TABLE inventory.StockEvent
ADD CONSTRAINT fk_event_reason FOREIGN KEY(EventReasonID) REFERENCES inventory.EventReason(EventReasonID);
-- Links
ALTER TABLE link.SupplyRequestProduct
ADD CONSTRAINT fk_link_supplyrequest FOREIGN KEY(SupplyRequestID) REFERENCES supply.SupplyRequest(SupplyRequestID);
ALTER TABLE link.SupplyRequestProduct
ADD CONSTRAINT fk_link_product FOREIGN KEY(ProductID) REFERENCES core.Product(ProductID);
ALTER TABLE link.VendorProduct
ADD CONSTRAINT FK_VendorProduct_Vendor FOREIGN KEY (VendorID) REFERENCES core.Vendor(VendorID);
ALTER TABLE link.VendorProduct
ADD CONSTRAINT FK_VendorProduct_Product FOREIGN KEY (ProductID) REFERENCES core.Product(ProductID);
ALTER TABLE link.OrderHistoryProduct
ADD CONSTRAINT FK_OrderHistoryProduct_Product FOREIGN KEY (ProductID) REFERENCES core.Product(ProductID);
ALTER TABLE link.OrderHistoryInventoryItem
ADD CONSTRAINT FK_OrderHistoryInventoryItem_InventoryItem FOREIGN KEY (InventoryItemID) REFERENCES inventory.InventoryItem(InventoryItemID);
COMMIT;
GO