USE LabHub_v2;
GO

INSERT INTO core.UserRole
    (UserRoleName)
VALUES
    ('Admin'),
    ('LabManager'),
    ('Researcher'),
    ('VendorRep');

INSERT INTO core.Department
    (DepartmentName)
VALUES
    ('Biology'),
    ('Chemistry'),
    ('Engineering'),
    ('Chemistry'),
    ('Physics'),
    ('Computer Science'),
    ('Mathematics'),
    ('Medicine'),
    ('Pharmacy'),
    ('Environmental Science');


INSERT INTO core.UnitOfMeasure
    (UnitName)
VALUES
    ('unit'),
    ('L'),
    ('g'),
    ('mol'),
    ('pack'),
    ('set');

INSERT INTO core.ProductCategory
    (CategoryName)
VALUES
    ('Equipment'),
    ('Chemical'),
    ('Consumable');


INSERT INTO supply.OrderStatus
    (StatusName)
VALUES
    ('Approved'),
    ('Cancelled'),
    ('Draft'),
    ('Fulfilled'),
    ('Submitted');

INSERT INTO inventory.EventReason
    (Reason)
VALUES
    ('Use'),
    ('Restock'),
    ('Adjustment'),
    ('Damage'),
    ('Transfer'),
    ('Disposal'),
    ('Audit');

INSERT INTO core.StorageConditions 
    (ConditionName, MinTemp, MaxTemp, ConditionDescription)
VALUES
    ('Ambient', 15, 25, 'Standard laboratory room temperature. Suitable for stable solids, hardware, and plastics.'),
    ('Refrigerated', 2, 8, 'Cold storage (4°C) for biological media, buffers, and reagents to prevent degradation.'),
    ('Frozen', -25, -15, 'Standard freezer storage (-20°C) for metabolic intermediates, serum, and enzymes.'),
    ('Ultra-Low Frozen', -85, -60, 'Ultra-low temperature storage (-80°C) for long-term preservation of sensitive biologicals.'),
    ('Flammable Storage', 15, 25, 'Ventilated, fire-rated safety cabinets for hazardous and volatile solvents like Ethanol and Acetonitrile.'),
    ('Desiccated', 15, 25, 'Storage in a low-humidity environment (Desiccator) to prevent moisture absorption by hygroscopic powders.');

