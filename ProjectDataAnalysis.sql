-- 1)Create Schemas
IF NOT EXISTS (SELECT 1 FROM
sys.schemas WHERE name ='stg')
	EXEC('CREATE SCHEMA stg');
IF NOT EXISTS (SELECT 1 FROM
sys.schemas WHERE name ='err')
	EXEC('CREATE SCHEMA err');
GO
/* 2) STAGING TABLES: 
-- stg.orders_Raw: all NVARCHAR for quick import.
stg.orders_Typed: strongly typed & cleaned.
err.Rows: captured rows that failed typing.
*/
IF OBJECT_ID('stg.orders_Raw') IS NOT NULL
	DROP TABLE stg.orders_Raw;
CREATE TABLE stg.orders_Raw(
	RawID  NVARCHAR(MAX),
	OrderID NVARCHAR(MAX),
	OrderDate NVARCHAR(MAX),
	ShipDate NVARCHAR(MAX),
	ShipMode NVARCHAR(MAX),
	CustomerID NVARCHAR(MAX),
	CustomerName NVARCHAR(MAX),
	Segment NVARCHAR(MAX),
	Country NVARCHAR(MAX),
	City    NVARCHAR(MAX),
	StateName NVARCHAR(MAX),
	PostalCode NVARCHAR(MAX),
	Region NVARCHAR(MAX),
	ProductID NVARCHAR(MAX),
	Category NVARCHAR(MAX),
	SubCategory NVARCHAR(MAX),
	ProductName NVARCHAR(MAX),
	Sales NVARCHAR(MAX),
	Quantity NVARCHAR(MAX),
	Discount NVARCHAR(MAX),
	Profit NVARCHAR(MAX)
	);

IF OBJECT_ID('stg.orders_Typed') IS NOT NULL
	DROP TABLE stg.orders_Typed;
CREATE TABLE stg.orders_Typed(
	OrderID  VARCHAR(20) Not NULL,
	OrderDate DATE NOT NULL,
	ShipDate DATE NOT NULL,
	ShipMode VARCHAR(20) NOT NULL,
	CustomerID VARCHAR(20) Not NULL,
	CustomerName VARCHAR(100) Not NULL,
	Segment VARCHAR(50)  NULL,
	Country VARCHAR(50)  NULL,
	City    VARCHAR(50)  NULL,
	StateName VARCHAR(50)  NULL,
	PostalCode VARCHAR(50)  NULL,
	Region VARCHAR(50)  NULL,
	ProductID VARCHAR(20) NOT NULL,
	Category VARCHAR(50)  NULL,
	SubCategory VARCHAR(50)  NULL,
	ProductName VARCHAR(200) NOT NULL,
	Sales DECIMAL (10,2) NOT NULL,
	Quantity INT  NOT NULL,
	Discount DECIMAL (5,2) NOT NULL,
	Profit DECIMAL (10,2) NOT NULL
	CONSTRAINT UQ_stg_orders_Typed UNIQUE (OrderID, ProductID)
	);

IF OBJECT_ID('err.ErrorRows') IS NOT NULL
	DROP TABLE err.ErrorRows;
CREATE TABLE err.ErrorRows(
	ErrorAt   DATETIME2  DEFAULT SYSDATETIME(),
	ErrorStage    VARCHAR(50), -- RAW_TO_TYPED / DIM_MERGE / FACT_LOAD
	Reason VARCHAR(4000),
	Source_orderID NVARCHAR(50) NULL,
	Source_productID NVARCHAR(50) NULL,
	RAWROW   NVARCHAR(MAX) NULL
	);
GO
 --3)
 -- check if file exist
 EXEC xp_fileexist 'C:\SuperStore\Sample - Superstore.csv'
 --import file
BULK INSERT stg.orders_Raw
FROM 'C:\SuperStore\Sample - Superstore.csv'
WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		ROWTERMINATOR='\n',
		TABLOCK
	);
GO

--4)Cleans & types data from raw ==> typed
CREATE OR ALTER FUNCTION 
dbo.fn_TryParseDate( @s NVARCHAR(MAX))
RETURNS DATE
AS
BEGIN
	DECLARE @d DATE;
	--ISO (YYYY-MM-DD)
	SET @d = TRY_CONVERT(DATE,@S,23);
	IF @d IS NOT NULL RETURN @d;

	--MDY (US) 101
	SET @d = TRY_CONVERT(DATE,@s,101);
	IF @d IS NOT NULL RETURN @d;

	--DMY (UK) 103
	SET @d = TRY_CONVERT(DATE,@s,103);
	RETURN @d;
END;
GO
--RAW ==> TYPED
SET XACT_ABORT ON;
BEGIN TRAN RAW_TO_TYPED;
WITH Cleaned AS (
	SELECT 
	LTRIM(RTRIM(OrderID)) AS OrderID ,
	dbo.fn_TryParseDate(LTRIM(RTRIM(OrderDate))) AS OrderDate,
	dbo.fn_TryParseDate(LTRIM(RTRIM(ShipDate))) AS ShipDate,
	LTRIM(RTRIM(ShipMode)) AS ShipMode,
	LTRIM(RTRIM(CustomerID)) AS CustomerID,
	LEFT(LTRIM(RTRIM(CustomerName)), 100) AS CustomerName,
	NULLIF(LTRIM(RTRIM(Segment)),'' ) AS Segment,
	NULLIF(LTRIM(RTRIM(Country)),'' ) AS Country,
	NULLIF(LTRIM(RTRIM(City)),'' ) AS City,
	NULLIF(LTRIM(RTRIM(StateName)),'' ) AS StateName,
	NULLIF(LTRIM(RTRIM(PostalCode)),'' ) AS PostalCode,
	NULLIF(LTRIM(RTRIM(Region)),'' ) AS Region,
	LTRIM(RTRIM(ProductID)) AS ProductID,
	NULLIF(LTRIM(RTRIM(Category)),'' ) AS Category,
	NULLIF(LTRIM(RTRIM(SubCategory)),'' ) AS SubCategory,
	LEFT(LTRIM(RTRIM(ProductName)),200) AS ProductName,
	TRY_CONVERT(DECIMAL(10,2),REPLACE(Sales,',','')) AS Sales,
	TRY_CONVERT(int,Quantity) AS Quantity,
	TRY_CONVERT(DECIMAL(5,2),Discount) AS Discount,
	TRY_CONVERT(DECIMAL(10,2),REPLACE(Profit,',','')) AS Profit,
	ROW_NUMBER()OVER 
	(PARTITION BY LTRIM(RTRIM(OrderID)), LTRIM(RTRIM(ProductID))ORDER BY dbo.fn_TryParseDate(LTRIM(RTRIM(OrderDate))) DESC) AS rn
	FROM stg.orders_Raw
)
INSERT INTO stg.orders_Typed (OrderID, OrderDate,ShipDate ,ShipMode ,CustomerID ,CustomerName ,Segment ,
	Country ,City  ,StateName ,PostalCode ,Region ,ProductID ,Category ,
	SubCategory ,ProductName ,Sales ,Quantity ,Discount ,Profit )
SELECT 
OrderID, OrderDate,ShipDate ,ShipMode ,CustomerID ,CustomerName ,Segment ,
	COALESCE (Country, 'United States') AS Country -- IF COUNTRY NULL ==> United States
	,City  ,StateName ,PostalCode ,Region ,ProductID ,Category ,
	SubCategory ,ProductName ,Sales ,Quantity ,
	CASE WHEN  Discount  >1 THEN Discount / 100.0 ELSE Discount END AS Discount --TRANSFORM 0..100 ==> 0..1
	,Profit 
FROM Cleaned c
WHERE 
	rn =1 AND 
	OrderID IS NOT NULL AND
	CustomerID IS NOT NULL AND
	ProductID IS NOT NULL AND
	OrderDate IS NOT NULL AND
	ShipDate IS NOT NULL AND
	Sales IS NOT NULL AND
	Quantity IS NOT NULL AND
	Discount IS NOT NULL AND
	Profit IS NOT NULL 
OPTION (MAXDOP 1); -- PREVENT AUTHENTICATION PROCESS

--RECORD ERRORS
WITH Cleaned AS (
	SELECT *,
		CASE
			WHEN LTRIM(RTRIM(OrderID)) IS NULL OR LTRIM(RTRIM(ProductID)) IS NULL OR LTRIM(RTRIM(CustomerID)) IS NULL
				THEN 'Missing keys'

			WHEN dbo.fn_TryParseDate(LTRIM(RTRIM(OrderDate))) IS NULL OR dbo.fn_TryParseDate(LTRIM(RTRIM(ShipDate))) IS NULL
				THEN 'Bad dates'

			WHEN TRY_CONVERT(DECIMAL(10,2),REPLACE(Sales,',',''))  IS NULL OR TRY_CONVERT(int,Quantity) IS NULL
				OR TRY_CONVERT(DECIMAL(5,2),Discount) IS NULL OR TRY_CONVERT(DECIMAL(10,2),REPLACE(Profit,',','')) IS NULL
				THEN 'Bad numerics'
			ELSE NULL 
		END AS ReasonCalc
	FROM stg.orders_Raw
)
INSERT INTO err.ErrorRows( ErrorStage, Reason, Source_orderID, Source_productID, RAWROW)
SELECT 'RAW_TO_TYPED', ReasonCalc, OrderID, ProductID, CONCAT_WS('|', OrderID, OrderDate,ShipDate ,ShipMode ,CustomerID ,CustomerName ,Segment ,
	Country ,City  ,StateName ,PostalCode ,Region ,ProductID ,Category ,
	SubCategory ,ProductName ,Sales ,Quantity ,Discount ,Profit )
FROM Cleaned
WHERE ReasonCalc IS NOT NULL;
COMMIT TRAN RAW_TO_TYPED;

--5) CREATE FACT TABLE + DIMENSIONS 

--DimCustomers
IF OBJECT_ID('dbo.DimCustomers','U') IS NULL
BEGIN
	CREATE TABLE dbo.DimCustomers (
		CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
		CustomerID VARCHAR(20) Not NULL,
		CustomerName VARCHAR(100),
		Segment VARCHAR(50),
		Country VARCHAR(50),
		City    VARCHAR(50),
		StateName VARCHAR(50),
		PostalCode VARCHAR(50),
		Region VARCHAR(50)
		);
END

--DimProducts
IF OBJECT_ID('dbo.DimProducts','U') IS NULL
BEGIN
	CREATE TABLE dbo.DimProducts (
		ProductKey INT IDENTITY(1,1) PRIMARY KEY,
		ProductID VARCHAR(20),
		Category VARCHAR(50),
		SubCategory VARCHAR(50),
		ProductName VARCHAR(200)
		);
END

--DimDates
IF OBJECT_ID('dbo.DimDates','U') IS NULL
BEGIN
	CREATE TABLE dbo.DimDates (
		DateKEY INT PRIMARY KEY, --YYYYMMDD
		FullDate DATE,
		[Day] INT,
		[Month] INT,
		[MonthName] VARCHAR(50),
		[Quarter] INT,
		[Year] INT
		);
END

--FactOrders
IF OBJECT_ID('dbo.FactOrders','U') IS NULL
BEGIN
	CREATE TABLE dbo.FactOrders (
		OrderID  VARCHAR(20),
		OrderDateKey INT FOREIGN KEY REFERENCES dbo.DimDates(DateKEY),
		CustomerKey INT FOREIGN KEY REFERENCES dbo.DimCustomers(CustomerKey),
		ProductKey INT FOREIGN KEY REFERENCES dbo.DimProducts(ProductKey),
		Sales DECIMAL (10,2),
		Quantity INT,
		Discount DECIMAL (5,2),
		Profit DECIMAL (10,2) 
	);
END
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_SuperStore_ETL
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	
	--6)LOAD DIMENSIONS (Type 1 Updates)
	--DimCustomers
	MERGE dbo.DimCustomers AS T 
	USING (
		SELECT CustomerID, CustomerName, Segment, Country, City, 
		StateName, PostalCode, Region
		FROM ( SELECT CustomerID, CustomerName, Segment, Country, City, 
		StateName, PostalCode, Region,
		ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY CustomerID) AS RowNum 
		FROM stg.orders_Typed ) AS SubQuery
		WHERE RowNum =1
		) AS S
		ON T.CustomerID = S.CustomerID
		WHEN MATCHED AND (
			ISNULL(T.CustomerName,'') <> ISNULL(S.CustomerName,'')
			OR 
			ISNULL(T.Segment,'') <> ISNULL(S.Segment,'')
			OR 
			ISNULL(T.Country,'') <> ISNULL(S.Country,'')
			OR 
			ISNULL(T.City,'') <> ISNULL(S.City,'')
			OR 
			ISNULL(T.StateName,'') <> ISNULL(S.StateName,'')
			OR 
			ISNULL(T.PostalCode,'') <> ISNULL(S.PostalCode,'')
			OR 
			ISNULL(T.Region,'') <> ISNULL(S.Region,'')
			)
			THEN UPDATE SET CustomerName = S.CustomerName,
			Segment = S.Segment,
			Country = S.Country,
			City = S.City,
			StateName = S.StateName,
			PostalCode = S.PostalCode,
			Region = S.Region
		WHEN NOT MATCHED BY TARGET THEN 
		INSERT (CustomerID, CustomerName, Segment, Country, City, 
		StateName, PostalCode, Region) 
		VALUES (S.CustomerID, S.CustomerName, S.Segment, S.Country, S.City, 
		S.StateName, S.PostalCode, S.Region);

	--DimProducts
	MERGE dbo.DimProducts AS T 
	USING (
		SELECT DISTINCT ProductID, Category, SubCategory, ProductName
		FROM stg.orders_Typed
		) AS S
		ON T.ProductID = S.ProductID
		WHEN MATCHED AND (
			ISNULL(T.Category,'') <> ISNULL(S.Category,'')
			OR 
			ISNULL(T.SubCategory,'') <> ISNULL(S.SubCategory,'')
			OR 
			ISNULL(T.ProductName,'') <> ISNULL(S.ProductName,'')
			)
			THEN UPDATE SET Category = S.Category,
			SubCategory = S.SubCategory,
			ProductName = S.ProductName
		WHEN NOT MATCHED BY TARGET THEN 
		INSERT (ProductID, Category, SubCategory, ProductName) 
		VALUES (S.ProductID, S.Category, S.SubCategory, S.ProductName);

	--7)POPULATE DIMDATES(IF EMPTY)
	IF NOT EXISTS (SELECT 1 FROM dbo.DimDates)
	BEGIN
		DECLARE @StartDate DATE = '2014-01-04', @EndDate DATE = '2017-12-30';
		WHILE @StartDate <= @EndDate
		BEGIN
			INSERT INTO dbo.DimDates ( DateKEY, FullDate, [Day], [Month], [MonthName],
		   [Quarter], [Year])
		   VALUES(CONVERT(INT,FORMAT(@StartDate,'yyyyMMdd')), @StartDate, DAY(@StartDate), MONTH(@StartDate),
		   DATENAME(MONTH,@StartDate), DATEPART(QUARTER,@StartDate),YEAR(@StartDate));
		   SET @StartDate=DATEADD(DAY,1,@StartDate);
		END
	END	

	--8) LOAD FACT TABLE
	IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UQ_FactOrders_Order_Product')
	BEGIN
		CREATE UNIQUE INDEX UQ_FactOrders_Order_Product ON dbo.FactOrders(OrderID, ProductKey);
	END
	MERGE INTO dbo.FactOrders AS f
	USING(
		SELECT s.OrderID, p.ProductKey, c.CustomerKey,
			d.DateKEY AS OrderDateKey, s.Sales, s.Quantity , s.Discount, s.Profit
		FROM stg.orders_Typed s 
		JOIN dbo.DimProducts p ON s.ProductID = p.ProductID
		JOIN dbo.DimCustomers c ON  s.CustomerID = c.CustomerID
		JOIN dbo.DimDates d ON d.FullDate =s.OrderDate) AS s
		ON f.OrderID = s.OrderID AND f.ProductKey = s.ProductKey
		WHEN NOT MATCHED THEN 
			INSERT (OrderID, ProductKey, CustomerKey, OrderDateKey, 
			Sales, Quantity, Discount,Profit)
			VALUES(s.OrderID, s.ProductKey, s.CustomerKey, s.OrderDateKey, 
			s.Sales, s.Quantity, s.Discount, s.Profit);

	--9) INDEXES + SIMPLE CHECKS
	IF NOT EXISTS ( SELECT 1 FROM sys.indexes WHERE name = 'IX_FactOrders_Order_Date')
		CREATE INDEX IX_FactOrders_Order_Date ON dbo.FactOrders(OrderDateKey);

	IF NOT EXISTS ( SELECT 1 FROM sys.indexes WHERE name = 'IX_FactOrders_Customer')
		CREATE INDEX IX_FactOrders_Customer ON dbo.FactOrders(CustomerKey);

	IF NOT EXISTS ( SELECT 1 FROM sys.indexes WHERE name = 'IX_FactOrders_Product')
		CREATE INDEX IX_FactOrders_Product ON dbo.FactOrders(ProductKey);

	PRINT 'ETL Load Complete';
	PRINT 'Row Counts:';

	SELECT 'DimCustomers' AS TableName,COUNT(*) AS Cnt FROM dbo.DimCustomers
	UNION ALL SELECT 'DimProducts', COUNT(*) FROM dbo.DimProducts
	UNION ALL SELECT 'DimDates', COUNT(*) FROM dbo.DimDates
	UNION ALL SELECT 'FactOrders', COUNT(*) FROM dbo.FactOrders;

END 
GO

EXEC dbo.usp_load_SuperStore_ETL;

SELECT TOP 10 * FROM dbo.FactOrders