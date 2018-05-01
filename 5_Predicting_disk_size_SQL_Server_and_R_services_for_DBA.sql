/*

- Author: Tomaž Kaštrun (twitter: @tomaz_tsql | blog: http://tomaztsql.wordpress.com )
- Published: Windays 2018
- Date: 26.04.2018
- Location: Poreè, Croatia

Content:

-	Disk space usage
-	Clustering queries 
-	Plotting heatmap on executed queries

*/

-- ****************************
--
--
--  Disk space usage 
--
--
-- ****************************



USE [master];
GO

-- DROP DATABASE FixSizeDB

CREATE DATABASE FixSizeDB
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'FixSizeDB', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER2017\MSSQL\DATA\FixSizeDB.mdf' , SIZE = 8192KB , FILEGROWTH = 0)
 LOG ON 
( NAME = N'FixSizeDB_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER2017\MSSQL\DATA\FixSizeDB_log.ldf' , SIZE = 8192KB , FILEGROWTH = 0)
GO

ALTER DATABASE [FixSizeDB] SET COMPATIBILITY_LEVEL = 130
GO

ALTER DATABASE [FixSizeDB] SET RECOVERY SIMPLE 
GO



USE FixSizeDB;
GO



-- create a table
CREATE TABLE DataPack
	(
	  DataPackID BIGINT IDENTITY NOT NULL
	 ,col1 VARCHAR(1000) NOT NULL
	 ,col2 VARCHAR(1000) NOT NULL
	 )

-- check space used
SP_SPACEUSED N'DataPack'




-- let's insert 1000 rows
-- populate data
DECLARE @i INT = 1;
BEGIN TRAN
	WHILE @i <= 1000
		BEGIN
			INSERT dbo.DataPack(col1, col2)
				SELECT 
					  REPLICATE('A',200)
					 ,REPLICATE('B',300);
			SET @i = @i + 1;
		END
COMMIT;
GO


-- check space used again
SP_SPACEUSED N'DataPack'



-- using allocation_units sys table
SELECT 
    t.NAME AS TableName
    ,s.Name AS SchemaName
    ,p.rows AS RowCounts
    ,SUM(a.total_pages) * 8 AS TotalSpaceKB
    ,SUM(a.used_pages) * 8 AS UsedSpaceKB
    ,(SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
FROM 
    sys.tables t
INNER JOIN sys.indexes AS i 
	ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions AS p 
	ON i.object_id = p.OBJECT_ID 
	AND i.index_id = p.index_id
INNER JOIN sys.allocation_units AS a 
	ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas AS s 
	ON t.schema_id = s.schema_id

WHERE 
	     t.NAME NOT LIKE 'dt%' 
    AND t.is_ms_shipped = 0
    AND i.OBJECT_ID > 255 
	AND t.Name = 'DataPack'
GROUP BY t.Name, s.Name, p.Rows


-- creating log table
SELECT 
    t.NAME AS TableName
    ,s.Name AS SchemaName
    ,p.rows AS RowCounts
    ,SUM(a.total_pages) * 8 AS TotalSpaceKB
    ,SUM(a.used_pages) * 8 AS UsedSpaceKB
    ,(SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
	,GETDATE() AS TimeMeasure

INTO dbo.DataPack_Info_SMALL

FROM 
    sys.tables AS t
INNER JOIN sys.indexes AS i 
	ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions AS p 
	ON i.object_id = p.OBJECT_ID 
	AND i.index_id = p.index_id
INNER JOIN sys.allocation_units AS a 
	ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas AS s 
	ON t.schema_id = s.schema_id

WHERE 
	     t.NAME NOT LIKE 'dt%' 
    AND t.is_ms_shipped = 0
	AND t.name = 'DataPack'
    AND i.OBJECT_ID > 255 
GROUP BY t.Name, s.Name, p.Rows
ORDER BY  t.Name


SELECT * FROM dbo.DataPack_Info_SMALL



-- it should fail at 8th step when step is 1000 rows
DECLARE @nof_steps INT = 0

WHILE @nof_steps < 15
BEGIN
	BEGIN TRAN
		-- insert some data
		DECLARE @i INT = 1;
		WHILE @i <= 1000 -- step is 100 rows
					BEGIN
						INSERT dbo.DataPack(col1, col2)
							SELECT 
								  REPLICATE('A',FLOOR(RAND()*200))
								 ,REPLICATE('B',FLOOR(RAND()*300));
						SET @i = @i + 1;
					END
			

		-- run statistics on table
		INSERT INTO dbo.DataPack_Info_SMALL
		SELECT 
			t.NAME AS TableName
			,s.Name AS SchemaName
			,p.rows AS RowCounts
			,SUM(a.total_pages) * 8 AS TotalSpaceKB
			,SUM(a.used_pages) * 8 AS UsedSpaceKB
			,(SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
			,GETDATE() AS TimeMeasure
		FROM  
				sys.tables AS t
			INNER JOIN sys.indexes AS i 
			ON t.OBJECT_ID = i.object_id
			INNER JOIN sys.partitions AS p 
			ON i.object_id = p.OBJECT_ID 
			AND i.index_id = p.index_id
			INNER JOIN sys.allocation_units AS a 
			ON p.partition_id = a.container_id
			LEFT OUTER JOIN sys.schemas AS s 
			ON t.schema_id = s.schema_id

		WHERE 
				 t.NAME NOT LIKE 'dt%' 
			AND t.is_ms_shipped = 0
			AND t.name = 'DataPack'
			AND i.OBJECT_ID > 255 
		GROUP BY t.Name, s.Name, p.Rows
	
		WAITFOR DELAY '00:00:02'

	COMMIT;

END
-- Duration 00:00:35

-- let's check the content
SELECT
*
--INTO AdventureWorks.dbo.DataPack_Info_SMALL_bck
FROM DataPack_Info_SMALL




-- and run statistical correlation between
DECLARE @RScript nvarchar(max)
SET @RScript = N'
				 library(Hmisc)	
				 mydata <- InputDataSet
				 all_sub <- mydata[2:3]
				 c <- cor(all_sub, use="complete.obs", method="pearson") 
				 t <- rcorr(as.matrix(all_sub), type="pearson")
			 	 c <- cor(all_sub, use="complete.obs", method="pearson") 
				 c <- data.frame(c)
				 OutputDataSet <- c'

DECLARE @SQLScript nvarchar(max)
SET @SQLScript = N'SELECT
						 TableName
						,RowCounts
						,UsedSpaceKB
						,TimeMeasure
						FROM DataPack_Info_SMALL'

EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript
	,@input_data_1 = @SQLScript
	WITH result SETS ( (
						 RowCounts VARCHAR(100)
						,UsedSpaceKB  VARCHAR(100)
						)
					 );

GO


-- imagine having some  deletes in between
SET NOCOUNT ON;

DROP TABLE DataPack;
DROP TABLE DataPack_Info_LARGE;


-- create a table
CREATE TABLE DataPack
	(
	  DataPackID BIGINT IDENTITY NOT NULL
	 ,col1 VARCHAR(1000) NOT NULL
	 ,col2 VARCHAR(1000) NOT NULL
	 )


-- populate data
--DECLARE @i INT = 1;
--BEGIN TRAN
--	WHILE @i <= 1000
--		BEGIN
--			INSERT dbo.DataPack(col1, col2)
--				SELECT 
--					  REPLICATE('A',200)
--					 ,REPLICATE('B',300);
--			SET @i = @i + 1;
--		END
--COMMIT;
--GO


-- creating log table
SELECT 
    t.NAME AS TableName
    ,s.Name AS SchemaName
    ,p.rows AS RowCounts
    ,SUM(a.total_pages) * 8 AS TotalSpaceKB
    ,SUM(a.used_pages) * 8 AS UsedSpaceKB
    ,(SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
	,GETDATE() AS TimeMeasure
	,'          ' AS Operation
	,0 AS NofRowsOperation

INTO dbo.DataPack_Info_LARGE

FROM 
    sys.tables AS t
INNER JOIN sys.indexes AS i 
	ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions AS p 
	ON i.object_id = p.OBJECT_ID 
	AND i.index_id = p.index_id
INNER JOIN sys.allocation_units AS a 
	ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas AS s 
	ON t.schema_id = s.schema_id

WHERE 
	     t.NAME NOT LIKE 'dt%' 
    AND t.is_ms_shipped = 0
	AND t.name = 'DataPack'
    AND i.OBJECT_ID > 255 
GROUP BY t.Name, s.Name, p.Rows


-- Check the table:

SELECT * FROM dbo.DataPack_Info_LARGE


-- Add delete into the inserts 
DECLARE @nof_steps INT = 0

WHILE @nof_steps < 15
BEGIN
	BEGIN TRAN
		-- insert some data
		DECLARE @i INT = 1;
		DECLARE @insertedRows INT = 0;
		DECLARE @deletedRows INT = 0;
		DECLARE @Rand DECIMAL(10,2) = RAND()*10
		
		IF @Rand < 5
		  BEGIN
					WHILE @i <= 1000 -- step is 100 rows
								BEGIN
									INSERT dbo.DataPack(col1, col2)
										SELECT 
											  REPLICATE('A',FLOOR(RAND()*200))  -- pages are filling up differently
											 ,REPLICATE('B',FLOOR(RAND()*300));
									SET @i = @i + 1;
								END
					SET @insertedRows = 1000

					
			END
		 IF @Rand  >= 5
			BEGIN			
					
					SET @deletedRows = (SELECT COUNT(*) FROM dbo.DataPack WHERE DataPackID % 3 = 0)
					DELETE FROM dbo.DataPack
							WHERE
					DataPackID % 3 = 0 OR DataPackID % 5 = 0

					
			END


		-- run statistics on table
		INSERT INTO dbo.DataPack_Info_LARGE
		SELECT 
			t.NAME AS TableName
			,s.Name AS SchemaName
			,p.rows AS RowCounts
			,SUM(a.total_pages) * 8 AS TotalSpaceKB
			,SUM(a.used_pages) * 8 AS UsedSpaceKB
			,(SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
			,GETDATE() AS TimeMeasure
			,CASE WHEN @Rand < 5 THEN 'Insert'
				  WHEN @Rand >= 5 THEN 'Delete'
				  ELSE 'meeeh' END AS Operation
			,CASE WHEN @Rand < 5 THEN @insertedRows
				  WHEN @Rand >= 5 THEN @deletedRows
				  ELSE 0 END AS NofRowsOperation
		FROM  
				sys.tables AS t
			INNER JOIN sys.indexes AS i 
			ON t.OBJECT_ID = i.object_id
			INNER JOIN sys.partitions AS p 
			ON i.object_id = p.OBJECT_ID 
			AND i.index_id = p.index_id
			INNER JOIN sys.allocation_units AS a 
			ON p.partition_id = a.container_id
			LEFT OUTER JOIN sys.schemas AS s 
			ON t.schema_id = s.schema_id

		WHERE 
				 t.NAME NOT LIKE 'dt%' 
			AND t.is_ms_shipped = 0
			AND t.name = 'DataPack'
			AND i.OBJECT_ID > 255 
		GROUP BY t.Name, s.Name, p.Rows
	
		WAITFOR DELAY '00:00:01'

	COMMIT;

END
-- Duration 00:00:50

-- check the distribution ... slightly not "that straightforward"
SELECT * FROM DataPack_Info_LARGE



DECLARE @RScript nvarchar(max)
SET @RScript = N'
				 library(Hmisc)	
				 mydata <- InputDataSet
				 all_sub <- mydata[2:3]
				 c <- cor(all_sub, use="complete.obs", method="pearson") 
				 c <- data.frame(c)
				 OutputDataSet <- c'

DECLARE @SQLScript nvarchar(max)
SET @SQLScript = N'SELECT
						 TableName
						,RowCounts
						,UsedSpaceKB
						,TimeMeasure
						FROM DataPack_Info_LARGE'

EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript
	,@input_data_1 = @SQLScript
	WITH result SETS ( (
						 RowCounts VARCHAR(100)
						,UsedSpaceKB  VARCHAR(100)
						)
					 );

GO

-- I presumme, everything is fine
-- but what if we switch the correlation between 
-- UnusedSpaceKB and UsedSpacesKB
-- I will run concurrently both Logs - small and large

DECLARE @RScript1 nvarchar(max)
SET @RScript1 = N'
				 library(Hmisc)	
				 mydata <- InputDataSet
				 all_sub <- mydata[4:5]
				 c <- cor(all_sub, use="complete.obs", method="pearson") 
				 c <- data.frame(c)
				 OutputDataSet <- c'

DECLARE @SQLScript1 nvarchar(max)
SET @SQLScript1 = N'SELECT
						 TableName
						,RowCounts
						,TimeMeasure
						,UsedSpaceKB	
						,UnusedSpaceKB
						FROM DataPack_Info_SMALL
						WHERE RowCounts <> 0'

EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript1
	,@input_data_1 = @SQLScript1
	WITH result SETS ( (
						 RowCounts VARCHAR(100)
						,UsedSpaceKB  VARCHAR(100)
						)
					 );

GO


DECLARE @RScript2 nvarchar(max)
SET @RScript2 = N'
				 library(Hmisc)	
				 mydata <- InputDataSet
				 all_sub <- mydata[4:5]
				 c <- cor(all_sub, use="complete.obs", method="pearson") 
				 c <- data.frame(c)
				 OutputDataSet <- c'

DECLARE @SQLScript2 nvarchar(max)
SET @SQLScript2 = N'SELECT
						 TableName
						,RowCounts
						,TimeMeasure
						,UsedSpaceKB	
						,UnusedSpaceKB
						FROM DataPack_Info_LARGE
						WHERE NofRowsOperation <> 0
						AND RowCounts <> 0'

EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript2
	,@input_data_1 = @SQLScript2
	WITH result SETS ( (
						 RowCounts VARCHAR(100)
						,UsedSpaceKB  VARCHAR(100)
						)
					 );

GO


--- making regression prediction 

-- my model:
SELECT 
	TableName
	,Operation
	,NofRowsOperation
	,UsedSpaceKB
	,UnusedSpaceKB
 FROM dbo.DataPack_Info_LARGE



 SELECT 
					TableName
					,CASE WHEN Operation = 'Insert' THEN 1 ELSE 0 END AS Operation
					,NofRowsOperation
					,UsedSpaceKB
					,UnusedSpaceKB
					 FROM dbo.DataPack_Info_LARGE
					 WHERE
						NofRowsOperation <> 0

-- GLM prediction
DECLARE @SQL_input AS NVARCHAR(MAX)
SET @SQL_input = N'SELECT 
					TableName
					,CASE WHEN Operation = ''Insert'' THEN 1 ELSE 0 END AS Operation
					,NofRowsOperation
					,UsedSpaceKB
					,UnusedSpaceKB
					 FROM dbo.DataPack_Info_LARGE
					 WHERE
						NofRowsOperation <> 0';

DECLARE @R_code AS NVARCHAR(MAX)
SET @R_code = N'
			 library(RevoScaleR)
			 library(dplyr)
			 
				DPLogR <- rxGlm(UsedSpaceKB ~ Operation + NofRowsOperation + UnusedSpaceKB, 
				data = DataPack_info, family = Gamma)
				df_predict <- data.frame(TableName=("DataPack"), Operation=(1), 
				NofRowsOperation=(451), UnusedSpaceKB=(20))
				predictions <- rxPredict(modelObject = DPLogR, data = df_predict, 
				outData = NULL,  predVarNames = "UsedSpaceKB", type = "response",
				checkFactorLevels=FALSE);
				OutputDataSet <- predictions'

EXEC sys.sp_execute_external_script
     @language = N'R'
    ,@script = @R_code
	,@input_data_1 = @SQL_input
	,@input_data_1_name = N'DataPack_info'

	WITH RESULT SETS ((
					  UsedSpaceKB_predict INT
					 )); 
					GO



-- GLM prediction
CREATE PROCEDURE Predict_UsedSpace 
(
 @TableName NVARCHAR(100)
,@Operation CHAR(1)  -- 1  = Insert; 0 = Delete
,@NofRowsOperation NVARCHAR(10)
,@UnusedSpaceKB NVARCHAR(10)
)
AS

DECLARE @SQL_input AS NVARCHAR(MAX)
SET @SQL_input = N'SELECT 
					TableName
					,CASE WHEN Operation = ''Insert'' THEN 1 ELSE 0 END AS Operation
					,NofRowsOperation
					,UsedSpaceKB
					,UnusedSpaceKB
					 FROM dbo.DataPack_Info_LARGE
					 WHERE
						NofRowsOperation <> 0';

DECLARE @R_code AS NVARCHAR(MAX)
SET @R_code = N'
			 library(RevoScaleR)
			 library(dplyr)
			 
				DPLogR <- rxGlm(UsedSpaceKB ~ Operation + NofRowsOperation + UnusedSpaceKB, data = DataPack_info, family = Gamma)
				df_predict <- data.frame(TableName=("'+@TableName+'"), Operation=('+@Operation+'), NofRowsOperation=('+@NofRowsOperation+'), UnusedSpaceKB=('+@UnusedSpaceKB+'))
				predictions <- rxPredict(modelObject = DPLogR, data = df_predict, outData = NULL,  predVarNames = "UsedSpaceKB", type = "response",checkFactorLevels=FALSE);
				OutputDataSet <- predictions'

EXEC sys.sp_execute_external_script
     @language = N'R'
    ,@script = @R_code
	,@input_data_1 = @SQL_input
	,@input_data_1_name = N'DataPack_info'

	WITH RESULT SETS ((
					  UsedSpaceKB_predict INT
					 )); 
					GO


EXECUTE Predict_UsedSpace 
			@TableName = 'DataPack'
			,@Operation = 1 
			,@NofRowsOperation = 120
			,@UnusedSpaceKB = 2;
GO


EXECUTE Predict_UsedSpace 
			@TableName = 'DataPack'
			,@Operation = 1 
			,@NofRowsOperation = 500
			,@UnusedSpaceKB = 12;
GO

-- we can also analyse table statistics
USE AdventureWorks;

-- this is where you also want to work on predictions
DBCC SHOW_STATISTICS ("Person.Address", AK_Address_rowguid);  
GO  




-- Addint IO Virtual Files stats
SELECT 
	DB_NAME(mf.database_id) AS DbName
    ,divfs.num_of_reads
    ,divfs.num_of_writes
    ,divfs.num_of_bytes_read
    ,divfs.num_of_bytes_written
    ,divfs.io_stall_read_ms
    ,divfs.io_stall_write_ms
    ,divfs.io_stall AS TotalIOStall
    ,GETDATE() AS CaptureDate
FROM 
	sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs 
    JOIN sys.master_files AS mf 
	ON mf.database_id = divfs.database_id 
	AND mf.file_id = divfs.file_id
WHERE
	 DB_NAME(mf.database_id) = 'FixSizeDB'





-- Work on BASELINE 	
SELECT DISTINCT 
 *
FROM sys.dm_os_performance_counters 
WHERE 
	instance_name = 'FixSizeDB'
and object_name = 'SQLServer:Databases'


SELECT 
	DB_NAME(mf.database_id) AS DbName
    ,divfs.num_of_reads
    ,divfs.num_of_writes
    ,divfs.num_of_bytes_read
    ,divfs.num_of_bytes_written
    ,divfs.io_stall_read_ms
    ,divfs.io_stall_write_ms
    ,divfs.io_stall AS TotalIOStall
    ,GETDATE() AS CaptureDate
	,*
FROM 
	sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs 
    JOIN sys.master_files AS mf 
	ON mf.database_id = divfs.database_id 
	AND mf.file_id = divfs.file_id
WHERE
	 DB_NAME(mf.database_id) = 'FixSizeDB'
AND mf.type = 1 --ROWS (not LOG)




USE [master];
GO
DROP DATABASE FixSizeDB;
GO

-- start Agent
--EXECUTE sp_startagent 




-- ****************************
--
--
--      Clustering queries 
--
--
-- ****************************


-- clean buffer
DECLARE @dbid INTEGER
SELECT @dbid = [dbid] 
FROM master..sysdatabases 
WHERE name = 'WideWorldImportersDW'
DBCC FLUSHPROCINDB (@dbid);
GO

-- Do some unneccessary load
USE WideWorldImportersDW;
GO



-- My Query 
--Finding arbitrary query:
SELECT cu.[Customer Key] AS CustomerKey, cu.Customer,
  ci.[City Key] AS CityKey, ci.City, 
  ci.[State Province] AS StateProvince, ci.[Sales Territory] AS SalesTeritory,
  d.Date, d.[Calendar Month Label] AS CalendarMonth, 
  d.[Calendar Year] AS CalendarYear,
  s.[Stock Item Key] AS StockItemKey, s.[Stock Item] AS Product, s.Color,
  e.[Employee Key] AS EmployeeKey, e.Employee,
  f.Quantity, f.[Total Excluding Tax] AS TotalAmount, f.Profit
FROM Fact.Sale AS f
  INNER JOIN Dimension.Customer AS cu
    ON f.[Customer Key] = cu.[Customer Key]
  INNER JOIN Dimension.City AS ci
    ON f.[City Key] = ci.[City Key]
  INNER JOIN Dimension.[Stock Item] AS s
    ON f.[Stock Item Key] = s.[Stock Item Key]
  INNER JOIN Dimension.Employee AS e
    ON f.[Salesperson Key] = e.[Employee Key]
  INNER JOIN Dimension.Date AS d
    ON f.[Delivery Date Key] = d.Date;
GO 3


-- FactSales Query
SELECT * FROM Fact.Sale
GO 4


-- Person Query
SELECT * FROM [Dimension].[Customer]
WHERE [Buying Group] <> 'Tailspin Toys'

  OR [WWI Customer ID] > 500
ORDER BY [Customer],[Bill To Customer]
GO 4



SELECT * 
FROM [Fact].[Order] AS o
INNER JOIN [Fact].[Purchase] AS p 
ON o.[Order Key] = p.[WWI Purchase Order ID]
GO 3
-- total Duration 00:00:24





-- let us run the query stats and get a headache
SELECT

	(total_logical_reads + total_logical_writes) AS total_logical_io
	,(total_logical_reads / execution_count) AS avg_logical_reads
	,(total_logical_writes / execution_count) AS avg_logical_writes
	,(total_physical_reads / execution_count) AS avg_phys_reads
	,substring(st.text,(qs.statement_start_offset / 2) + 1,  ((CASE qs.statement_end_offset 
																WHEN - 1 THEN datalength(st.text) 
																ELSE qs.statement_end_offset END  - qs.statement_start_offset) / 2) + 1) AS statement_text
	,*
-- Don't drop table query_stats_LOG - is used in Report
-- DROP TABLE query_stats_LOG_2
INTO query_stats_LOG_3
FROM
		sys.dm_exec_query_stats AS qs
	CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
ORDER BY
total_logical_io DESC
-- (26 row(s) affected)


SELECT 
 [total_logical_io]
,[avg_logical_reads]
,[avg_phys_reads]
,execution_count
,[total_physical_reads]
,[total_elapsed_time]
,total_dop
,left([text],100) AS [text]
,row_number() over (order by (select 1)) as ln
 FROM query_stats_LOG_2
WHERE Number is null





ALTER PROCEDURE [dbo].[SP_Query_Stats_Cluster]
AS
DECLARE @RScript nvarchar(max)

SET @RScript = N'
				 library(cluster)
				 All <- InputDataSet
				 image_file <- tempfile()
				 jpeg(filename = image_file, width = 500, height = 500)
					d <- dist(All, method = "euclidean") 
					fit <- hclust(d, method="ward.D")
					op <- par(bg = "lightblue")
					plot(fit,main="Query Hierarchical Clustering", ylab ="Distance", xlab = "", hang = -1, sub = "")
					groups <- cutree(fit, k=3) 
					rect.hclust(fit, k=3, border="DarkRed")			
				 dev.off()
				 OutputDataSet <- data.frame(data=readBin(file(image_file, "rb"), what=raw(), n=1e6))' 


DECLARE @SQLScript nvarchar(max)
SET @SQLScript = N'
				SELECT 
					 [total_logical_io]
					,[avg_logical_reads]
					,[avg_phys_reads]
					,execution_count
					,[total_physical_reads]
					,[total_elapsed_time]
					,total_dop
					,[text]
					,ROW_NUMBER() OVER (order by (select 1)) as ID
				 FROM query_stats_LOG_2
				WHERE Number is null';

EXECUTE sp_execute_external_script
@language = N'R',
@script = @RScript,
@input_data_1 = @SQLScript
WITH RESULT SETS ((Plot varbinary(max)))

GO

EXECUTE [SP_Query_Stats_Cluster]

SELECT
* FROM
(
SELECT 
					 [total_logical_io]
					,[avg_logical_reads]
					,[avg_phys_reads]
					,execution_count
					,[total_physical_reads]
					,[total_elapsed_time]
					,total_dop
					,[text]
					,ROW_NUMBER() OVER (order by (select 1)) as ID
				 FROM query_stats_LOG_2
				WHERE Number is null
) AS x
WHERE x.ID IN (5,20,8,33,12,25)


---***********************
--
--  Plotting heatmap on executed queries
--
---***********************

USE AdventureWorks;
GO

--- Turn on the query store

ALTER DATABASE AdventureWorks SET QUERY_STORE = ON;  
GO

SELECT * FROM sys.database_query_store_options


SELECT 
'SELECT * FROM AdventureWorks.'+QUOTENAME(s.Name)+'.' + QUOTENAME(t.Name) + '; '
FROM AdventureWorks.sys.Tables as T
JOIN AdventureWorks.sys.schemas AS S
ON T.SCHEMA_ID = S.schema_id
WHERE [type] = 'U';
GO


-- run bunch of queries :-)
SELECT * FROM AdventureWorks.[Production].[ScrapReason]; 
SELECT * FROM AdventureWorks.[HumanResources].[Shift]; 
SELECT * FROM AdventureWorks.[Production].[ProductCategory]; 
SELECT * FROM AdventureWorks.[Purchasing].[ShipMethod]; 
SELECT * FROM AdventureWorks.[Production].[ProductCostHistory]; 
SELECT * FROM AdventureWorks.[Production].[ProductDescription]; 
SELECT * FROM AdventureWorks.[Sales].[ShoppingCartItem]; 
SELECT * FROM AdventureWorks.[Production].[ProductDocument]; 
SELECT * FROM AdventureWorks.[dbo].[DatabaseLog]; 
SELECT * FROM AdventureWorks.[Production].[ProductInventory]; 
SELECT * FROM AdventureWorks.[Sales].[SpecialOffer]; 
SELECT * FROM AdventureWorks.[Production].[ProductListPriceHistory]; 
SELECT * FROM AdventureWorks.[Person].[Address]; 
SELECT * FROM AdventureWorks.[Sales].[SpecialOfferProduct]; 
SELECT * FROM AdventureWorks.[Production].[ProductModel]; 
SELECT * FROM AdventureWorks.[Person].[AddressType]; 
SELECT * FROM AdventureWorks.[Person].[StateProvince]; 
SELECT * FROM AdventureWorks.[Production].[ProductModelIllustration]; 
SELECT * FROM AdventureWorks.[Production].[ProductModelProductDescriptionCulture]; 
SELECT * FROM AdventureWorks.[Production].[BillOfMaterials]; 
SELECT * FROM AdventureWorks.[Sales].[Store]; 
SELECT * FROM AdventureWorks.[Production].[ProductPhoto]; 
SELECT * FROM AdventureWorks.[Production].[ProductProductPhoto]; 
SELECT * FROM AdventureWorks.[Production].[TransactionHistory]; 
SELECT * FROM AdventureWorks.[Production].[ProductReview]; 
SELECT * FROM AdventureWorks.[Person].[BusinessEntity]; 
SELECT * FROM AdventureWorks.[Production].[TransactionHistoryArchive]; 
SELECT * FROM AdventureWorks.[Production].[ProductSubcategory]; 
SELECT * FROM AdventureWorks.[Person].[BusinessEntityAddress]; 
SELECT * FROM AdventureWorks.[Purchasing].[ProductVendor]; 
SELECT * FROM AdventureWorks.[Person].[BusinessEntityContact]; 
SELECT * FROM AdventureWorks.[Production].[UnitMeasure]; 
SELECT * FROM AdventureWorks.[Purchasing].[Vendor]; 
SELECT * FROM AdventureWorks.[Person].[ContactType]; 
SELECT * FROM AdventureWorks.[Sales].[CountryRegionCurrency]; 
SELECT * FROM AdventureWorks.[Person].[CountryRegion]; 
SELECT * FROM AdventureWorks.[Production].[WorkOrder]; 
SELECT * FROM AdventureWorks.[Purchasing].[PurchaseOrderDetail]; 
SELECT * FROM AdventureWorks.[Sales].[CreditCard]; 
SELECT * FROM AdventureWorks.[Production].[Culture]; 
SELECT * FROM AdventureWorks.[Production].[WorkOrderRouting]; 
SELECT * FROM AdventureWorks.[Sales].[Currency]; 
SELECT * FROM AdventureWorks.[Purchasing].[PurchaseOrderHeader]; 
SELECT * FROM AdventureWorks.[Sales].[CurrencyRate]; 
SELECT * FROM AdventureWorks.[Sales].[Customer]; 
SELECT * FROM AdventureWorks.[HumanResources].[Department]; 
SELECT * FROM AdventureWorks.[Production].[Document]; 
SELECT * FROM AdventureWorks.[Sales].[SalesOrderDetail]; 
SELECT * FROM AdventureWorks.[Person].[EmailAddress]; 
SELECT * FROM AdventureWorks.[HumanResources].[Employee]; 
SELECT * FROM AdventureWorks.[Sales].[SalesOrderHeader]; 
SELECT * FROM AdventureWorks.[HumanResources].[EmployeeDepartmentHistory]; 
SELECT * FROM AdventureWorks.[HumanResources].[EmployeePayHistory]; 
SELECT * FROM AdventureWorks.[Sales].[SalesOrderHeaderSalesReason]; 
SELECT * FROM AdventureWorks.[Sales].[SalesPerson]; 
SELECT * FROM AdventureWorks.[Production].[Illustration]; 
SELECT * FROM AdventureWorks.[HumanResources].[JobCandidate]; 
SELECT * FROM AdventureWorks.[Production].[Location]; 
SELECT * FROM AdventureWorks.[Person].[Password]; 
SELECT * FROM AdventureWorks.[dbo].[Orders]; 
SELECT * FROM AdventureWorks.[Sales].[SalesPersonQuotaHistory]; 
SELECT * FROM AdventureWorks.[Person].[Person]; 
SELECT * FROM AdventureWorks.[Sales].[SalesReason]; 
SELECT * FROM AdventureWorks.[Sales].[SalesTaxRate]; 
SELECT * FROM AdventureWorks.[Sales].[PersonCreditCard]; 
SELECT * FROM AdventureWorks.[Person].[PersonPhone]; 
SELECT * FROM AdventureWorks.[Sales].[SalesTerritory]; 
SELECT * FROM AdventureWorks.[Person].[PhoneNumberType]; 
SELECT * FROM AdventureWorks.[Production].[Product]; ; 
SELECT * FROM AdventureWorks.[Sales].[SalesTerritoryHistory]; 



	-- Collect the data from Query Store
	SELECT 
		 qsq.*
		,query_sql_text 

	INTO QS_Query_stats_bck

		FROM sys.query_store_query AS qsq
	JOIN sys.query_store_query_text AS qsqt
	ON qsq.query_text_id = qsqt.query_text_id

	WHERE 
		Query_id >= 41
	ORDER BY 
		 qsq.query_id
-- (1039 row(s) affected)

SELECT 
 query_sql_text
,last_compile_batch_offset_start
,last_compile_batch_offset_end
,count_compiles
,avg_compile_duration
,last_compile_duration
,avg_bind_duration
,last_bind_duration
,avg_bind_cpu_time
,last_bind_cpu_time
,avg_optimize_duration
,last_optimize_duration
,avg_optimize_cpu_time
,last_optimize_cpu_time
,avg_compile_memory_kb
,last_compile_memory_kb
,max_compile_memory_kb

FROM QS_Query_stats_bck


-- for R table
SELECT  
	 LEFT(query_sql_text,70) AS Query_Name
	,last_compile_batch_offset_start
	,last_compile_batch_offset_end
	,count_compiles
	,avg_compile_duration
	,avg_bind_duration
	,avg_bind_cpu_time
	,avg_optimize_duration
	,avg_optimize_cpu_time
    ,avg_compile_memory_kb 

	-- DROP TABLE QS_Query_stats_bck_2			   
INTO QS_Query_stats_bck_2			   

FROM QS_Query_stats_bck

WHERE
	LEFT(query_sql_text,70) LIKE 'SELECT * FROM AdventureWorks.%'
ORDER BY  Query_Name


DECLARE @SQL_heat NVARCHAR(MAX)
SET @SQL_heat = 'SELECT  
	 LEFT(query_sql_text,70) AS Query_Name
	,last_compile_batch_offset_start
	,last_compile_batch_offset_end
	,count_compiles
	,avg_compile_duration
	,avg_bind_duration
	,avg_bind_cpu_time
	,avg_optimize_duration
	,avg_optimize_cpu_time
    ,avg_compile_memory_kb 
			  
		FROM QS_Query_stats_bck

		WHERE
			LEFT(query_sql_text,70) LIKE ''SELECT * FROM AdventureWorks.%''
		ORDER BY  Query_Name';

DECLARE @RScript NVARCHAR(MAX)
SET @RScript = N'
				 library(d3heatmap)
				 All <- InputDataSet
				 image_file <- tempfile()
				 jpeg(filename = image_file, width = 500, height = 500)		
					# sort data
				 All <- All[order(All$avg_compile_duration),]
					#row_names
				 row.names(All) <- All$Query_Name
				 All <- All[,2:10]
				 All_QS_matrix <- data.matrix(All)							
				 heatmap(All_QS_matrix, Rowv=NA, Colv=NA, col = heat.colors(256), scale="column", margins=c(5,10))
				 dev.off()
				 OutputDataSet <- data.frame(data=readBin(file(image_file, "rb"), what=raw(), n=1e6))' 

EXECUTE sp_execute_external_script
@language = N'R',
@script = @RScript,
@input_data_1 = @SQL_heat
WITH RESULT SETS ((Plot VARBINARY(MAX)));
GO


-- Query store OFF
ALTER DATABASE [DBA4R] SET QUERY_STORE = OFF;

