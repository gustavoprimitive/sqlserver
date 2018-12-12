/*
Procedimiento INTERCAMBIO_PARTICION.
Transfiere una partici�n, de una fecha determinada, de una tabla origen a otra destino.
Par�metros: @v_source_tab_name. Nombre de la tabla cuya partici�n se quiere transferir.
			@v_target_tab_name. Nombre de la tabla a la que se quiere transferir la partici�n.
			@v_date. Fecha de la partici�n a transferir (la fecha est� en el intervalo etl_particiones.left_boundary y etl_particiones.right_boundary).					
					
Incluye output de errores y eventos.			
10/12/2018	v. 0.1
*/

CREATE PROCEDURE [dbo].[INTERCAMBIO_PARTICION]
		@v_source_tab_name VARCHAR(128),
		@v_target_tab_name VARCHAR(128),	   
		@v_date DATETIME
AS
BEGIN

DECLARE @v_partition_number INT, @v_sql VARCHAR(MAX), @v_count INT, @v_tab_name VARCHAR(128), @v_num_rows INT, @v_error BIT = 0
DECLARE cur_check_tabs CURSOR FOR SELECT @v_source_tab_name UNION SELECT @v_target_tab_name

--Comprobaci�n de existencia de las tablas recibidas
OPEN cur_check_tabs
FETCH NEXT FROM cur_check_tabs INTO @v_tab_name
WHILE @@FETCH_STATUS = 0
BEGIN 

	IF object_id('dbo.' + @v_tab_name) IS NULL
		BEGIN
			PRINT CONVERT(VARCHAR(50), GETDATE()) + CHAR(9) + CHAR(9) + 'ERROR: No se encuentra la tabla ' + @v_tab_name
			SET @v_error = 1
		END

	FETCH NEXT FROM cur_check_tabs INTO @v_tab_name
END
CLOSE cur_check_tabs
DEALLOCATE cur_check_tabs

--Obtenci�n de ID de partici�n
IF @v_error = 0
	BEGIN
		BEGIN TRY 
			SELECT @v_partition_number = partition_number, @v_num_rows = rows
			FROM (SELECT DISTINCT TOP (100) PERCENT f.name AS file_group_name, SCHEMA_NAME(t.schema_id) AS table_schema, p.partition_number AS partition_number, pf.boundary_value_on_right, 
						ISNULL(left_prv.value, CAST(-53690 AS DATETIME)) AS left_boundary, ISNULL(right_prv.value, CAST(2958463 AS DATETIME)) AS right_boundary, p.rows, t.name, pf.name as partition_funcion, ps.name as patition_schema
					FROM sys.partitions AS p INNER JOIN
						sys.tables AS t ON p.object_id = t.object_id INNER JOIN
						sys.indexes AS i ON p.object_id = i.object_id AND p.index_id = i.index_id INNER JOIN
						sys.allocation_units AS au ON p.hobt_id = au.container_id INNER JOIN
						sys.filegroups AS f ON au.data_space_id = f.data_space_id LEFT OUTER JOIN
						sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id LEFT OUTER JOIN
						sys.partition_functions AS pf ON ps.function_id = pf.function_id LEFT OUTER JOIN
						sys.partition_range_values AS left_prv ON left_prv.function_id = ps.function_id AND left_prv.boundary_id + 1 = p.partition_number LEFT OUTER JOIN
						sys.partition_range_values AS right_prv ON right_prv.function_id = ps.function_id AND right_prv.boundary_id = p.partition_number) aux
			WHERE @v_date BETWEEN aux.left_boundary AND aux.right_boundary
			AND aux.name = @v_source_tab_name 
			AND aux.table_schema = 'dbo'
			
			IF @@ROWCOUNT = 0
				BEGIN
					PRINT CONVERT(VARCHAR(50), GETDATE()) + CHAR(9) + CHAR(9) + 'ERROR: No se encuentra partici�n para la fecha y tabla indicadas'	
					SET @v_error = 1
				END
		END TRY  
		BEGIN CATCH  
			PRINT CONVERT(VARCHAR(50), GETDATE()) + CHAR(9) + CHAR(9) + 'ERROR: No se ha podido ejecutar la b�squeda de partici�n'
			SET @v_error = 1
		END CATCH;
		
		--Construcci�n de sentencia de intercambio de partici�n
		SET @v_sql = N'ALTER TABLE ' + '[dbo].' + @v_source_tab_name + ' SWITCH PARTITION ' + CONVERT(VARCHAR(100), @v_partition_number) + ' TO ' + '[dbo].' + @v_target_tab_name + ' PARTITION ' + CONVERT(VARCHAR(100), @v_partition_number)
		
		PRINT CONVERT(VARCHAR(50), GETDATE()) + CHAR(9) + CHAR(9) + 'INFO: Sentencia DDL ' + @v_sql
	END
	
--Intercambio
IF @v_error = 0
	BEGIN
		BEGIN TRY  
		
			--Ejecuci�n de DDL
			EXEC(@v_sql)
			--Check de operaci�n realizada
			SELECT @v_count = COUNT(1)
			FROM etl_particiones
			WHERE name = @v_target_tab_name 
			AND table_schema = 'dbo'
			AND partition_number = @v_partition_number
			AND rows = @v_num_rows
		
			IF @v_count = 1
				PRINT CONVERT(VARCHAR(50), GETDATE()) + CHAR(9) + CHAR(9) + 'INFO: Se ha transferido la partici�n ' + CONVERT(VARCHAR(100), @v_partition_number) + ' a la tabla [dbo].' + @v_target_tab_name
		
		END TRY  
		BEGIN CATCH  
			PRINT CONVERT(VARCHAR(50), GETDATE()) + CHAR(9) + CHAR(9) + 'ERROR: No se ha podido ejecutar el intercambio de partici�n'
		END CATCH;
	END
END