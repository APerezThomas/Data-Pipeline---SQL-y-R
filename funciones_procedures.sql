USE master
GO

USE Cyclistic_Database
GO

--Este codigo es de SQL Server y sirve para habilitar la ejecución de consultas ad hoc distribuidas (como OPENROWSET o OPENDATASOURCE)

--EXEC sp_configure 'show advanced options', 1;
--RECONFIGURE;
--EXEC sp_configure 'Ad Hoc Distributed Queries', 1;
--RECONFIGURE;
--GO



--Stored Procedure que nos permite cargar los datos del archivo de la ruta de movimiento 2020
--Actualiza los datos en la base de datos
CREATE OR ALTER PROCEDURE esquema_Logistica.importar_y_procesar_movimientos_2020 (@ruta_archivo NVARCHAR(MAX))
AS
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION
				
				SET XACT_ABORT ON 
				SET NOCOUNT ON

				DECLARE @consulta NVARCHAR(MAX)
				DECLARE @existe_archivo int


				--Verifico si existe la ruta del archivo 
				SELECT @existe_archivo = T.file_exists
				FROM sys.dm_os_file_exists(@ruta_archivo) AS T
				

				IF @existe_archivo = 0
					BEGIN
						RAISERROR('El archivo CSV no existe en la ruta especificada.', 16, 1)
						RETURN;
					END
				ELSE
					BEGIN
						--creo la tabla temporal donde guardar el CSV
						CREATE TABLE #tabla_temporal(
							id_bicicleta VARCHAR(100),
							tipo_bicicleta VARCHAR(100),
							tiempo_de_comienzo VARCHAR(100),
							tiempo_de_final VARCHAR(100),
							nombre_estacion_comienzo VARCHAR(100),
							id_estacion_comienzo VARCHAR(100),
							nombre_estacion_final VARCHAR(100),
							id_estacion_final VARCHAR(100),
							latitud_comienzo VARCHAR(100),
							longitud_comienzo VARCHAR(100),
							latitud_final VARCHAR(100),
							longitud_final VARCHAR(100),
							tipo_cliente VARCHAR(100)
						);

						--uso SQL DINAMICO para generar el importe de los datos a la tabla temporal
						SET @consulta = N' 
						BULK INSERT #tabla_temporal 
						FROM ''' + @ruta_archivo + ''' 
						WITH ( 
							FIELDTERMINATOR = '','', 
							ROWTERMINATOR = ''0x0a'', 
							CODEPAGE = ''65001'',
							FIRSTROW = 2
						);';
						EXEC sp_executesql @consulta;--ejecuto la consulta dinamica
						


						--actualizo los datos de la tabla bicicletas
						--a su vez verifico que no hayan duplicados
						WITH CTE (id_bicicleta,tipo_bicicleta) AS (
							SELECT 
								TRIM(REPLACE(t.id_bicicleta,'"','')),
								t.tipo_bicicleta
							FROM #tabla_temporal AS t
							WHERE t.id_bicicleta IS NOT NULL 
							AND NOT EXISTS(SELECT 1 FROM esquema_Logistica.Bicicletas AS t1 WHERE t1.id_bicicleta = TRIM(REPLACE(t.id_bicicleta,'"','')))			
						)
						INSERT INTO esquema_Logistica.bicicletas(id_bicicleta,tipo_bicicleta)
						SELECT * FROM CTE;


						--actualizo los datos de la tabla Estaciones_Acoplamiento
						--a su vez verifico que no hayan duplicados
						WITH CTE (id_estacion,nombre,latitud,logintud) AS (
							SELECT DISTINCT
								TRY_CAST(t.id_estacion_comienzo AS INT),
								t.nombre_estacion_comienzo,
								TRY_CAST(t.latitud_comienzo AS DECIMAL(10,6)),
								TRY_CAST(t.longitud_comienzo AS DECIMAL(10,6))
							FROM #tabla_temporal AS t
							WHERE t.id_estacion_comienzo IS NOT NULL 
								AND NOT EXISTS(SELECT 1 FROM esquema_Logistica.Estaciones_Acoplamiento AS t1 WHERE t1.id_estacion = TRY_CAST(t.id_estacion_comienzo AS INT) )
						)
						INSERT INTO esquema_Logistica.Estaciones_Acoplamiento(id_estacion,nombre,latitud,logintud)
						SELECT * FROM CTE;


						WITH CTE (id_estacion,nombre,latitud,logintud) AS (
	
							SELECT DISTINCT
								TRY_CAST(t.id_estacion_final AS INT),
								t.nombre_estacion_final,
								TRY_CAST(t.latitud_final AS DECIMAL(10,6)),
								TRY_CAST(t.longitud_final AS DECIMAL(10,6))
							FROM #tabla_temporal AS t
							WHERE t.id_estacion_final IS NOT NULL 
								AND NOT EXISTS(SELECT 1 FROM esquema_Logistica.Estaciones_Acoplamiento AS t1 WHERE t1.id_estacion = TRY_CAST(t.id_estacion_final AS INT) )
						)
						INSERT INTO esquema_Logistica.Estaciones_Acoplamiento(id_estacion,nombre,latitud,logintud)
						SELECT * FROM CTE;


						INSERT INTO esquema_Datos_Importados.movimientos_Bicicletas_2020(id_bicicleta,tipo_bicicleta ,tiempo_de_comienzo ,tiempo_de_final ,
							id_estacion_comienzo ,nombre_estacion_comienzo,id_estacion_final,nombre_estacion_final,latitud_comienzo,
							longitud_comienzo,latitud_final,longitud_final,tipo_cliente)
						SELECT 
							t.id_bicicleta,
							t.tipo_bicicleta,
							TRY_CAST(t.tiempo_de_comienzo AS DATETIME),
							TRY_CAST(t.tiempo_de_final AS DATETIME),
							TRY_CAST(t.id_estacion_comienzo AS INT),
							t.nombre_estacion_comienzo,
							TRY_CAST(t.id_estacion_final AS INT),
							t.nombre_estacion_final,
							TRY_CAST(t.latitud_comienzo AS DECIMAL(10,6)),
							TRY_CAST(t.longitud_comienzo AS DECIMAL(10,6)),
							TRY_CAST(t.latitud_final AS DECIMAL(10,6)),
							TRY_CAST(t.longitud_final AS DECIMAL(10,6)),
							TRIM(REPLACE(REPLACE(t.tipo_cliente, CHAR(13), ''),'"',''))
						FROM #tabla_temporal AS t;


						
					END
			COMMIT TRANSACTION
		END TRY
		BEGIN CATCH

			PRINT 'No se pudieron obtener los datos de la ruta: ' + @ruta_archivo ;
			DECLARE @error_message NVARCHAR(MAX)
			DECLARE @error_severety INT
			DECLARE @error_state INT

			SET @error_message = ERROR_MESSAGE()
			SET @error_severety = ERROR_SEVERITY()
			SET @error_state = ERROR_STATE()

			RAISERROR(@error_message,@error_severety,@error_state);
			
			ROLLBACK TRANSACTION
		END CATCH
	END
GO

CREATE OR ALTER PROCEDURE esquema_Logistica.procesar_movimientos_2019 
AS
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION
				
				SET XACT_ABORT ON 
				SET NOCOUNT ON

				IF NOT EXISTS(SELECT 1 FROM esquema_Datos_Importados.movimientos_Bicicletas_2019)
					BEGIN
						RAISERROR('La tabla "movimientos_Bicicletas_2019" no tiene registros', 16, 1)
						RETURN;
					END
				ELSE
					BEGIN

						--actualizo los datos de la tabla bicicletas
						--a su vez verifico que no hayan duplicados
						WITH CTE (id_bicicleta) AS (
							SELECT DISTINCT
								TRIM(REPLACE(REPLACE(t.id_bicicleta, CHAR(13), ''),'"',''))
							FROM esquema_Datos_Importados.movimientos_Bicicletas_2019 AS t
							WHERE t.id_bicicleta IS NOT NULL 
							AND NOT EXISTS(SELECT 1 FROM esquema_Logistica.Bicicletas AS t1 WHERE t1.id_bicicleta = TRIM(REPLACE(t.id_bicicleta,'"','')))			
						)
						INSERT INTO esquema_Logistica.bicicletas(id_bicicleta)
						SELECT * FROM CTE;


						--actualizo los datos de la tabla Estaciones_Acoplamiento
						--a su vez verifico que no hayan duplicados
						WITH CTE (id_estacion,nombre) AS (
							SELECT DISTINCT
								TRY_CAST(t.id_estacion_comienzo AS INT),
								t.nombre_estacion_comienzo
							FROM esquema_Datos_Importados.movimientos_Bicicletas_2019 AS t
							WHERE t.id_estacion_comienzo IS NOT NULL 
								AND NOT EXISTS(SELECT 1 FROM esquema_Logistica.Estaciones_Acoplamiento AS t1 WHERE t1.id_estacion = TRY_CAST(t.id_estacion_comienzo AS INT) )
						)
						INSERT INTO esquema_Logistica.Estaciones_Acoplamiento(id_estacion,nombre)
						SELECT * FROM CTE;

						WITH CTE (id_estacion,nombre) AS (
							SELECT DISTINCT
								TRY_CAST(t.id_estacion_final AS INT),
								t.nombre_estacion_final
							FROM esquema_Datos_Importados.movimientos_Bicicletas_2019 AS t
							WHERE t.id_estacion_final IS NOT NULL 
								AND NOT EXISTS(SELECT 1 FROM esquema_Logistica.Estaciones_Acoplamiento AS t1 WHERE t1.id_estacion = TRY_CAST(t.id_estacion_final AS INT) )
						)
						INSERT INTO esquema_Logistica.Estaciones_Acoplamiento(id_estacion,nombre)
						SELECT * FROM CTE;

						
					END
			COMMIT TRANSACTION
		END TRY
		BEGIN CATCH

			PRINT 'No se pudo procesar los datos 2019 ';
			DECLARE @error_message NVARCHAR(MAX)
			DECLARE @error_severety INT
			DECLARE @error_state INT

			SET @error_message = ERROR_MESSAGE()
			SET @error_severety = ERROR_SEVERITY()
			SET @error_state = ERROR_STATE()

			RAISERROR(@error_message,@error_severety,@error_state);
			
			ROLLBACK TRANSACTION
		END CATCH
	END
GO



--Funcion para obtener la distancia entre dos puntos geograficos
CREATE OR ALTER FUNCTION esquema_Logistica.Funcion_Obtener_Distancia_Entre_Estaciones
(
    @lat1 FLOAT, @lon1 FLOAT,
    @lat2 FLOAT, @lon2 FLOAT
)
RETURNS FLOAT
AS
BEGIN
    DECLARE @R FLOAT = 6371000.0;
    DECLARE @dLat FLOAT = RADIANS(@lat2 - @lat1);
    DECLARE @dLon FLOAT = RADIANS(@lon2 - @lon1);
    DECLARE @a FLOAT =SIN(@dLat / 2.0) * SIN(@dLat / 2.0) +COS(RADIANS(@lat1)) * COS(RADIANS(@lat2)) *SIN(@dLon / 2.0) * SIN(@dLon / 2.0);
    DECLARE @c FLOAT = 2.0 * ATN2(SQRT(@a), SQRT(1 - @a));
    RETURN @R * @c;
END;
