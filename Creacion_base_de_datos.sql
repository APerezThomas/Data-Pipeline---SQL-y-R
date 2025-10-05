
USE MASTER
GO


------------------CREAR BASE DE DATOS
IF EXISTS( SELECT 1 FROM sys.databases WHERE name = 'Cyclistic_Database')
	BEGIN
		print 'La base de datos ya existe';
	END
ElSE
	BEGIn
		CREATE DATABASE Cyclistic_Database;
		print 'LA base de datos ha sido creada';
	END
GO

USE Cyclistic_Database
GO




------------------CREAR ESQUEMAS

IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'esquema_Logistica')
	BEGIN
		print 'El esquema ya existe';
	END
ElSE
	BEGIn
		EXEC('CREATE SCHEMA esquema_Logistica');
		print 'El esquema ha sido creada';
	END
GO


IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'esquema_Datos_Importados')
	BEGIN
		print 'El esquema ya existe';
	END
ElSE
	BEGIn
		EXEC('CREATE SCHEMA esquema_Datos_Importados');
		print 'El esquema ha sido creada';
	END
GO






------------------ELIMINAR TABLAS

DROP TABLE esquema_Datos_Importados.movimientos_Bicicletas_2020
GO

DROP TABLE esquema_Datos_Importados.movimientos_Bicicletas_2019
GO

DROP TABLE esquema_Logistica.Bicicletas
GO

DROP TABLE esquema_Logistica.estaciones_Acoplamiento
GO





------------------CREAR TABLAS 

IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'esquema_Logistica' AND TABLE_NAME = 'estaciones_Acoplamiento')
	BEGIN
		print 'La tabla "estaciones_Acoplamiento" ya existe';
	END
ElSE
	BEGIn
		CREATE TABLE esquema_Logistica.Estaciones_Acoplamiento (
		id_estacion INT NOT NULL,
		nombre VARCHAR(100),
		latitud DECIMAL(10,6),
		logintud DECIMAL(10,6),
		constraint pk_estacion primary key(id_estacion)
		);
		print 'La tabla "Estaciones_Acoplamiento" ha sido creada';
	END
GO


IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'esquema_Logistica' AND TABLE_NAME = 'Bicicletas')
	BEGIN
		print 'La tabla "bicicletas" ya existe';
	END
ElSE
	BEGIn
		CREATE TABLE esquema_Logistica.Bicicletas (
		id_bicicleta VARCHAR(100) NOT NULL,
		tipo_bicicleta VARCHAR(50)
		constraint pk_bicicleta primary key (id_bicicleta)
		);
		print 'La tabla "bicicletas" ha sido creada';
	END
GO

IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'esquema_Datos_Importados' AND TABLE_NAME = 'movimientos_Bicicletas_2020')
	BEGIN
		print 'La tabla "movimientos_Bicicletas_2020" ya existe';
	END
ElSE
	BEGIn
		CREATE TABLE esquema_Datos_Importados.movimientos_Bicicletas_2020 (
		id_movimientos_b int Identity(1,1) NOT NULL,
		id_bicicleta VARCHAR(100),
		tipo_bicicleta VARCHAR(50),
		tiempo_de_comienzo DATETIME,
		tiempo_de_final DATETIME,
		id_estacion_comienzo INT,
		nombre_estacion_comienzo VARCHAR(100),
		id_estacion_final INT,
		nombre_estacion_final VARCHAR(100),
		latitud_comienzo DECIMAL(10,6),
		longitud_comienzo DECIMAL(10,6),
		latitud_final DECIMAL(10,6),
		longitud_final DECIMAL(10,6),
		tipo_cliente VARCHAR(20),
		constraint pk_movimientos_b20 primary key (id_movimientos_b),
		constraint fk_bicibletas20 foreign key (id_bicicleta) references esquema_Logistica.bicicletas (id_bicicleta),
		constraint fk_id_est_comienzo20 foreign key (id_estacion_comienzo) references esquema_Logistica.estaciones_Acoplamiento (id_estacion),
		constraint fk_id_est_final20 foreign key (id_estacion_final) references esquema_Logistica.estaciones_Acoplamiento (id_estacion),
		);
		print 'La tabla "movimientos_Bicicletas_2020" ha sido creada';
	END
GO


IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'esquema_Datos_Importados' AND TABLE_NAME = 'movimientos_Bicicletas_2019')
	BEGIN
		print 'La tabla "movimientos_Bicicletas_2019" ya existe';
	END
ElSE
	BEGIN
		CREATE TABLE esquema_Datos_Importados.movimientos_Bicicletas_2019 (
		id_recorrido int NOT NULL,
		tiempo_de_comienzo DATETIME,
		tiempo_de_final DATETIME,
		id_bicicleta VARCHAR(100),
		duracion_minutos INT,
		id_estacion_comienzo INT,
		nombre_estacion_comienzo VARCHAR(100),
		id_estacion_final INT,
		nombre_estacion_final VARCHAR(100),
		tipo_cliente VARCHAR(20),
		genero VARCHAR(10),
		año_cumpleaños INT
		);
		print 'La tabla "movimientos_Bicicletas_2019" ha sido creada';
	END
GO
