
USE Cyclistic_Database
GO


----------------------------------------------Importar datos .csv

EXEC esquema_Logistica.importar_y_procesar_movimientos_2020 @ruta_archivo = 'C:\Users\PC\Desktop\database\Archivos\Divvy_Trips_2020_Q1.csv'
GO




---------------------------------Muestro los datos importados en la base de datos

SELECT * FROM esquema_Logistica.Estaciones_Acoplamiento
GO
SELECT * FROM esquema_Logistica.Bicicletas
GO
SELECT * FROM esquema_Datos_Importados.movimientos_Bicicletas_2020 
GO



--------------------------------------------------CONSULTA SQL

-------- Creo un CTE para calcular tiempo de recorrido y distancia entre estaciones de acoplamientos para bicicletas

WITH CTE (tiempo_minutos,id_bicicleta,tipo_bicicleta,nombre_estacion_comienzo,nombre_estacion_final,dist_entre_estaciones_km,tipo_cliente) AS (

	SELECT 
		DATEDIFF(MINUTE, t.tiempo_de_comienzo, t.tiempo_de_final) AS minutos,
		t.id_bicicleta,
		t.tipo_bicicleta,
		t.nombre_estacion_comienzo,
		t.nombre_estacion_final,
		esquema_Logistica.Funcion_Obtener_Distancia_Entre_Estaciones(t.latitud_comienzo,t.longitud_comienzo,t.latitud_final,t.longitud_final)/1000 AS dist_entre_estaciones_km,
		t.tipo_cliente
	FROM esquema_Datos_Importados.movimientos_Bicicletas_2020 AS t
)
SELECT * FROM CTE
GO




------ Consulto los la cantidad de clientes de cada tipo

WITH CTE (tiempo_minutos,id_bicicleta,tipo_bicicleta,nombre_estacion_comienzo,nombre_estacion_final,dist_entre_estaciones_km,tipo_cliente) AS (

	SELECT 
		DATEDIFF(MINUTE, t.tiempo_de_comienzo, t.tiempo_de_final) AS minutos,
		t.id_bicicleta,
		t.tipo_bicicleta,
		t.nombre_estacion_comienzo,
		t.nombre_estacion_final,
		esquema_Logistica.Funcion_Obtener_Distancia_Entre_Estaciones(t.latitud_comienzo,t.longitud_comienzo,t.latitud_final,t.longitud_final)/1000 AS dist_entre_estaciones_km,
		t.tipo_cliente
	FROM esquema_Datos_Importados.movimientos_Bicicletas_2020 AS t
)
SELECT COUNT(tipo_cliente) AS cantidad, tipo_cliente 
FROM CTE 
GROUP BY tipo_cliente
GO






-------- Consulto el tiempo de recorrido promedio por mes de cada estacion de comienzo 

WITH CTE (mes,tiempo_minutos,id_bicicleta,tipo_bicicleta,nombre_estacion_comienzo,nombre_estacion_final,dist_entre_estaciones_km,tipo_cliente) AS (

	SELECT 
		DATEPART(MONTH,t.tiempo_de_comienzo) AS mes,
		DATEDIFF(MINUTE, t.tiempo_de_comienzo, t.tiempo_de_final) AS tiempo_minutos,
		t.id_bicicleta,
		t.tipo_bicicleta,
		t.nombre_estacion_comienzo,
		t.nombre_estacion_final,
		esquema_Logistica.Funcion_Obtener_Distancia_Entre_Estaciones(t.latitud_comienzo,t.longitud_comienzo,t.latitud_final,t.longitud_final)/1000 AS dist_entre_estaciones_km,
		t.tipo_cliente
	FROM esquema_Datos_Importados.movimientos_Bicicletas_2020 AS t
)
SELECT mes,nombre_estacion_comienzo,AVG(tiempo_minutos) as promedio_minutos
FROM CTE 
GROUP BY nombre_estacion_comienzo, mes
HAVING AVG(tiempo_minutos)/60 >=1
ORDER BY nombre_estacion_comienzo,mes DESC
GO




------ Consulto el tiempo total por tipo de cliente, mes y estacion de comienzo 

WITH CTE (mes,tiempo_minutos,id_bicicleta,tipo_bicicleta,nombre_estacion_comienzo,nombre_estacion_final,dist_entre_estaciones_km,tipo_cliente) AS (

	SELECT 
		DATENAME(MONTH,t.tiempo_de_comienzo) AS mes,
		DATEDIFF(MINUTE, t.tiempo_de_comienzo, t.tiempo_de_final) AS tiempo_minutos,
		t.id_bicicleta,
		t.tipo_bicicleta,
		t.nombre_estacion_comienzo,
		t.nombre_estacion_final,
		esquema_Logistica.Funcion_Obtener_Distancia_Entre_Estaciones(t.latitud_comienzo,t.longitud_comienzo,t.latitud_final,t.longitud_final)/1000 AS dist_entre_estaciones_km,
		t.tipo_cliente
	FROM esquema_Datos_Importados.movimientos_Bicicletas_2020 AS t
)
SELECT tipo_cliente,mes,nombre_estacion_comienzo,SUM(tiempo_minutos) as minutos_x_mes
FROM CTE 
GROUP BY nombre_estacion_comienzo, mes, tipo_cliente
ORDER BY nombre_estacion_comienzo,mes DESC
GO





-------- Consulta del promedio de tiempo recorrido por mes para cada tipo_cliente

WITH CTE (dia,mes,tiempo_minutos,id_bicicleta,tipo_bicicleta,nombre_estacion_comienzo,nombre_estacion_final,dist_entre_estaciones_km,tipo_cliente) AS (

	SELECT 
		DATENAME(DAY,t.tiempo_de_comienzo) AS dia,
		DATEPART(MONTH,t.tiempo_de_comienzo) AS mes,
		DATEDIFF(MINUTE, t.tiempo_de_comienzo, t.tiempo_de_final) AS tiempo_minutos,
		t.id_bicicleta,
		t.tipo_bicicleta,
		t.nombre_estacion_comienzo,
		t.nombre_estacion_final,
		esquema_Logistica.Funcion_Obtener_Distancia_Entre_Estaciones(t.latitud_comienzo,t.longitud_comienzo,t.latitud_final,t.longitud_final)/1000 AS dist_entre_estaciones_km,
		t.tipo_cliente
	FROM esquema_Datos_Importados.movimientos_Bicicletas_2020 AS t
)
SELECT tipo_cliente,mes AS mes,SUM(tiempo_minutos)/30 as minutos_x_mes
FROM CTE 
GROUP BY  mes, tipo_cliente
ORDER BY mes DESC
GO


--------Consulta del promedio de tiempo recorrido por mes para cada tipo_cliente

WITH CTE (dia,mes,tiempo_minutos,id_bicicleta,tipo_bicicleta,nombre_estacion_comienzo,nombre_estacion_final,dist_entre_estaciones_km,tipo_cliente) AS (

	SELECT 
		DATENAME(DAY,t.tiempo_de_comienzo) AS dia,
		DATEPART(MONTH,t.tiempo_de_comienzo) AS mes,
		DATEDIFF(MINUTE, t.tiempo_de_comienzo, t.tiempo_de_final) AS tiempo_minutos,
		t.id_bicicleta,
		t.tipo_bicicleta,
		t.nombre_estacion_comienzo,
		t.nombre_estacion_final,
		esquema_Logistica.Funcion_Obtener_Distancia_Entre_Estaciones(t.latitud_comienzo,t.longitud_comienzo,t.latitud_final,t.longitud_final)/1000 AS dist_entre_estaciones_km,
		t.tipo_cliente
	FROM esquema_Datos_Importados.movimientos_Bicicletas_2020 AS t
)
SELECT tipo_cliente,mes,SUM(dist_entre_estaciones_km)/30 AS promedio_recorrido_x_mes
FROM CTE 
GROUP BY  mes, tipo_cliente
ORDER BY mes DESC
GO






-------------------------------------------------------PROCESAMIENTO DE DATOS 2019


IF NOT EXISTS(SELECT 1 FROM esquema_Datos_Importados.movimientos_Bicicletas_2019)
BEGIN

	--Se debe primero procesar el archivo donde se importan los datos desde RStudio a SQL
	SELECT * FROM esquema_Datos_Importados.movimientos_Bicicletas_2019;

	--Luego actualizar los datos de las tablas de la base de datos con los datos importados desde RStudio
	EXEC esquema_Logistica.procesar_movimientos_2019;

END
GO