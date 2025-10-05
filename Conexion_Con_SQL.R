
library("RODBC")
library("ggplot2")



#-------------------------------------------IMPORTAR LOS DATOS DE SQL (DATOS_2020)

#Establezco conexion con la base de datos de SQL server

Cyclic_DB <- odbcDriverConnect("Driver={ODBC Driver 18 for SQL Server};Server=DESKTOP-RDG2MCM\\SQLEXPRESS;Database=Cyclistic_Database;Trusted_Connection=Yes;Encrypt=no;TrustServerCertificate=yes;")

?odbcDriverConnect



#Detalles de la conexion

Cyclic_DB


#Importo los datos de la tabla de datos 2020

datos_2020 <- sqlFetch(Cyclic_DB,"esquema_Datos_Importados.movimientos_Bicicletas_2020")

# O Realizo una consulta

datos_2020 <- sqlQuery(Cyclic_DB,"WITH CTE (id_estacion_comienzo,id_estacion_final,tiempo_minutos,id_bicicleta,tipo_bicicleta,nombre_estacion_comienzo,nombre_estacion_final,dist_entre_estaciones_km,tipo_cliente) AS (
	SELECT 
	  t.id_estacion_comienzo,
	  t.id_estacion_final,
		DATEDIFF(MINUTE, t.tiempo_de_comienzo, t.tiempo_de_final) AS minutos,
		t.id_bicicleta,
		t.tipo_bicicleta,
		t.nombre_estacion_comienzo,
		t.nombre_estacion_final,
		esquema_Logistica.Funcion_Obtener_Distancia_Entre_Estaciones(t.latitud_comienzo,t.longitud_comienzo,t.latitud_final,t.longitud_final)/1000 AS dist_entre_estaciones_km,
		t.tipo_cliente
	FROM esquema_Datos_Importados.movimientos_Bicicletas_2020 AS t
)
SELECT * FROM CTE;")


# Vista previa de los datos importados

head(data_2020)
str(data_2020)
colnames(data_2020)





#-----------------------------------------IMPORTAR DATOS A SQL server

# Una vez procesado el script "importar_datos_2019.R" donde se importan los datos del .CSV que contienen los datos del 2019
# Cambio el tipo de los datos importados del .CSV para que sean compatibles con la tabla donde seran importados en SQLs

datos_a_importar2019 <- datos_2019 %>%
  transmute(
    id_recorrido = as.integer(trip_id),
    tiempo_de_comienzo = as.POSIXct(start_time),
    tiempo_de_final = as.POSIXct(end_time),
    id_bicicleta = as.character(bikeid),
    duracion_minutos = as.integer(tripduration),
    id_estacion_comienzo = as.integer(from_station_id),
    nombre_estacion_comienzo = as.character(from_station_name),
    id_estacion_final = as.integer(to_station_id),
    nombre_estacion_final = as.character(to_station_name),
    tipo_cliente = as.character(usertype),
    genero = as.character(gender),
    año_cumpleaños = as.integer(birthyear)
  )

str(datos_a_importar2019)

# Guardo los datos importados del archivo 2019 en la base de datos SQL
sqlSave(Cyclic_DB, datos_a_importar2019 ,tablename = "esquema_Datos_Importados.movimientos_Bicicletas_2019",rownames = FALSE,append = TRUE)  



#remuevo el dataframe 

rm(datos_a_Importar2019)



#Una vez importados los datos cierro conexion con la base de datos SQL

odbcClose(Cyclic_DB)




