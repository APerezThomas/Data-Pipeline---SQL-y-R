# 📊 **SQL & R Data Pipeline** 

**SQL** y **R**, construyendo un flujo de trabajo (pipeline):  
1. Recepción de archivos `.CSV`.
2. Carga en **SQL Server** mediante consultas y normalización de datos.
3. Envío de datos procesados a **R** usando `RODBC`.
4. Análisis y visualización en R con distintos **plots**  .
5. Limpieza y normalización de un segundo `.CSV` directamente en R.

---

## 🗂️ **Estructura del Proyecto**

## **Datos**
- Descomprimir el .rar *"Archivos"*.
- En esta carpeta se encuentran los datos que se van a manejar.
- Archivos en formato .csv.
```
-Divvy_Trips_2019_Q1.csv
-Divvy_Trips_2020_Q1.csv
```

---

## 📈 **SQL Server**

1. Ejecutar el Scrip `Crecion_base_de_datos.sql`. En este se creara toda la estructura y los componentes de la base de datos.
   

```sql
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
------------------CREAR ESQUEMA

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
```

2. Ejecutar el Scrip `funciones_procedures.sql`. En este se crearan los Stored Procedures que complemetan la importacion del archivo `Divvy_Trips_2020_Q1.csv` entre otras funciones complementarias.
   Considerar que para poder exportar el archivo `.csv` a SQL Server debes tener el proveedor [Microsoft.ACE.OLEDB](https://www.microsoft.com/es-es/download/details.aspx?id=54920).
3. Una vez hecho todo lo anterior se podra ir testeando con el script `Testing.sql`. El Script contiene la prueba de los Stored Procedure y ciertas consulta a la base de datos.


```sql
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

-------- Consulta 

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

```

- El Stored Procedure `esquema_Logistica.importar_y_procesar_movimientos_2020` recibe como parametro el `PATH` del archivo `Divvy_Trips_2020_Q1.csv` ejecuta como transaccion la normalizacion(antes de insertarlos en la bd) e importacion de los datos sin insertar duplicados.
- Una parte del testing, especificamente, aquella que verifica que se haya importado el archivo `Divvy_Trips_2020_Q1.csv` a la base de datos se testeara mas tarde.



```sql
-------------------------------------------------------PROCESAMIENTO DE DATOS 2019

IF NOT EXISTS(SELECT 1 FROM esquema_Datos_Importados.movimientos_Bicicletas_2019)
BEGIN

	--Se debe primero procesar el archivo donde se importan los datos desde RStudio a SQL
	SELECT * FROM esquema_Datos_Importados.movimientos_Bicicletas_2019;

	--Luego actualizar los datos de las tablas de la base de datos con los datos importados desde RStudio
	EXEC esquema_Logistica.procesar_movimientos_2019;

END
``` 

---

## 📈 **R**

1. Primero ejecutar la siguiente linea de codigo para descargar los paquetes que han sido utilizados en la demostracion.

```r
 install.packages(c("RODBC", "ggplot2", "dplyr", "lubridate","readxl","SimDesign"))
```
2. Establecer el directorio de trabajo (carpeta actual de tu computadora que R utiliza por defecto para buscar y guardar archivos, como datos para importar o resultados de análisis).

```r
 setwd(" C/TU/PATH" )
```
3. Ejecutar el Scrip `Importar_datos_2019.R`. Se impotara a un `dataframe` el contenido del archivo `Divvy_Trips_2019_Q1.csv`.

```r
 #-----------------------Importacion de los datos del archivo .CSV
datos_2019 <- read_csv("C:/Users/PC/Desktop/database/Archivos/Divvy_Trips_2019_Q1.csv")
```

4. Ejecutar el Script `Conexion_Con_SQL.R`. Se establecera la conexion con la base de datos de `SQL Server`, se realizara una consulta directa a la base de datos, retornando los datos consultados para su manejo en `R`.
Para mas detalle abrir el script.

```r

#Establezco conexion con la base de datos de SQL server
Cyclic_DB <- odbcDriverConnect("Driver={ODBC Driver 18 for SQL Server};Server=DESKTOP-RDG2MCM\\SQLEXPRESS;Database=Cyclistic_Database;Trusted_Connection=Yes;Encrypt=no;TrustServerCertificate=yes;")

#Consulta a la base de datos
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
```

5. En este mismo Scrip se realizara la exportacion de los datos cargados del archivo `Divvy_Trips_2019_Q1.csv` a la base de datos `Cyclistic_database`.

```r
# Guardo los datos importados del archivo 2019 en la base de datos SQL
sqlSave(Cyclic_DB, datos_a_importar2019 ,tablename = "esquema_Datos_Importados.movimientos_Bicicletas_2019",rownames = FALSE,append = TRUE)  
# Una vez importados los datos cierro conexion con la base de datos SQL
odbcClose(Cyclic_DB)
```
- En este punto se puede volver al tenting de `SQL` para ver los datos importados de `Divvy_Trips_2019_Q1.csv` en la base de datos.

## 📈 **Plots**

1. Una vez ejecutados los Scripts anteriores, los dataframes estan listos para generar `plots`.
2. Ejecutar el Script `Creacion_de_Plots.R`. Primero se toma una muestra(`sample`) de los datos importados para que no se saturen los plots por la masiva cantidad de datos.

```r

# Este ejemplo tambien filtra los datos por "tiempo_minutos"
sample <- datos_2020 %>% sample_n(3000) %>% filter(tiempo_minutos <180)

```

3. Se crean `plots` del los datos provenientes del archivo `Divvy_Trips_2020_Q1.csv`.
### *Ejemplos de codigo:* 
```r
#Diagrama de dispersion.

p<-ggplot(data=sample) +
  geom_point(mapping =  aes(x = dist_entre_estaciones_km, y = tiempo_minutos, colour = tipo_cliente,shape = tipo_cliente),alpha = 0.6) +
  geom_smooth(mapping =  aes(x = dist_entre_estaciones_km, y = tiempo_minutos),method = "lm", se = FALSE, color = "blue") +
  facet_wrap(~tipo_cliente)+
  labs(title = "Tiempo vs. Distancia - Por Tipo De Cliente", x = "Distancia (km)", y = "Tiempo (minutos)")
#guardar plot
ggsave("plot_Diagrama_de_Dispersion_2_2020.png", plot = p, width = 7, height = 5, dpi = 300)


# Mapa

p <- ggplot(data = viajes_top) +
  geom_bin2d(mapping = aes(x = nombre_estacion_final.x, y = nombre_estacion_comienzo.y)) +
  scale_fill_gradient(low = "lightblue", high = "darkblue") +
  labs(title = "Mapa de viajes entre estaciones top 10", x = "Estación inicio", y = "Estación final")+
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#guardar plot
ggsave("plot_Mapa_2020.png", plot = p, width = 7, height = 5, dpi = 300)

```
![imagen1](https://github.com/APerezThomas/Data-Pipeline---SQL-y-R/blob/main/Plots/plot_Diagrama_de_Dispersion_2_2020.png)
![imagen2](https://github.com/APerezThomas/Data-Pipeline---SQL-y-R/blob/main/Plots/plot_Mapa_2020.png)


4. Se crean `plots` del los datos provenientes del archivo `Divvy_Trips_2019_Q1.csv`.
### *Ejemplos de codigo:*

```r

# Diagrama de dispersion

p <- ggplot(data=sample)+
  geom_point(mapping = aes(x = tripduration/60, y = year(today())- birthyear, colour = gender,shape = gender ), alpha=0.6 )+
  geom_smooth(mapping = aes(x = tripduration/60, y = year(today())- birthyear), method="lm",color="blue")+
  facet_wrap(~gender~usertype)+
  labs(title = "Relacion entre duracion y edad de clientes",subtitle="Clientes menores de 80 años y duracion de recorrido menor a 30 minutos",x = "Duracion en minutos",y = "Edad de los clientes")
#guardar plot
ggsave("plot_Dispersion_2019.png", plot = p, width = 7, height = 5, dpi = 300)

# Histograma

p <- ggplot(data = sample)+
  geom_histogram(mapping = aes(x=tripduration/60,fill = gender),binwidth =1,color="black",alpha= 0.5)+
  labs(title = "Relacion entre duracion y frecuencia de clientes",subtitle="Clientes menores de 80 años y duracion de recorrido menor a 30 minutos",x = "Duracion en minutos",y = "Apariciones")+
  facet_wrap(~gender)+
  scale_x_continuous(breaks = seq(0, 30, by = 2)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#guardar plot
ggsave("plot_Histograma_2019.png", plot = p, width = 7, height = 5, dpi = 300)

```
![imagen3](https://github.com/APerezThomas/Data-Pipeline---SQL-y-R/blob/main/Plots/plot_Histograma_2019.png)
![imagen4](https://github.com/APerezThomas/Data-Pipeline---SQL-y-R/blob/main/Plots/plot_Dispersion_2019.png)

## 📝 **Notas Técnicas**

1.Se usó RODBC para transferir datos desde SQL Server a R.
2.La normalización se realiza en SQL antes de los plots exceptuando los datos `Divvy_Trips_2019_Q1.csv` que han sido importados en R para luego limpiarlos antes de generar los plots.
3.Se incluye limpieza y validación de datos para asegurar calidad.
4.Los CSV se cargan con BULK INSERT y funciones auxiliares.
