#Libreria para importar datos desde .xls o .csv

library(readxl)

#Otras
library(tidyverse)
library(lubridate)
library(dplyr)



#-----------------------Importacion de los datos del archivo .CSV

datos_2019 <-read_csv("C:/Users/PC/Desktop/database/Archivos/Divvy_Trips_2019_Q1.csv")



# Cambio el tipo de dato a datetime

datos_2019$start_time <- as.POSIXct(datos_2019$start_time,format = "%Y-%m-%d %H:%M:%S")
datos_2019$end_time <- as.POSIXct(datos_2019$end_time,format = "%Y-%m-%d %H:%M:%S")



# Vista previa de los datos cargardos

head(datos_2019)
str(datos_2019)
colnames(datos_2019)







