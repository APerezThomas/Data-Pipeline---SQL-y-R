
library("ggplot2")
library("tidyverse")
library("SimDesign")



#Establezco el directorio de trabajo

setwd("C:/Users/PC/Documents/programming/R")


#--------------------------------Creacion de plots proveniente de la consulta a SQL (DATOS_2020)


# Vista previa de los datos cargardos

head(datos_2020)
str(datos_2020)
colnames(datos_2020)

#Tomo una muestra mas pequeña de los datos

sample <- datos_2020 %>% sample_n(3000) %>% filter(tiempo_minutos <180)





#--------------------------Plots en relacion tiempo y distancia 


#Diagrama de dispersion.



p<-ggplot(data=na.omit(sample)) + 
  geom_point(mapping= aes(x=tiempo_minutos,y=dist_entre_estaciones_km),alpha=0.6,color= "blue")
#guardar plot
ggsave("plot_Diagrama_de_Dispersion_1_2020.png", plot = p, width = 7, height = 5, dpi = 300)





p<-ggplot(data=sample) +
  geom_point(mapping =  aes(x = dist_entre_estaciones_km, y = tiempo_minutos, colour = tipo_cliente,shape = tipo_cliente),alpha = 0.6) +
  geom_smooth(mapping =  aes(x = dist_entre_estaciones_km, y = tiempo_minutos),method = "lm", se = FALSE, color = "blue") +
  facet_wrap(~tipo_cliente)+
  labs(title = "Tiempo vs. Distancia - Por Tipo De Cliente", x = "Distancia (km)", y = "Tiempo (minutos)")
#guardar plot
ggsave("plot_Diagrama_de_Dispersion_2_2020.png", plot = p, width = 7, height = 5, dpi = 300)






#------------------------Plot, minutos y frecuencias por tipo de cliente



# Histograma 

p<-ggplot(data=sample) +
  geom_histogram(mapping=, aes(x = tiempo_minutos,fill = tipo_cliente),binwidth = 20, color = "black") +
  labs(title = "Distribución del tiempo de viaje", x = "Minutos", y = "Frecuencia")
#guardar plot
ggsave("plot_Histograma_2020.png", plot = p, width = 7, height = 5, dpi = 300)




# Diagrama de caja

p<-ggplot(data=sample) +
  geom_boxplot(mapping= aes(x = tipo_cliente, y = tiempo_minutos, fill = tipo_cliente)) +
  labs(title = "Tiempo de viaje por tipo de cliente", x = "Tipo de cliente", y = "Tiempo (minutos)")
#guardar plot
ggsave("plot_Diagrama_de_cajas_2020.png", plot = p, width = 7, height = 5, dpi = 300)




#-----------------------Plot en relacion a top 10 de estaciones de comienzo


# Mapa entre estaciones top 10

top_estaciones_inicio <- sample %>%
  count(id_estacion_comienzo,nombre_estacion_comienzo, sort = TRUE) %>%
  slice_max(n, n = 10)

top_estaciones_final <- sample %>%
  count(id_estacion_final,nombre_estacion_final, sort = TRUE) %>%
  slice_max(n, n = 10)

viajes_top <- sample %>% 
  inner_join(top_estaciones_inicio,by= "id_estacion_comienzo")%>%
  inner_join(top_estaciones_final,by = "id_estacion_final") %>%
  select(nombre_estacion_comienzo.y,nombre_estacion_final.x)

p <- ggplot(data = viajes_top) +
  geom_bin2d(mapping = aes(x = nombre_estacion_final.x, y = nombre_estacion_comienzo.y)) +
  scale_fill_gradient(low = "lightblue", high = "darkblue") +
  labs(title = "Mapa de viajes entre estaciones top 10", x = "Estación inicio", y = "Estación final")+
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#guardar plot
ggsave("plot_Mapa_2020.png", plot = p, width = 7, height = 5, dpi = 300)





# Grafico de barras

p <- ggplot(data = top_estaciones_inicio) +
  geom_col(mapping =aes(x = reorder(nombre_estacion_comienzo, n), y = n),fill = "orange") +
  coord_flip() +
  labs(title = "Top 10 estaciones de inicio", x = "Estación", y = "Cantidad de viajes")
#guardar plot
ggsave("plot_Grafico_de_barras_2020.png", plot = p, width = 7, height = 5, dpi = 300)

#Remuevo dataframe
rm(datos_2020)





#-------------------------------Creacion de plots provenientes del archivo .csv (DATOS_2019) 


# Vista previa de los datos cargardos

head(datos_2019)
str(datos_2019)
colnames(datos_2019)


# Tomo una muestra

sample <- datos_2019 %>%
  drop_na(gender,usertype)%>%
  filter(tripduration < 1800,year(today())- birthyear<80)%>%
  sample_n(2000)


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



#Remuevo dataframe 
rm(datos_2019)