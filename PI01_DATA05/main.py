from fastapi import FastAPI
import pandas as pd
from funciones import get_count, get_max, get_actormax, get_gen

app= FastAPI(title="PI05 -  Data Engineering - Ricardo Talavera")




@app.get("/")
async def read_root():
        return("API  Consulta de Datos de Peliculas y Series... / Plataformas =[Netflix, Amazon-Prime, Hulu, Disney]")


#Máxima duración según tipo de film (película/serie), por plataforma y por año: 
# El request debe ser: get_max_duration(año, plataforma, [min o season])
@app.get("/get_max_duration/{year},{plataforma},{unidad_tipo}")
async def get_max_duration(year:int,plataforma:str ,unidad_tipo:str):
          return(get_max(year,plataforma,unidad_tipo))          
          


#Cantidad de películas y series (separado) por plataforma El request debe ser: get_count_plataform

@app.get("/get_count_plataform)/{plataforma}")
async def get_count_plataform(plataforma:str):
          return(get_count(plataforma))


#Actor que más se repite según plataforma y año. El request debe ser: get_actor(plataforma, año)
@app.get("/get_actor/{year},{plataforma}")
async def get_actor(year:int,plataforma:str):
          return(get_actormax(year,plataforma))



#Cantidad de veces que se repite un género y plataforma con mayor frecuencia del mismo. El request debe ser: get_listedin('genero')
#Como ejemplo de género pueden usar 'comedy', el cuál deberia devolverles un cunt de 2099 para la plataforma de amazon.
@app.get("/get_listedin/{genero}")
async def get_listedin(genero:str):
          return(get_gen(genero))


