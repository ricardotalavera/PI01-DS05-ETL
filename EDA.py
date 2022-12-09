# %%

import pandas as pd
import numpy as np
import os
from IPython.display import clear_output
from tqdm import tqdm
import warnings

# %%
#Aseguramos directorio de trabajo y quitamos warnings
warnings.filterwarnings("ignore")
os.chdir("C:/Users/Ricardo/Desktop/Cursos Varios/")
os.chdir("Henry/Henry Carrera/")
os.chdir("PI1/PI01_DATA05/")

# %%
#Ordenamos la lista de archivos para mas modularidad mas tarde puede ingresarse esta lista por el usuario
files = ["amazon_prime_titles.csv","disney_plus_titles.csv","hulu_titles.csv","netflix_titles.json"]
files=np.array(files)
data = pd.DataFrame()


# %%
#Empezamos cargando los archivos segun la extensión, revisamos si hay duplicados a nivel de Todo un registro si es que hubiern, se remueven
#Luego empezamos a analizar cada columna de los dataframes, los cales ya se revisaron tienen el mismo formato en pandas ya leidos.



for  j,file in enumerate(files):

    extension = file[(len(file)-3):len(file)]
    path = "Datasets/"+file
    print("Procesando archivo ... ",file)

    if extension == "csv":
        
        plat = file[0:(len(file)-11)]
        df= pd.read_csv(path)
        print("Extension actual del archivo...",df.shape)
        duplicados = df.duplicated().sum()
        if duplicados > 0:
            print("Extrayendo registros duplicados")
            df.drop_duplicates(inplace=True)
            print("Nueva Extension del archivo...",df.shape)

         
    if extension == "son":
        plat = file[0:(len(file)-12)]
        df= pd.read_json(path)
        print("Extension actual del archivo...",df.shape)
        duplicados = df.duplicated().sum()
        if duplicados > 0:
            print("Extrayendo registros duplicados")
            df.drop_duplicates(inplace=True)
            print("Nueva Extension del archivo...",df.shape)

    print("Situacion encontrada en Nas : ",df.isna().sum().sum())
   
   #Trabajo con los Nas por cada columna
   
    df["director"].fillna("Sin datos del Director",inplace=True)
    df["cast"].fillna("Sin datos del cast",inplace=True)
    df["country"].fillna("Sin datos del Pais",inplace=True)
    df["date_added"].fillna("January 1, 1900",inplace=True) #Colocamos este fill porque luego puede desearse trabajar con fechas
    df["rating"].fillna("Sin rating",inplace=True)
    df["duration"].fillna("0 min",inplace=True) #Colocamos este fill para facilitar luego la transformacion a numeros
    df["description"].fillna("Sin información",inplace=True) #Colocamos este fill para facilitar luego la transformacion a numeros
    df["listed_in"].fillna("Sin genero",inplace=True) #Colocamos este fill por si acaso en futuras cargas figuraen NAs en este campo

    print(" Nas Neutralizados ")
    
    #Creando nueva columna de plataforma

    df["Plataforma"]= plat

    #Uniformizando la duracion y creando nuevas columnas a este respecto
    df["duration_"]=0
    df["dur_unidad"]="Sin Unidades"

    loop = tqdm(total = len(df.iloc[:,1]),position = 0,leave = False)

    for j,i in enumerate(df["show_id"]):
        loop.set_description("Transforming columna duration... ".format(j))
        loop.update(1)
        df["duration"][j]=df["duration"][j].strip()
        df["duration"][j]=df["duration"][j].replace("Seasons","Season")
        particion = df["duration"][j].split()
        df["duration_"][j]=int(particion[0])
        if int(particion[0]) == 0 :
            if df["type"][j]=="TV Show":
                df["dur_unidad"][j]="Season"
            else:
                pass
        else:
            df["dur_unidad"][j]=particion[1]

    loop.close()

    #Eliminando a la vieja duration
    df.drop(["duration"], axis=1, inplace=True)

    #Uniendo el dataframe procesado al dataset general
    data = data.append(df)
    print("Size dataset union : ",data.shape,"  Size de dataframe ",plat,df.shape)
    print("**************************************************************************")

  

# %%
data.reset_index(inplace=True)
print("**************************************************************************")
print("Se genero archivo unificado : data.csv")

# %%
cast = pd.DataFrame()

cast["actor"]=None
cast["pelicula"]=None

# %%
print("Generando tabla de actores - películas y tabla de género - películas")

actores_pelis=[]
genero_pelis=[]

indice = -1
for i,j in enumerate(data["show_id"]):
    if data["cast"][i]!= "Sin datos del cast":
        lista = data["cast"][i].split(",")
        for s,k in enumerate(lista):
            indice+=1
            actores_pelis.append([lista[s].strip().capitalize(),data["title"][i]])

actor_peli = pd.DataFrame(actores_pelis,columns=["cast","title"])
actor_peli.shape  

indice = -1
for i,j in enumerate(data["show_id"]):
    if data["listed_in"][i]!= "Sin genero":
        lista = data["listed_in"][i].split(",")
        for s,k in enumerate(lista):
            indice+=1
            genero_pelis.append([lista[s],data["title"][i]])

genero_peli = pd.DataFrame(genero_pelis,columns=["genero","title"])
   
        
# %% Generamos dos archivos de data central uno para el proceso de carga y otro para la API,
#tengamos en cuenta que esto se aplica para el archivo data. Las tablas de actores y generos van
#de manera similar tanto para la API como para el loading futuro dentro del funnel.
print("Eliminando duplicados y seleccionando columnas relevantes de consultas en API ...")
actor_peli.drop_duplicates(inplace=True)
genero_peli.drop_duplicates(inplace=True)
data.drop(["cast"], axis=1, inplace=True)
data.drop(["index"], axis=1, inplace=True)
data2=data[["type","title","release_year","Plataforma","duration_","dur_unidad"]]

# %%
genero_peli.reset_index(inplace=True)
for i,j in enumerate(genero_peli["title"]):
    genero_peli["genero"][i]=genero_peli["genero"][i].strip()

actor_peli.to_csv("actor_peli.csv")
genero_peli.to_csv("genero_peli.csv")
data.to_csv("data_api.csv")
data.drop(["listed_in"], axis=1, inplace=True)
data2.to_csv("data.csv")