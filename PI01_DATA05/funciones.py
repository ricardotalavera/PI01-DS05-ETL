import pandas as pd


def get_count(plataforma):
    kernel=pd.read_csv("data_api.csv")
    kernel["Total"]=0
    s=kernel[kernel["Plataforma"]==plataforma][["type","Plataforma","Total"]].groupby(["type","Plataforma"]).count()
    salida = "[Plataforma] : "+plataforma+"    [ Tipo ] : Movie   [ cantidad ] : " + str(s.iloc[0][0]) + "   [ Tipo ] : TV Show   [ cantidad ] : " + str(s.iloc[1][0])
    return(salida)

def get_max(year,plataforma,unidad_tipo):
    kernel=pd.read_csv("data_api.csv")
    s=kernel[(kernel["Plataforma"]==plataforma) & (kernel["release_year"]==year) & (kernel["dur_unidad"]==unidad_tipo)].sort_values(by="duration_",ascending=False).head(1)[["title","duration_","dur_unidad"]]
    titulo=str(s.iloc[0][0])
    duracion=str(s.iloc[0][1])
    anio=str(year)
    if unidad_tipo == "min":
        tip="Movie"
    if unidad_tipo == "season":
        tip="TV Show"
    salida = "[ Plataforma ] : " + plataforma + "   [ Tipo ] : " + tip  + "   [ año ] : " + anio + "   [ Movie / Serie ] :" + titulo +"  [ Duracion ] : " + duracion + " " + unidad_tipo
    return(salida)

def get_actormax(year,plataforma):
    kernel=pd.read_csv("data_api.csv")
    actores=pd.read_csv("actor_peli.csv")
    pelis=kernel[(kernel["Plataforma"]==plataforma) & (kernel["release_year"]==year)]["title"]
    anio=str(year)
    mix= pd.merge(actores[["cast","title"]],pelis,on="title", how="inner").groupby(["cast"]).count().sort_values(by="title",ascending=False).head(5)
    s=mix.index[0].strip()
    salida = "[ Plataforma ] : " + plataforma + "   [ año ] : " + anio + "   [ Actor ] : "+s
    return(salida)
    
def get_gen(gen):
    kernel=pd.read_csv("data_api.csv")
    kernel["contain"]=kernel["listed_in"].str.contains(gen)
    s=kernel[kernel["contain"]==True][["Plataforma","contain"]].groupby(["Plataforma"]).count().sort_values(by="contain",ascending=False).head(1)
    salida = "[ Plataforma ] : " + s.index[0] + "  [ Genero ] : "+gen+"   [ Total Peliculas ] : " + str(s.iloc[0][0])
    return(salida)

