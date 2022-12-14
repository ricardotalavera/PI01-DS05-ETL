# PROYECTO INDIVIDUAL Nº1 -
## Data Engineering

![](https://cdn-ajfbi.nitrocdn.com/GuYcnotRkcKfJXshTEEKnCZTOtUwxDnm/assets/static/optimized/rev-b8262e6/wp-content/uploads/2019/07/ETL-e1563879776366.jpg)

El presente proyecto en su fase obligatoria consta de tres etapas claramente marcadas :
- Extracción de datos.
- Transformación de los datos.
- Carga de los datos.

### 1.- Extracción

En esta fase se reciben específicamente 4 archivos concernientes a datos de la industria del streaming. Los archivos pertenecen a cada una de las siguientes cuatro plataformas ampliamente conocidas : **Netflix, Disney, Hulu y Amazon-Prime.**

Debemos recordar que la data de entrada puede tener cualquier tipo de forma o extensión de archivos, siendo la extensiones más trilladas : **CSV, TXT, JSON y PARQUET**.

El programa en su fase de extracción, es capaz de diferenciar por si solo el tipo de extensión que le está llegando y por ende conducir la lógica hacia un read en PANDAS acorde con la extensión propia del archivo.

La primera parte importante, es lograr cargar los 4 archivos a través de PANDAS, para luego poder proceder a la fase de transformación.

### 2.- Transformación

Una vez los datos estuvieron levantados, la primera consigna fue verificar que los archivos de origen tuvieran la misma estructura en cuanto a orden y tipo de campos, de tal forma de poder decidir si el programa se personalizaba por cada archivo o simplemente se podía a traves de un bucle hacer un solo tipo de procesamiento para todos ellos.

En este caso se logró verificar que los 4 archivos poseen estructuras similares, por lo que la fase de transformación de datos dentro del programa fue la misma para cada uno de ellos.

#### 2.1.- Valores Perdidos (NAs)

El bucle de transformación inicia procediendo a verificar **valores perdidos (NAs)** en cada columna y se decidió **no eliminar data alguna** por este concepto, toda vez que el archivo puede luego ser consumido para algún modelo de Machine Learning, caso en el cual será decisión del científico de datos a cargo ver con que datos trabaja. Los valores perdidos fueron completados con glosas o descripciones adecuadas.

#### 2.2.- Campo : Duración del film o serie

Como segundo paso se observo que **la columna de duración** se encontraba en formato no numérico, debido a que el campo contenía la duración cuantificada junto con las unidades correspondientes. Cabe resaltar que las unidades de medición de la duración podian ser : min en caso de Movies o peliculas o season en caso de TV Shows.

Debido a que las futuras consultas en la API implican duración cuantificada, asi como que el consumo de este dato no es util tal como está para un modelo de Machine Learning o analítica de datos, se procedió a dividir la columna de Duración, de tal forma que se pueda contar con una  duración en formato numérico entero y otra columna de unidades en formato string. Finalmente ya efectuado el proceso descrito, se procdió a eliminar la columna de duración original.

Luego de la transformación anterior :

**> Es relevante recalcar que hasta la etapa anterior, el bucle de procesamiento fue apilando cada archivo secuencialmente, de tal manera de conformar finalmente una sola tabla o dataframe donde se encuentren todos los registros de todas las plataformas de streaming.**

#### 2.3.- Campo : Cast o reparto de actores

Una vez ya se cuenta con una sola tabla uniformizada, se observó que la columna "Cast" contenia una pila de valores, es decir se encontraban los actores y actrices separados por coma. La columna citada como viene dada, tiene valor nulo para alguna analítica o modelo, por lo que se procedió a crear una nueva tabla de actores y peliculas. La pelicula sería el nexo de consulta con la tabla madre. Al separar el campo lista de actores mediante la instrucción **split** y anexar la pelicula se creo una tabla de actores-peliculas. Cabe resaltar que en esta etapa de reciente creación en la tabla actores-pelicula se pueden encontrar :
- El mismo actor o actriz en diversas peliculas.
- La misma pelicula para varios actores o actrices.
- Registros duplicados ya que se han incluido todas la plataformas de streaming y es valido que una msma pelicula esté en mas de una plataforma. **Razón por la cual luego se ejecuta un drop duplicates para asegurar unicidad de alguna manera.**

#### 2.4.- Genero.

El **campo genero** adolecía del mismo problema que el **campo cast**, es decir
para una misma pelicula podía ser clasificada en mas de un genero y todos ellos estaban en una lista separados por coma.
Razón por la cual que se crea tambien una tabla genero-peliculas similar a lo ya planteado.

#### 2.5.- API y uso futuro de los datos.

Sabiendo que los datos vasn a ser consumidos en primer lugar por una API para consultas así como para futura analítca de la empresa omodelos de predicción. Se decide generar dos versiones de la tabla original, una primera versión para la API con colmnas más resumidas que sirvan especificamente para las consultas y otra tablas más completa para uso de analítica y predicción.
Cabe resaltar que las tablas creadas : actores-peliculas y genero-peliculas se crean en una única versión pues serán consumidas de igual manera.

### 3.- Carga
![](https://res.cloudinary.com/practicaldev/image/fetch/s--iOsUGN0b--/c_limit%2Cf_auto%2Cfl_progressive%2Cq_auto%2Cw_880/https://dev-to-uploads.s3.amazonaws.com/uploads/articles/l4jt274288k241g94r66.png)

Como tercera etapa, se solicitaba que la data generada sirviera de Materia Prima para una API, la cual debía ser desarrollada a través de **FASTAPI** propio de Python y usando la tecnología de containers aportada por **DOCKERS**.

Para lo cual se genera el directorio **PI-05** en forma local para empezar esta fase, el cual contendrá todo lo necesario para levantar el container de DOCKERS, por tanto los archivos de la fase de transformación como productos terminados, son dejados en este directoriocomo primera decisión.

### 3.1.- Instalación de Uvicorn y Pydantic.

Como primer paso se procede a instalar el servidor **Uvicorn y Pydantic **que soporta los modelos de datos. Ambos a traves de **pip install.**

### 3.2.- Conformación de archivo de requerimientos.

Se conforma una archivo requirements.txt, el cual contiene justamente todas las **librerias** que necesitará el programa **main**.**py**. El archivo de requerimientos se aloja en el directorio PI-05.

### 3.3.- Conformación de DOCKER FILE.
Se conforma el archivo docker file que creará el container posteriormente.  Cabe resaltar que se tiene instalado Docker Desktop. El archivo dockerfile se aloja en el directorio PI-05.

### 3.4.- Programa main.py.

Se conforma el programa Main.py con las 4 funciones gets solicitadas. Cabe mencionar que también de añade el programa funciones.py el cual realmente contiene el detalle de las funciones que consiguen los datos solicitados por la API. Los programas main.py y funciones.py se alojan en el directorio PI-05.

### 3.5.- Levantamiento del servicio de forma local

Luego a traves de **uvicorn main:app -- reload** se levanta de manera local fastapi y se prueba en la dirección 127.0.0.1. Cabe resaltar que el servicio se corre ubicados en el directorio PI-05.

### 3.6.- Construcción de DOCKER

Con el servidor uvicorn corriendo localmente, se procede a construir el container de docker con : **docker build -t lab_app**.

Luego se corre : 

**docker run -it -p 8000:8000 -v cd:/usr/src/app lab_app**

Finalmente : **docker start <nombre o volumen>**

Luego de lo anterior se logra leventar el servicio del container, es decir todo cambio que se efectue en el directorio que incluyen los archivod del container será detectado por docker y reflejado inmediatamente, es decir tenemos la API corriendo desde docker.

### 3.7.- Generación de las consultas

Finalmente se procede a probar el servicio con las consultas solicitadas, las cuales entregan las respuestas correctas.

