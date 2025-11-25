# Load GRI
Este servicio será el responsable de obtener los datos de GRI para todos los hoteles y guardarlos en BigQuery.
El servicio consume desde el topic de pubsub "load_gri" y en funcion del contenido del mensaje ejecuta un proceso u otro. De momento, este repositorio está preparado para ejecutar "load_monthly_gri", que es una extracción del GRI a nivel mensual.

## Descripción Funcional
A continuación se hará una breve explicación de qué hace cada .py en este repositorio:
1. secretmanager_loader.py: Carga las credenciales necesarias para la ejecución de la tarea.
2. gri_extractor.py: Consta de 4 tareas esenciales. Extracción del listado de hoteles sobre los que iterar, generación de URLs de descarga para cada hotel, descarga de los datos para cada hotel a nivel mensual en un único dataframe y carga de datos en Bigquery. 
3. cloudbuild: archivo de configuración de compilación básico para cloud build (esto es lo que permitirá que cada vez que se haga un commit a git se actualice el repositorio)
4. main_py: ejecución de gri_extractor. Aquí lo más remarcable es que en el código está comentado cómo evalúa la información que le llega del topic de PubSub.

## Descripción Técnica:

1. Hay que obtener las credenciales de la api de ReviewPro. Para ello, habrá que acceder con los datos del cliente (su partner id) y registrasre en estre enlace: https://developer.reviewpro.com/.

2. Habrá que crear el secreto (credenciales) en el secret manager de Google Cloud. Para ello, accederemos al SecretManager y crearemos la key cuyo nombre será siempre REVIEWPRO_APIKEY: 

Nota: Para ver la previsalización de imágenes markdown en Vscode, hay que pulsar ctrl + shift + v

<img src="imagenes/secretmanager.png" width="400" height="400" alt="Creación de Topic de PubSub">


3. Creamos topic de pubsub llamado load_gri. Para ello, buscamos "Pubsub" en la consola de búsqueda de Google Cloud. Ejemplo:

<img src="imagenes/crear_topic.png" width="400" height="400" alt="Creación de Topic de PubSub">

4. Se creará una subscription automáticamente (La podemos ver buscando "Subscriptions"): 

<img src="imagenes/subscription.png" width="500" height="100" alt="Ver Subscription">

5. Creamos un Cloud Source Repository que obtenga datos del repo en cuestión (desde Github). Para ello, buscamos "cloud source repositories" en la consola de Google, le damos a "add repository" arriba a la derecha, "connect external repository" y seleccionamos el repositorio en cuestión. En este caso, será mhi_reviewpro_extractor:

<img src="imagenes/cloud_source_repository.png" width="400" alt="Repo linkeado">


6. Previamente tendremos que haber configurado/creado el cloud build.yaml (este build creará automáticamente la cloud function con el nombre que especifiquemos en el yaml). A efectos de seguir esta documentación, el .yaml se debe dejar igual ya que es genérico para este extractor.

7. Creamos trigger en cloud build para que cada vez que se haga un push, se auto ejecute la cloud function. Para ello, buscamos "Triggers", Seleccionamos "Triggers" de Cloud Build --> Create Trigger y añadimos la siguiente información (la parte de advanced no hace falta tocarla):

<img src="imagenes/trigger_1.png" width="400" height="400" alt="Trigger 1">

<img src="imagenes/trigger_2.png" width="400" height="400" alt="Trigger 2">

Importante: Las variables de entorno se definirán en el trigger del cloud build, tal y como muestra la siguiente captura:

Por tanto, hay que respetar la nomenclatura de las variables por que .yaml depende estas:  El nombre de las variables siempre deberá empezar con _, el nombre de la tabla (TABLE_ID) será 'nombre-cliente.01_ingestion.reviewpro_gri'.

8. Hacemos click en "Run" del trigger:

<img src="imagenes/run_trigger.png" width="800" height="100" alt="Run Trigger">

9. Vamos a Cloud Build para ver si se ha ejecutado correctamente:

<img src="imagenes/ejecucion_cloud_build.png" width="800" height="75" alt="Run Trigger">


10. (Opcional): Hacemos un dummy commit y hacemos push del cambio dummy para ver si cloud build funciona correctamente.

11. Vamos a Cloud Functions y veremos que se habrá creado una Cloud Function llamada "load_gri_ep", tal y como hemos especificado en el yaml. 

<img src="imagenes/cloud_function.png" width="800" height="100" alt="Run Trigger">

12. En la Cloud Function creada deberemos incluir las variables de entorno:

<img src="imagenes/variables_cloud_function.png" width="400" height="200" alt="Run Trigger">

Nota: El TABLE_ID no es el id de la tabla únicamente, sino que contiene el nombre de proyecto, el dataset y el nombre de la tabla: nombre-de-proyecto.01_ingestion.reviewpro_gri. Lo único que cambiará será el nombre de proyecto ya que la estructura de tablas en BigQuery debe ser la misma para todos los clients.
13. Creamos un job en Cloud Scheduler. Importante, en este punto es donde le pasamos como string el evento 'action': 'load_monthly_gri' que enviará a pubsub y ejecutará todo el proceso. Hay que añadir "{"action":"load_monthly_gri"}" en el message body.

<img src="imagenes/scheduler.png" width="400" height="400" alt="Run Trigger">

# Ejecución
Esta funcion está preparada para que se ejecute cuando entre un mensaje en el topic "load_gri" de Google PubSub, cada día a las 7 AM.

# Despliegue
Esta configurado Cloud Build para que se despliegue en integración continua

# Uso de variables y credenciales de este proyecto:

1. Para las variables estáticas (que no cambian de un cliente a otro, se fijarán al comienzo del script: url base, endpoints, etc)
2. Para las variables que cambian de un cliente a otro (tabla donde ingestar, nombre del proyecto) se establecerán en la Cloud Function
3. Las credenciales deberán estar guardadas en el SecretManager de Google Cloud.

# Para ejecutar en local:

1. Setear las variables de entorno: GCP_PROJECT, TABLE_ID en tu local.
2. Hay que generar el credentials.json de la cuenta de servicio de Google. Una vez lo obtengamos ejecutamos: $env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\Manu\Documents\projects\fergus_extract_reviewpro\dwh-fergus-70b2948832a1.json" o creamos la variable de entorno
3. Instalamos paquetes y configuramos variables de github
    - pip install pyarrow
    - pip install google-cloud-bigquery
    - pip install google-secret-manager
    - git config --global user.email "tunombre@mindanalytics.es"
    - git config --global user.name "usuariomind" 
