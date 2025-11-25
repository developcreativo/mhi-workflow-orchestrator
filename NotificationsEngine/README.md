# Notifications Worker
Este repositorio es para la ejecución en una Cloud Function (GCP)\
El repositorio se compila y desplega (CI/CD) con Cloud Build \
Este código se ejecuta cuando entra un mensaje en el topic de PubSub Notification y ejecuta la notificacion para enviar correos a usuarios internos o externos\
La función envía uno o varios emails a través del servicio mailgun utilizando su API: https://documentation.mailgun.com/en/latest/api_reference.html \
La función también puede enviar un correo utilizando una plantilla, estas plantilla están definidas en otro repositorio: fergus-templates que se mantienen de forma autónoma y se almacenan en un bucket de GCS.


## Descripción Funcional:
El servicio es el responsable de enviar notificaciones por email.\
Recibe el evento desde un topic y lo procesa para enviar una notificacion por correo.\
El mensaje que se recibe contiene los siguientes parámetros:
| Valor | Tipo de datos | Obligatorio | Valores posibles |
|:-----:|---------------|----------|------------------|
|type |string|si|S, N, P|
|subject|string|si|el titulo del mensaje|
|text|string|no|el contenido del mensaje en formato texto|
|from_email |string|no|email solo o con nombre con el formato "Nombre <<email@dominio.com>>"|
|to_email|string|si|email receptor, pueden ser varios separados por comas|
|cc|string|no|email en copia, pueden ser varios separados por comas|
|bcc|string|no|email en copia oculta, pueden ser varios separados por comas|
|template|string|no|el nombre de la plantilla que se tiene que enviar|
|parameters|dict|no|este parámetro recibe la clave-valor de los parámetros de la plantilla. 

Existen 3 tipos de notificaciones:
1) sistema: son notificaciones que envía el mismo sistema para alertar a algún usuario administrador del sistema. Necesita los parámetros type=S, subject y text
2) negocio: son notificaciones que envía el mismo sistema para alertar de algún evento de un dominio en particular. Necesita los parámetros type:N, to_email, subject y text
3) plantilla: son notificaciones con una plantilla asociada para enviar a un destinatario, este tipo recibe los datos para hacer el "merge" con la plantilla. Necesita los parámetros type=P, to_email, subject, template y parameters.

El servicio valida que los parámetros sean correctos para el tipo que sea, en cada caso genera el html basado en alguna plantilla definida en el repositorio de plantillas, se realizará el merge y se envia a los destinatarios del mensaje con el mensaje particular para cada destinatario.


## Descripción Técnica:

### Ejecución:
Esta aplicación se ejecuta en una Google Cloud Function con el nombre de notifications_worker, esta función se activa cuando entra un nuevo mensaje en PubSub en el topic notifications

### Compilación y Despliegue:
El repositorio está conectado a Cloud Build de forma que cuando entra una nueva actualización en la rama main se autodespliega.

### Seguridad
El repositorio requiere la APIKEY de Mailgun para su ejecución, ésta está almacenada en Google Cloud Secrets y se consulta en cada ejecución.