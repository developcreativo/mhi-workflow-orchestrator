# Extractor: Avalon
## Descripción

Este conector extrae los datos del PMS Avalon On Premise, el conector traspasará el firewall del cliente y leerá los datos de la base de datos, para ello el cliente debe hacer la siguiente configuración:

- Crear un regla en el firewall que deje pasar el tráfico TCP y UDP de las ips de mind ocean de DEV, QA y PROD y enrutarlo a la máquina donde está la base de datos de Avalon
- 
Contiene los siguientes datos:
- Maestro de Hoteles
- Maestro de Huespeded
- Inventario Habitaciones
- Datos de Reservas
- Datos de Producción
- Otra información satélite para montar el modelo hotelero

El extractor está preparado para ejecutarse cuando llegue un mensaje a pubsub con la siguiente estructura:

```
{
    "_m_account": _m_account,
    "datasets": [dataset1, dataset2,...,datasetn]
}
```
El topic de pubsub es: **extractor-avalon**

La configuración está en el fichero **config.json**

El extractor está preparado tanto para cargas totales como incrementales

La fecha por defecto de inicio de las cargas coincide con el lanzamiento de la aplicación: **2015**

Los datos de fecha están en **UTC**

La clave de la api está en google cloud secret se almacena en minúsculas con el nombre ***<codigo_de_cuenta>_*ulysescloud**
En el secret tambien encontraremos:
```{
    "host":hostname or ip,
    "user":user,
    "password":pass,
    "database":database,
    start_date:"yyyy-mm-dd"
}
```

## Extensión
Para incoporar nuevos tipos de información es necesario añadir la entrada en dos sitios del fichero de configuración:

```
1) datasets
2) master_datasets o transactional_datasets
```

Si hay nuevos tipos de parámetros que difieren con los existentes hay que modificar le método:
```
__getParamSets
````

## Pendientes
Esta es la lista de temas pendientes que le quedan por desarrolar al extractor:

* Los microservicios http no están implementados ya que no se pudieron hacer funcionar
