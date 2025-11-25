# Extractor: Holded
## Descripción

Este conector extrae los datos de la plataforma SaaS Holded: www.holded.com
Contiene los siguientes datos:
- Contabilidad
- Ventas
- Compras
- Proyectos
- Usuarios
- Empleados

La documentación de la API la encontramos en: https://developers.holded.com/reference/api-key

El extractor está preparado para ejecutarse cuando llegue un mensaje a pubsub con la siguiente estructura:

```
{
    "_m_account": _m_account,
    "extraction_type": extraction_type
}
```

Los tipos de carga admitidos son ['MASTER','TRANSACTIONAL','TOTAL']

El topic de pubsub es: **extractor-holded**

La configuración está en el fichero **config.json**

El extractor está preparado tanto para cargas totales como incrementales

La fecha por defecto de inicio de las cargas coincide con el lanzamiento de la aplicación: **2015**

Los datos de fecha están en **UTC**

La clave de la api está en google cloud secret se almacena en minúsculas con el nombre ***<codigo_de_cuenta>*_holded**

## Extensión
Para incoporar nuevos tipos de información es necesario añadir la entrada en dos sitios del fichero de configuración:

```
1) datasets
2) master_datasets o transactional_datasets
```

Si hay nuevos tipos de parámetros que difieren con los existentes hay que modificar le método:
```
__EjecutarCargaPaginada
````

## Pendientes
Esta es la lista de temas pendientes que le quedan por desarrolar al extractor:

* La integración con notificaciones no está implementada
* Los microservicios http no están implementados ya que no se pudieron hacer funcionar
