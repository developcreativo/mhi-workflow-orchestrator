# Contector Fourvenues
## Documentación de la API
https://docs.fourvenues.com/integrations/introduction/overview

## Acceso a la aplicación
https://pro.fourvenues.com/

## Decripción general:
El funcionamiento de este extractor se basa en la siguiente logica, tenemos la siguiente clasificacion de dataset
1) Eventos --> Maestro de eventos que cargaremos siempre todos los activos desde la fecha de inicio del conector hasta 365 días por delante y los activos desde ayer en cargas incrementales
2) Dependiente de Eventos --> Cargaremos por cada evento que procesamos sus datasets dependientes 
3) Datos Maestros --> Datasets independientes que se carga todo y se guarda en parquet

### Lógica de carga
Paso 0: 
Inicialización - Leo última lectura de eventos, calculo fecha fin (hoy + 365)
Leemos listado de eventos y lo almacenamos
Dejamos como variable global la lista de eventos a procesar

Paso 1: 
Clasificar datasets por tipos:
- Tipo 1: no relacionado con evento
- Tipo 3: relacionado con evento no incremental
- Tipo 4: relacionado con eventos incremental
Paso 2: Evaluar estado de eventos (4 estados posibles)  
- Estado 1: Finalizado y procesado completamente
- Estado 2: Finalizado y NO procesado completamente
- Estado 3: No Finalizado y NO procesado completamente
Paso 3: Procesar datasets según tipo y eventos según estado