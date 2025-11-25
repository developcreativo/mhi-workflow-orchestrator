# MSQlikAppReload

Microservicio para ejecutar recargas de aplicaciones de Qlik Cloud.

## Descripción

Este microservicio está basado en `MSQlikAutomationExecution` pero adaptado específicamente para manejar recargas de aplicaciones de Qlik Cloud. Utiliza los mismos secretos de autenticación pero se enfoca en los endpoints de recarga de aplicaciones.

## Funcionalidades

- **Iniciar Recarga**: Ejecuta una recarga completa de una aplicación Qlik
- **Polling de Estado**: Monitorea el progreso de la recarga hasta su finalización
- **Notificaciones**: Integra con el sistema de notificaciones para informar sobre el estado

## Estados de Recarga

El microservicio monitorea los siguientes estados de Qlik para recargas:

**Estados Activos:**
- `QUEUED`: Recarga en cola
- `RELOADING`: Recarga en progreso
- `CANCELING`: Cancelación en progreso

**Estados Finales:**
- `SUCCEEDED`: Recarga completada exitosamente
- `FAILED`: Recarga falló
- `CANCELED`: Recarga cancelada
- `EXCEEDED_LIMIT`: Recarga excedió límites

## Endpoints Qlik Utilizados

- **POST**: `https://{{tenant_name}}.{{deployment_location}}.qlikcloud.com/api/v1/reloads`
- **GET**: `https://{{tenant_name}}.{{deployment_location}}.qlikcloud.com/api/v1/reloads/{{reload_id}}`

## Parámetros Requeridos

- `_m_account`: Cuenta de Mind Hotel Insights
- `flow_id`: ID del flujo (para notificaciones)
- `run_id`: ID de la ejecución (para notificaciones)
- `task_id`: ID de la tarea (para notificaciones)
- `app_id`: ID de la aplicación Qlik a recargar
- `reload_token`: Token de autenticación para Qlik Cloud
- `max_attempts`: (Opcional) Número máximo de intentos de polling (default: 60)
- `poll_interval`: (Opcional) Intervalo entre polls en segundos (default: 30)

Ver `example_message.json` para un ejemplo completo de la estructura del mensaje.

## Estructura del Proyecto

```
MSQlikAppReload/
├── main.py                    # Punto de entrada principal
├── config.json               # Configuración de URLs y parámetros
├── requirements.txt          # Dependencias Python
├── cloudbuild.yaml          # Configuración de despliegue
├── core/
│   ├── config.py            # Configuración centralizada
│   ├── handlers/
│   │   └── qlik_polling_handler.py  # Handler para polling
│   └── utils/
│       └── message_utils.py  # Utilidades de mensajes
├── utils/
│   ├── controller.py        # Controlador para secretos
│   ├── publisher.py         # Publicador de mensajes Pub/Sub
│   └── *.json              # Estructuras de mensajes
└── worker/
    └── appreload.py        # Worker principal para recargas
```

## Cloud Functions

- **IniciarRecargaApp**: Función principal para iniciar recargas
- **PollingEstadoRecargaQlik**: Función para monitorear el estado

## Topics Pub/Sub

- **Trigger Principal**: `ms-qlik-app-reload` - Para iniciar recargas de aplicaciones
- **Polling**: `ms-qlik-app-reload-polling` - Para monitorear el estado de recargas
- **Notificaciones**: `ms-flows-controller` - Para notificar resultados al FlowController

## Despliegue

El microservicio se despliega automáticamente mediante Cloud Build cuando se hace push a las ramas correspondientes:

- `develop` → Despliegue en entorno de desarrollo
- `qa` → Despliegue en entorno de QA  
- `main` → Despliegue en entorno de producción

## Configuración

El microservicio utiliza el mismo sistema de secretos que `MSQlikAutomationExecution` para mantener la consistencia en la autenticación con Qlik Cloud.
