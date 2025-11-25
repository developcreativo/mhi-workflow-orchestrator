import os
import importlib
import functions_framework
from fastapi import FastAPI
from fastapi.testclient import TestClient

# Crear la app FastAPI
app = FastAPI(
    title       = "Mind Ocean Internal Services", 
    description = "Internal APIs for Mind Ocean components",
    openapi_url = None,
    docs_url    = None,
    redoc_url   = None
)

# Cargar routers automáticamente desde la carpeta 'endpoints'
endpoints_dir = os.path.join(os.path.dirname(__file__), "endpoints")

for filename in os.listdir(endpoints_dir):
    if filename.endswith(".py") and not filename.startswith("__"):
        module_name = f"endpoints.{filename[:-3]}"  # sin .py
        module = importlib.import_module(module_name)

        if hasattr(module, "router"):
            # El prefijo será el nombre del fichero sin extensión
            prefix = f"/{filename[:-3]}"
            app.include_router(module.router, prefix=prefix)
            #print(f"✅ Cargado endpoint: {prefix}")

# Endpoint raíz que lista endpoints disponibles
@app.get("/")
async def root():
    return {
        "endpoints": [
            route.path for route in app.routes if hasattr(route, "methods")
        ],
        "status": "API funcionando correctamente"
    }

# Cliente FastAPI para reenviar las peticiones
client = TestClient(app)

@functions_framework.http
def InternalServices(request):
    """
    Adaptador para que todas las peticiones que entren por la Cloud Function
    sean manejadas por FastAPI.
    """
    method = request.method
    path = request.path

    if request.query_string:
        path = f"{path}?{request.query_string.decode()}"

    headers = dict(request.headers)
    body = request.get_data()

    # Reenviar la petición a FastAPI
    response = client.request(method, path, headers=headers, data=body)
    return (response.content, response.status_code, response.headers.items())
