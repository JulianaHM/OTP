''' Librerias OpenTelemetry'''
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
''' Librerias OpenTelemetry'''

from fastapi import FastAPI, Request
from validador import extract_fields
import json
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
import re


app = FastAPI()

# Configuración de OpenTelemetry
provider = TracerProvider()
processor = SimpleSpanProcessor(ConsoleSpanExporter())
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

# Instrumentar FastAPI con OpenTelemetry
FastAPIInstrumentor.instrument_app(app)


templates = Jinja2Templates(directory="templates")

# Endpoint para recibir OTLP
@app.post("/otlp")
async def receive_otlp(request: Request):
       
    body = await request.body()
    try:
        decoded_body = body.decode('utf-8', errors='ignore')
        #print("Datos decodificados (ignorando errores):", decoded_body)
        pattern = re.compile(r'[a-zA-Z0-9.\-]+')
        extracted_text = pattern.findall(decoded_body)
        coherent_text = " ".join(extracted_text)
        extract_fields(coherent_text)
    except UnicodeDecodeError:
        print("Error al decodificar los datos")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
