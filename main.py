from fastapi import FastAPI, Request, Response
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import io
from validador import extract_fields

app = FastAPI()

@app.post("/otlp")
async def ingest_otlp(request: Request):
    # Leer el cuerpo de la solicitud en formato binario
    body = await request.body()

    try:
        # Deserializar los datos del request usando Protobuf
        export_request = ExportTraceServiceRequest()
        export_request.ParseFromString(body)

        response_data = []

        # Iterar sobre los resourceSpans (padre)
        for resource_span in export_request.resource_spans:
            # Extraer los atributos del hijo 'resource'
            resource_data = {}
            for attribute in resource_span.resource.attributes:
                
                    resource_data[attribute.key] = attribute.value.string_value

            # Extraer los spans desde scope_spans (hijo de resource_span)
            spans_data = []
            for scope_span in resource_span.scope_spans:
                
                    for span in scope_span.spans:
                        
                            span_info = {
                                "trace_id": span.trace_id.hex(),
                                "span_id": span.span_id.hex(),
                                "flags": span.flags,
                                "name": span.name,
                                "start_time_unix_nano": span.start_time_unix_nano,
                                "end_time_unix_nano": span.end_time_unix_nano,
                                "attributes": {attr.key: attr.value.string_value for attr in span.attributes}
                            }
                            if not span_info['attributes']:
                                continue
                            
                            spans_data.append(span_info)

            # Añadir los datos procesados del resource y los spans
            response_data.append({
                "resource": resource_data,
                "spans": spans_data
            })

        
        extract_fields(response_data,resource_data,spans_data)
        #return {"export_request": response_data}

    except Exception as e:
        # Manejo de errores
        return Response(f"Failed to process request: {e}", status_code=400)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
