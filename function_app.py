import azure.functions as func
import logging
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="helloworldlkt")
def helloworldlkt(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    return func.HttpResponse(
        json.dumps({"message": "Diego Simeone egy mocskos ember"}),
        mimetype="application/json"
    )

