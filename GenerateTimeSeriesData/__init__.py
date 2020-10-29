import logging

import azure.functions as func

from azure_monitor import AzureMonitorSpanExporter
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib

from .business_layer_adls import BusinessLayer

trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)

instrumentation_key = os.environ["APPINSIGHTS_INSTRUMENTATIONKEY"]

# SpanExporter receives the spans and send them to the target location
exporter = AzureMonitorSpanExporter(connection_string='InstrumentationKey=' +
                                    instrumentation_key, )

span_processor = BatchExportSpanProcessor(exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# connection_string = os.environ["SQL_CONNECTION_STRING"]

# params = urllib.parse.quote_plus(
#     connection_string)  # urllib.parse.quote_plus for python 3

# conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
# engine = create_engine(conn_str, echo=True)

# SQLAlchemyInstrumentor().instrument(
#     engine=engine,
#     service="GenerateTimeSeriesData",
# )

def log_ip():
    import requests
    import json 
    response = requests.get("https://api.ipify.org?format=json")
    obj = response.json()
    logging.info("IP Address is: %s" % obj['ip'])

def main(mytimer: func.TimerRequest) -> None:    
    
    utc_timestamp = datetime.utcnow()    

    with tracer.start_as_current_span('BusinessLayer.run'):
        log_ip()
        # BusinessLayer.run(engine, utc_timestamp, enable_anomaly=False)
        BusinessLayer.run(utc_timestamp, enable_anomaly=False)

    logging.info('Version: 1.6 - Python timer trigger function ran at %s',
                 utc_timestamp.isoformat())
