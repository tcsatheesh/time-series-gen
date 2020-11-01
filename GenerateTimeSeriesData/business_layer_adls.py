import os
import json
import random
from datetime import datetime, timedelta
import logging
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

# Base = declarative_base()

datetime_format = "%Y-%m-%dT%H:%M:00Z"
json.JSONEncoder.default = lambda self, obj: (obj.strftime(
    datetime_format) if isinstance(obj, datetime) else None)


class SensorReading():
    def __init__(self, timestamp, equipment_tag, value):
        self.timestamp = timestamp
        self.equipment_tag = equipment_tag
        self.value = value


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SensorReading):
            return obj.__dict__
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class BaseLayer():
    def __init__(self, args):
        pass

    def logme(self, message):
        logging.info(message)
        print(message)


class DataAccessLayer(BaseLayer):
    def __init__(self):
        self.file_system_name = "metadv"
        self.file_system_client = self.get_file_system_client()
        self.last_records_blob_name = "last-records.json"
        self.anomaly_file_name = "anomaly.json"

    def get_file_system_client(self):
        connect_str = os.environ["ADLS_CONNECTION_STRING"]
        service_client = DataLakeServiceClient.from_connection_string(
            connect_str)
        file_system_client = service_client.get_file_system_client(
            file_system=self.file_system_name)
        return file_system_client

    def is_anomaly_enabled(self):
        anomaly_status = False
        try:
            file_client = self.file_system_client.get_file_client(
                self.anomaly_file_name)
            obj = json.loads(file_client.download_file().readall())
            anomaly_status =  bool(obj["is_anomaly_enabled"])
        except ResourceNotFoundError:
            anomaly_record = {
                "is_anomaly_enabled": anomaly_status
            }
            file_client = self.file_system_client.get_file_client(
                self.anomaly_file_name)
            self.logme("\nUploading anomaly record to Azure Data Lake Store as: " +
                    self.anomaly_file_name)
            json_str = json.dumps(anomaly_record, cls=ComplexEncoder)
            file_client.upload_data(json_str, overwrite=True)
        return anomaly_status

    def get_last_records(self):
        last_records = []
        last_record_timestamp = datetime.utcnow()
        try:
            file_client = self.file_system_client.get_file_client(
                self.last_records_blob_name)
            obj = json.loads(file_client.download_file().readall())
            last_record_timestamp = datetime.strptime(
                obj["last_record_timestamp"], datetime_format)
            last_records = []
            for record in obj["records"]:
                last_records.append(
                    SensorReading(
                        datetime.strptime(record["timestamp"],
                                          datetime_format),
                        record["equipment_tag"], record["value"]))
        except ResourceNotFoundError:
            start_timestamp = datetime.strptime("2020-10-13T02:02:00Z",
                                                datetime_format)
            last_record_timestamp = start_timestamp  - timedelta(minutes=1) # datetime.utcnow() - timedelta(minutes=2)
            equipment_list = {
                "turbine_temperature", "turbine_humidity", "turbine_pressure",
                "booster_temperature", "booster_humidity", "booster_pressure",
                "engine_temperature", "engine_humidity", "engine_pressure",
                "main_valve_temperature", "main_valve_humidity",
                "main_valve_pressure"
            }
            last_records = []
            for equipment in equipment_list:
                last_record = SensorReading(timestamp=last_record_timestamp,
                                            equipment_tag=equipment,
                                            value=None)
                last_records.append(last_record)
        return last_record_timestamp, last_records

    def write_records(self, new_timestamp, records):
        json_str = json.dumps(records, cls=ComplexEncoder)
        _blob_name = new_timestamp.strftime("%Y-%m-%d-%H-%M.json")
        directory_client = self.file_system_client.create_directory(
            new_timestamp.strftime("%Y/%m/%d/%H"))
        file_client = directory_client.get_file_client(_blob_name)
        self.logme("\nUploading to Azure Data Lake Store as: " + _blob_name)
        file_client.upload_data(json_str, overwrite=True)

    def write_last_records(self, last_timstamp, records):
        last_record = {
            "last_record_timestamp": last_timstamp.strftime(datetime_format),
            "records": records
        }
        file_client = self.file_system_client.get_file_client(
            self.last_records_blob_name)
        self.logme("\nUploading last record to Azure Data Lake Store as: " +
                   self.last_records_blob_name)
        json_str = json.dumps(last_record, cls=ComplexEncoder)
        file_client.upload_data(json_str, overwrite=True)


class BusinessLayer(BaseLayer):
    def __init__(self, current_datetime, enable_anomaly):
        self.dal = DataAccessLayer()
        self.current_datetime = current_datetime
        self.enable_anomaly = self.dal.is_anomaly_enabled()

    def get_value(self, previous_record):
        equipment_list = {
            "turbine_temperature": {
                "min": 30,
                "max": 50
            },
            "turbine_humidity": {
                "min": 40,
                "max": 70
            },
            "turbine_pressure": {
                "min": 12,
                "max": 16
            },
            "booster_temperature": {
                "min": 30,
                "max": 50
            },
            "booster_humidity": {
                "min": 40,
                "max": 70
            },
            "booster_pressure": {
                "min": 12,
                "max": 16
            },
            "engine_temperature": {
                "min": 30,
                "max": 50
            },
            "engine_humidity": {
                "min": 40,
                "max": 70
            },
            "engine_pressure": {
                "min": 12,
                "max": 16
            },
            "main_valve_temperature": {
                "min": 30,
                "max": 50
            },
            "main_valve_humidity": {
                "min": 40,
                "max": 70
            },
            "main_valve_pressure": {
                "min": 12,
                "max": 16
            }
        }
        start = equipment_list[previous_record.equipment_tag]["min"]
        end = equipment_list[previous_record.equipment_tag]["max"]
        x = round(random.uniform(start, end), 2)
        if self.enable_anomaly:
            anomaly = random.uniform(-1, 1)
            if anomaly > 0:
                x = round(end * anomaly, 2)
            elif anomaly < 0:
                x = round(start * -1 * anomaly, 2)
        return x

    def create_next_record(self, previous_record, new_timestamp):
        _time_difference = self.current_datetime - new_timestamp
        _next_record = None
        if _time_difference.days > -1 and _time_difference.seconds > 0:
            _next_record = SensorReading(
                timestamp=new_timestamp,
                equipment_tag=previous_record.equipment_tag,
                value=self.get_value(previous_record))
        return _next_record

    def create_next_records(self, previous_records, new_timestamp):
        _next_records = []
        for _previous_record in previous_records:
            _next_record = self.create_next_record(_previous_record,
                                                   new_timestamp)
            if _next_record:
                _next_records.append(_next_record)
        return _next_records

    def write_records(self, x):
        self.dal.write_records(x['new_timestamp'], x['records'])

    def process(self, pooled_connection=False):
        _last_record_time, _previous_records = self.dal.get_last_records()
        new_timestamp = _last_record_time + timedelta(seconds=60)
        _next_records = self.create_next_records(_previous_records,
                                                 new_timestamp)
        records_to_write = []
        while (len(_next_records) > 0):
            record_to_write = {
                "new_timestamp" : new_timestamp, 
                "records" : _next_records
            }
            records_to_write.append(record_to_write)
            _previous_records = _next_records
            _last_record_time = new_timestamp
            new_timestamp = new_timestamp + timedelta(seconds=60)
            _next_records = self.create_next_records(_previous_records,
                                                     new_timestamp)        
        if pooled_connection:
            from multiprocessing import Pool
            with Pool(10) as p:
                p.map(self.write_records, records_to_write)
        else:
            for x in records_to_write:
                self.dal.write_records(x['new_timestamp'], x['records'])
            self.dal.write_last_records(_last_record_time, _previous_records)

    @classmethod
    def run(cls, current_datetime, enable_anomaly):
        bl = BusinessLayer(current_datetime=current_datetime,
                           enable_anomaly=enable_anomaly)
        bl.process()


if __name__ == "__main__":
    utc_timestamp = datetime.utcnow()
    BusinessLayer.run(utc_timestamp, False)
   
