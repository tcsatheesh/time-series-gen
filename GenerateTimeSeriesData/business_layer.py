import os
import urllib
import random
from datetime import datetime, timedelta
import logging
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import types, case, inspect
from sqlalchemy.sql import expression, select, literal_column
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

datetime_format = "%Y-%m-%dT%H:%M:00Z"


class SensorReading(Base):
    __tablename__ = 'sensor_reading_2'

    timestamp = Column(DateTime, primary_key=True)
    equipment_tag = Column(String, primary_key=True)
    value = Column(Float)

    def __repr__(self):
        return "<sensor_reading_2(timestamp='%s', equipment_tag='%s', value='%s')>" % (
            self.timestamp, self.equipment_tag, self.value)


class BaseLayer():
    def __init__(self, args):
        pass

    def logme(self, message):
        logging.info(message)


class DataAccessLayer(BaseLayer):
    def __init__(self, engine):
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def create_table(self, engine):
        Base.metadata.create_all(engine)

    def add_sensor_reading(self, sensor_reading):
        self.session.add(sensor_reading)
        self.logme("added record for timestamp: %s" %
                   str(sensor_reading.timestamp))

    def add_record(self, timestamp, equipment_tag, value):
        sensor_reading = SensorReading(timestamp=timestamp,
                                       equipment_tag=equipment_tag,
                                       value=value)
        self.add_sensor_reading(sensor_reading)

    def query_record(self, timestamp):
        sensor_readings = self.session.query(SensorReading).filter(
            SensorReading.timestamp.in_([timestamp])).all()
        self.logme("Query returned %s record(s)" % str(len(sensor_readings)))
        return sensor_readings

    def delete_record(self, sensor_readings):
        for sensor_reading in sensor_readings:
            self.session.delete(sensor_reading)
            self.logme("record deleted for timestamp: %s" %
                       str(sensor_reading.timestamp))

    def test(self):
        datetime_value_str = "2020-10-23T13:31:00Z"  # datetime.utcnow().strftime(datetime_format) #
        timestamp = datetime.strptime(datetime_value_str, datetime_format)
        self.add_record(timestamp=timestamp,
                        equipment_tag="turbine_pressure",
                        value=13.3)
        sensor_readings = self.query_record(timestamp=timestamp)
        self.delete_record(sensor_readings=sensor_readings)

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def get_last_records(self):
        last_record_query = self.session.query(
            sqlalchemy.func.max(SensorReading.timestamp).label("timestamp"),
            SensorReading.equipment_tag).group_by(
                SensorReading.equipment_tag).order_by(
                    sqlalchemy.func.max(SensorReading.equipment_tag).asc())
        return last_record_query.all()

    def close(self):
        self.session.close()


class BusinessLayer(BaseLayer):
    def __init__(self, current_datetime, engine, enable_anomaly):
        self.dal = DataAccessLayer(engine)
        self.current_datetime = current_datetime
        self.enable_anomaly = enable_anomaly

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

    def create_next_record(self, previous_record):
        new_timestamp = previous_record.timestamp + timedelta(seconds=60)
        _time_difference = self.current_datetime - new_timestamp
        _next_record = None
        if _time_difference.days > -1 and _time_difference.seconds > 0:
            _next_record = SensorReading(
                timestamp=new_timestamp,
                equipment_tag=previous_record.equipment_tag,
                value=self.get_value(previous_record))
            self.dal.add_sensor_reading(_next_record)
        return _next_record

    def create_next_records(self, previous_records):
        _next_records = []
        for _previous_record in previous_records:
            _next_record = self.create_next_record(_previous_record)
            if _next_record:
                _next_records.append(_next_record)
        return _next_records

    def process(self):
        _all_records = []
        _previous_records = self.dal.get_last_records()
        _next_records = self.create_next_records(_previous_records)
        while (len(_next_records) > 0):
            _all_records = _all_records + _next_records
            _previous_records = _next_records
            _next_records = self.create_next_records(_previous_records)
        return _all_records

    @classmethod
    def run(cls, engine, current_datetime, enable_anomaly):
        bl = BusinessLayer(current_datetime=current_datetime,
                           engine=engine,
                           enable_anomaly=enable_anomaly)
        next_records = bl.process()
        # bl.logme(next_records)
        bl.dal.commit()
        bl.dal.close()


if __name__ == "__main__":
    utc_timestamp = datetime.utcnow()
    connection_string = os.environ["SQL_CONNECTION_STRING"]

    params = urllib.parse.quote_plus(
        connection_string)  # urllib.parse.quote_plus for python 3

    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=True)
    BusinessLayer.run(engine, utc_timestamp, False)