from sqlalchemy import Column, Integer, String, DateTime, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()
Base = declarative_base()

class FeederMasteData(Base):
    __tablename__ = 'feedermasterdata'

    id = Column(Integer, primary_key=True)
    s_no = Column(String)
    ut = Column(String)
    discom = Column(String)
    zone = Column(String)
    zone_id = Column(String)
    district_name = Column(String)
    circle_name = Column(String)
    block_name_census = Column(String)
    division_name = Column(String)
    division_id = Column(String)
    sub_division_name = Column(String)
    sub_division_id = Column(String)
    latitude = Column(String)
    longitude = Column(String)
    substation_name = Column(String)
    substation_id = Column(String)
    voltage_ratio_kv = Column(String)
    capacity_mva = Column(String)
    configuration_no_mva = Column(String)
    feeder_code = Column(String)
    feeder_name = Column(String)
    feeder_category = Column(String)
    feeder_length_km = Column(String)
    feeder_voltage_kv = Column(String)
    feeder_conductor_type = Column(String)
    original_commissioning_date = Column(String)
    r_m_works_date = Column(String)
    incoming_feeder_name = Column(String)
    feeder_monitored_amr = Column(String)
    amr_system_integrator = Column(String)
    feeder_monitored_rt_das = Column(String)
    rt_das_system_integrator = Column(String)
    fms_system_nfms = Column(String)
    sanctioned_scheme_name = Column(String)
    meter_serial_no_rt_das = Column(String)
    meter_protocol = Column(String)
    meter_multiplication_factor = Column(String)
    meter_digits_before_decimal = Column(String)
    meter_digits_after_decimal = Column(String)
    installation_date_mfm = Column(String)
    cb_status_capturing_method = Column(String)
    line_pt_arrangement = Column(String)
    connected_dt_count = Column(String)
    total_dt_capacity_kva = Column(String)
    urban_consumers = Column(String)
    rural_consumers = Column(String)
    industrial_consumers = Column(String)
    agricultural_consumers = Column(String)
    priority_consumers = Column(String)
    block_name_supplied = Column(String)
    mobile_no_je = Column(String)
    mobile_no_aee = Column(String)
    meter_serial = Column(String)

class MdmBlockload(Base):
    __tablename__ = 'mdm_blockload'

    id = Column(Integer, primary_key=True)
    data = Column(JSON)  # Assuming data is stored as JSON
    meter_id = Column(Integer)
    data_timestamp = Column(DateTime)
    create_timestamp = Column(DateTime)
    data_source = Column(String)  # Adjust the length if needed

engine = create_engine(os.getenv('DATABASE_URL'))
Session = sessionmaker(bind=engine)
