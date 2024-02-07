import os
import pytz
import uuid
import json
import requests
# from main import app
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from settings import API_HEADER as HEADERS
from settings import get_time_shift
from datetime import datetime, timezone, timedelta
from db.models import Session, FeederMasteData, MdmBlockload

load_dotenv()
utc_timezone = pytz.utc
ist_timezone = pytz.timezone('Asia/Kolkata') 

# @app.task
def BlockloadTask(): # Task is Start from Here
    # Get Feeder Master Data
    meters_list, data_as_dicts = GetFeederMasterData()
    # Get BlockLoad Data
    payload_dict = GetBlockloadData(meters_list, data_as_dicts)
    # Data Process for Body Request
    resp_body = ProcessData(payload_dict)
    # Calling API
    BlockloadAPICall(resp_body)
    
# Calling API
def BlockloadAPICall(resp_body):
    API_URL = os.getenv('BLOCKLOAD_API')
    AUTH = HTTPBasicAuth(os.getenv('API_USERNAME'), os.getenv('API_PASSWORD'))
    for i in resp_body:
        response = requests.post(API_URL, headers=HEADERS, auth=AUTH, json=i, verify='/etc/ssl/certs/')
        print(f'response :: {response}')
        if response.status_code == 200:
            print("Request successful!")
            print("BlockloadAPI fetching...")
            print(response.json())
        elif response.status_code == 429 or response.status_code == 502:
            print("You Need To Retry this task")
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
        
        # TODO Remove the break for api call of all feeder meter
        # break
        
# Get Feeder Master Data
def GetFeederMasterData():
    session = Session()
    try:
        data_from_db = session.query(
            FeederMasteData.meter_serial_no_rt_das,
            FeederMasteData.discom, 
            FeederMasteData.feeder_code,
            FeederMasteData.amr_system_integrator,
            FeederMasteData.fms_system_nfms,
            FeederMasteData.meter_serial
        ).all()
        data_as_dicts = [
            {
                'mtrNmbr': meter_serial_no_rt_das,
                'discomCode': discom,
                'fdrCode': feeder_code,
                'sourceSystem': amr_system_integrator,
                'sourceType': fms_system_nfms,
                'meter_id': meter_serial
            } 
            for meter_serial_no_rt_das, discom, feeder_code, amr_system_integrator, fms_system_nfms, meter_serial in data_from_db
        ]
        meter_id_list = [item['meter_id'] for item in data_as_dicts]
        return meter_id_list, data_as_dicts
    except Exception as e:
        print(f"Error retrieving data from the database: {str(e)}")
    finally:
        session.close()
        
# Get BlockLoad Data
def GetBlockloadData(meter_id : list, data_as_dicts):
    session = Session()
    try:
        today_date = datetime.now()
        start_date_str = (today_date - timedelta(days=1)).strftime('%Y-%m-%d 00:30:00')
        strt_time_obj = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
        strt_time_obj_utc = strt_time_obj.replace(tzinfo=timezone.utc)
        end_date_str = today_date.strftime('%Y-%m-%d 00:00:00')
        end_time_obj = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
        end_time_obj_utc = end_time_obj.replace(tzinfo=timezone.utc)

        data_from_db = (
            session.query(
                MdmBlockload.data
            )
            .filter(
                MdmBlockload.meter_id.in_(meter_id),
                MdmBlockload.data_timestamp >= strt_time_obj_utc,
                MdmBlockload.data_timestamp <= end_time_obj_utc,
            ).all()
        )
        list_of_dict = [item[0] for item in data_from_db]

        final_dict = {}
        for i in data_as_dicts:
            meter_id = i['meter_id']
            final_dict[meter_id] = i
            final_dict[meter_id]['LoadProfile'] = []
            for j in list_of_dict:
                if j['meter_number'] == meter_id:
                    final_dict[meter_id]['LoadProfile'].append(j)
        return final_dict
    except Exception as e:
        pass
    finally:
        session.close()

# Get Blank Data When any Block is missing
def GetBlankData(base_data):
    load_profile_blocks = []
    count = 1
    TIME_SHIFT = get_time_shift()
    for i in TIME_SHIFT[1:]:
        is_present = next((entry for entry in base_data['LoadProfile'] if entry['meter_rtc'] == i), None)
        if is_present:
            if is_present not in load_profile_blocks:
                is_present['sequence'] = count
                load_profile_blocks.append(is_present)
                base_data['LoadProfile'].remove(is_present)
        else:
            new_data = temp_data(count, i)
            if new_data not in load_profile_blocks:
                load_profile_blocks.append(new_data)
        count += 1
    return load_profile_blocks

# Data Process for Body Request
def ProcessData(final_dict):
    dict_key = list(final_dict.keys())
    all_meter_resp = []
    for i in dict_key:
        tranx_id = uuid.uuid4()
        payload_dict = {
            "transactionId": str(tranx_id),
            "mtrNmbr": final_dict[i]["mtrNmbr"],
            "discomCode": final_dict[i]["discomCode"],
            "fdrCode": final_dict[i]["fdrCode"],
            "sourceSystem": final_dict[i]["sourceSystem"],
            "sourceType": final_dict[i]["sourceType"],
            "secPerInterval": "900",
            "LoadProfile": [
                {
                    "sequence": 1,
                    "R_Voltage": float(entry["RN_BLS_volatge"]),
                    "Y_Voltage": float(entry["YN_BLS_volatge"]),
                    "B_Voltage": float(entry["BN_BLS_volatge"]),
                    "R_Current": float(entry["Rphase_BLS_current"]),
                    "Y_Current": float(entry["Yphase_BLS_current"]),
                    "B_Current": float(entry["Bphase_BLS_current"]),
                    "block_kWh": float(entry["import_Wh"]),
                    "block_kVAh": float(entry["export_Wh"]),
                    "block_kVArh_lag": "NA",
                    "block_kVArh_lead": "NA",
                    "meter_rtc": entry["blockload_datetime"]
                }
                for entry in final_dict[i]["LoadProfile"]
            ]
        }
        # Meter RTC time
        for entry in payload_dict['LoadProfile']:
            entry['meter_rtc'] = subtract_time(entry['meter_rtc'])
    
    # Check All Blocks are available or not, if not then fill the blank data
        new_load_profile = GetBlankData(payload_dict)
        payload_dict["LoadProfile"] = new_load_profile
        all_meter_resp.append(payload_dict)
        
    return all_meter_resp

# str UTC time to IST
def subtract_time(datetime_str):
    dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S") - timedelta(hours=5, minutes=30)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# Temp Data
def temp_data(seq, date):
    data_temp = {
        "sequence": seq,
        "R_Voltage": "NA",
        "Y_Voltage": "NA",
        "B_Voltage": "NA",
        "R_Current": "NA",
        "Y_Current": "NA",
        "B_Current": "NA",
        "block_kWh": "NA",
        "block_kVAh": "NA",
        "block_kVArh_lag": "NA",
        "block_kVArh_lead": "NA",
        "meter_rtc": date
    }
    return data_temp