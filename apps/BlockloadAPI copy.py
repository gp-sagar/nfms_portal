import pytz
import uuid
# from main import app
from constants import GetFeederMasterData, API_Calling
from dotenv import load_dotenv
from settings import get_time_shift
from datetime import datetime, timezone, timedelta
from db.models import Session, MdmBlockload
from sqlalchemy import func

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
    API_Calling(resp_body, API_FOR='BLOCKLOAD_API')
    print(f'Blockload API Called')
        
# Get BlockLoad Data
def GetBlockloadData(meter_id : list, data_as_dicts):
    session = Session()
    try:
        today_date = datetime.now()
        start_date_str = (today_date - timedelta(days=1)).strftime('%Y-%m-%d 00:30:00')
        # strt_time_obj = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
        # strt_time_obj_utc = strt_time_obj.replace(tzinfo=timezone.utc)
        end_date_str = today_date.strftime('%Y-%m-%d 00:00:00')
        # end_time_obj = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
        # end_time_obj_utc = end_time_obj.replace(tzinfo=timezone.utc)

        data_from_db = (
            session.query(
                MdmBlockload.data
            )
            .filter(
                MdmBlockload.meter_id.in_(meter_id),
                # MdmBlockload.data_timestamp >= strt_time_obj_utc,
                # MdmBlockload.data_timestamp <= end_time_obj_utc,
                func.jsonb_extract_path_text(MdmBlockload.data, 'blockload_datetime') >= start_date_str,
                func.jsonb_extract_path_text(MdmBlockload.data, 'blockload_datetime') <= end_date_str
            ).all()
        )
        # list_of_dict = [item[0] for item in data_from_db]
        meter_list = []
        list_of_dict = [item[0] for item in data_from_db if (item[0]['meter_number'] not in meter_list) and (meter_list.append(item[0]['meter_number']) or True)]

        # Check how many meters data is not received from DB
        if len(meter_id) != len(meter_list):
            meter_data_none = [meter for meter in meter_id if meter not in meter_list]
            # Blank data is generated for meters from which data has not been received from DB
            for i in meter_data_none:
                dummy_data = {
                    'export_Wh' : 'NA', 
                    'import_Wh' : 'NA', 
                    'export_VAh' : 'NA', 
                    'import_VAh' : 'NA', 
                    'meter_number' : i, 
                    'exec_datetime' : 'NA', 
                    'BN_BLS_volatge' : 'NA', 
                    'RN_BLS_volatge' : 'NA', 
                    'YN_BLS_volatge' : 'NA', 
                    'Bphase_BLS_current' : 'NA', 
                    'Rphase_BLS_current' : 'NA', 
                    'Yphase_BLS_current' : 'NA', 
                    'blockload_datetime' : 'NA', 
                    'reactive_energy_Q1' : 'NA', 
                    'reactive_energy_Q2' : 'NA', 
                    'reactive_energy_Q3' : 'NA', 
                    'reactive_energy_Q4' : 'NA'
                }
                list_of_dict.append(dummy_data)

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
                    "R_Voltage": entry["RN_BLS_volatge"],
                    "Y_Voltage": entry["YN_BLS_volatge"],
                    "B_Voltage": entry["BN_BLS_volatge"],
                    "R_Current": entry["Rphase_BLS_current"],
                    "Y_Current": entry["Yphase_BLS_current"],
                    "B_Current": entry["Bphase_BLS_current"],
                    "block_kWh": entry["import_Wh"],
                    "block_kVAh": entry["export_Wh"],
                    "block_kVArh_lag": "NA",
                    "block_kVArh_lead": "NA",
                    "meter_rtc": entry.get("blockload_datetime", "NA")
                }
                for entry in final_dict[i]["LoadProfile"]
            ]
        }
        # Meter RTC time
        # for entry in payload_dict['LoadProfile']:
        #     try:
        #         entry['meter_rtc'] = subtract_time(entry['meter_rtc'])
        #         print(entry['meter_rtc'])
        #     except Exception as e:
        #         print(e)
    
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