import pytz
import uuid
# from main import app
from constants import GetFeederMasterData, API_Calling
from dotenv import load_dotenv
from settings import get_time_shift
from datetime import datetime, timezone, timedelta
from db.models import Session, MdmBlockload

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