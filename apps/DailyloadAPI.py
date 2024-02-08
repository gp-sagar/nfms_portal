# from main import app
from datetime import datetime, timezone, timedelta
from db.models import Session, MdmDailyload
from constants import GetFeederMasterData, API_Calling
from sqlalchemy import func
import uuid

# @app.task
def DailyloadTask():
    meters_list, data_as_dicts = GetFeederMasterData()
    resp_body = GetDailyloadData(meters_list, data_as_dicts)
    API_Calling(resp_body, API_FOR='DAILYLOAD_API')
    print(f'Dailyload API Called')

# Get Dailyload Data
def GetDailyloadData(meter_id : list, data_as_dicts):
    session = Session()
    try:
        today_date = datetime.now()
        start_date_str = (today_date - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')

        data_from_db = session.query(MdmDailyload.data).filter(
            MdmDailyload.meter_id.in_(meter_id),
            func.jsonb_extract_path_text(MdmDailyload.data, 'dailyload_datetime') == start_date_str
        ).all()

        meter_list = []
        list_of_dict = [item[0] for item in data_from_db if (meter_list.append(item[0]['meter_number']) or True)]
        # Check how many meters data is not received from DB
        if len(meter_id) != len(list_of_dict):
            meter_data_none = [meter for meter in meter_id if meter not in meter_list]
            # Blank data is generated for meters from which data has not been received from DB
            for i in meter_data_none:
                dummy_data = {
                    'import_Wh' : 'NA',
                    'import_VAh' : 'NA',
                    'meter_number' : i,
                    'exec_datetime' : 'NA',
                    'dailyload_datetime' : start_date_str
                }
                list_of_dict.append(dummy_data)
        final_dict = []
        for i in data_as_dicts:
            tranx_id = uuid.uuid4()
            meter_serial = i['meter_id']
            for j in list_of_dict:
                if j['meter_number'] == meter_serial:
                    payload_dict = {
                        "transactionId" : str(tranx_id),
                        "DiscomCode" : i['discomCode'],
                        "mtrNmbr" : i['mtrNmbr'],
                        "fdrCode" : i['fdrCode'],
                        "meter_rtc" : j['dailyload_datetime'],
                        "cummActiveEnergy" : j['import_Wh'],
                        "cummApparentEnery" : j['import_VAh']
                    }
                    final_dict.append(payload_dict)
        return final_dict
    except Exception as e:
        pass
    finally:
        session.close()