# from main import app
from datetime import datetime, timezone, timedelta
from db.models import Session, MdmBilling
from constants import GetFeederMasterData, API_Calling
from sqlalchemy import func
import uuid

# @app.task
def BillingTask():
    meters_list, data_as_dicts = GetFeederMasterData()
    resp_body = GetBillingData(meters_list, data_as_dicts)
    API_Calling(resp_body, API_FOR='DAILYLOAD_API')
    print(f'Billing API Called')


# Get Billing Data
def GetBillingData(meter_id : list, data_as_dicts):
    session = Session()
    try:
        today_date = datetime.now()
        start_date_str = (today_date - timedelta(days=1)).strftime('%Y-%m-01 00:00:00')
        # strt_time_obj = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
        # strt_time_obj_utc = strt_time_obj.replace(tzinfo=timezone.utc)

        data_from_db = session.query(
            MdmBilling.data
        ).filter(
            MdmBilling.meter_id.in_(meter_id),
            func.jsonb_extract_path_text(MdmBilling.data, 'billing_datetime') == start_date_str
        ).all()
        meter_list = []
        list_of_dict = [item[0] for item in data_from_db if (meter_list.append(item[0]['meter_number']) or True)]
        if len(meter_id) != len(list_of_dict):
            meter_data_none = [meter for meter in meter_id if meter not in meter_list]
            for i in meter_data_none:
                dummy_data = {
                    'MD_W': 'NA', 
                    'MD_VA': 'NA', 
                    'WH_TZ5': 'NA', 
                    'WH_TZ6': 'NA', 
                    'WH_TZ7': 'NA', 
                    'WH_TZ8': 'NA', 
                    'avg_PF': 'NA', 
                    'KVAH_TZ5': 'NA', 
                    'KVAH_TZ6': 'NA', 
                    'KVAH_TZ7': 'NA', 
                    'KVAH_TZ8': 'NA', 
                    'MD_KW_TZ5': 'NA', 
                    'MD_KW_TZ6': 'NA', 
                    'MD_KW_TZ7': 'NA', 
                    'MD_KW_TZ8': 'NA', 
                    'export_Wh': 'NA', 
                    'MD_KVA_TZ5': 'NA', 
                    'MD_KVA_TZ6': 'NA', 
                    'MD_KVA_TZ7': 'NA', 
                    'MD_KVA_TZ8': 'NA', 
                    'MD_W_TOD_1': 'NA', 
                    'MD_W_TOD_2': 'NA', 
                    'MD_W_TOD_3': 'NA', 
                    'MD_W_TOD_4': 'NA', 
                    'export_VAh': 'NA', 
                    'MD_VA_TOD_1': 'NA', 
                    'MD_VA_TOD_2': 'NA', 
                    'MD_VA_TOD_3': 'NA', 
                    'MD_VA_TOD_4': 'NA', 
                    'meter_number': i, 
                    'MD_W_datetime': 'NA', 
                    'exec_datetime': 'NA', 
                    'MD_VA_datetime': 'NA', 
                    'cumm_VARH_lead': 'NA', 
                    'cumm_import_Wh': 'NA', 
                    'cumm_import_VAh': 'NA', 
                    'import_Wh_TOD_1': 'NA', 
                    'import_Wh_TOD_2': 'NA', 
                    'import_Wh_TOD_3': 'NA', 
                    'import_Wh_TOD_4': 'NA', 
                    'billing_datetime': start_date_str, 
                    'import_VAh_TOD_1': 'NA', 
                    'import_VAh_TOD_2': 'NA', 
                    'import_VAh_TOD_3': 'NA', 
                    'import_VAh_TOD_4': 'NA', 
                    'MD_KW_TZ6_datetime': 'NA', 
                    'MD_KW_TZ7_datetime': 'NA', 
                    'MD_KW_TZ8_datetime': 'NA', 
                    'MD_KVA_TZ6_datetime': 'NA', 
                    'MD_KVA_TZ7_datetime': 'NA', 
                    'MD_KVA_TZ8_datetime': 'NA', 
                    'MD_KW_TZ5_date_time': 'NA', 
                    'MD_W_TOD_1_datetime': 'NA', 
                    'MD_W_TOD_2_datetime': 'NA', 
                    'MD_W_TOD_3_datetime': 'NA', 
                    'MD_W_TOD_4_datetime': 'NA', 
                    'cumm_energy_VArh_Q2': 'NA', 
                    'cumm_energy_VArh_Q3': 'NA', 
                    'MD_VA_TOD_1_datetime': 'NA', 
                    'MD_VA_TOD_2_datetime': 'NA', 
                    'MD_VA_TOD_3_datetime': 'NA', 
                    'MD_VA_TOD_4_datetime': 'NA', 
                    'cumm_energy_KVARH_LAG': 'NA', 
                    'MD_KVA_TZ5_date_datetime': 'NA', 
                    'total_poweron_duraion_min': 'NA'
                }
                list_of_dict.append(dummy_data)

        final_dict = []
        for i in data_as_dicts:
            tranx_id = uuid.uuid4()
            # meter_data = {}
            meter_serial = i['meter_id']
            for j in list_of_dict:
                # meter_data[meter_id] = {}
                if j['meter_number'] == meter_serial:
                    # TODO Key Mapping Pending
                    payload_dict = {
                        'transactionId' : str(tranx_id),
                        'discomCode' : i['discomCode'],
                        'mtrNumbr' : i['mtrNmbr'],
                        'fdrCode' : i['fdrCode'],
                        'sourceSystem' : i['sourceSystem'],
                        'sourceType' : i['sourceType'],
                        'billDateTime' : j['billing_datetime'],
                        'sysPF' : j['avg_PF'],
                        'cummActiveEnergy' : 'NA',
                        'activeEnergyTZ1' : 'NA',
                        'activeEnergyTZ2' : 'NA',
                        'activeEnergyTZ3' : 'NA',
                        'activeEnergyTZ4' : 'NA',
                        'cummApparentEnergy' : 'NA',
                        'apparentEnergyTZ1' : 'NA',
                        'apparentEnergyTZ2' : 'NA',
                        'apparentEnergyTZ3' : 'NA',
                        'apparentEnergyTZ4' : 'NA',
                        'cumm_kVArh_lag' : 'NA',
                        'cumm_kVArh_lead' : 'NA',
                        'max_Demand_kW' : 'NA',
                        'max_Demand_kVA' : 'NA'
                        }
                    final_dict.append(payload_dict)
        return final_dict
    except Exception as e:
        pass
    finally:
        session.close()