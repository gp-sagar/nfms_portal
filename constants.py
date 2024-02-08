from db.models import Session, FeederMasteData
import os
import json
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from settings import API_HEADER as HEADERS
import requests
load_dotenv()

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
        
# Calling API
def API_Calling(resp_body, API_FOR):
    API_URL = os.getenv(API_FOR)
    AUTH = HTTPBasicAuth(os.getenv('API_USERNAME'), os.getenv('API_PASSWORD'))
    for i in resp_body:
        i = json.dumps(i, indent=4)
        response = requests.post(API_URL, headers=HEADERS, auth=AUTH, json=i, verify='/etc/ssl/certs/')
        if response.status_code == 200:
            print("Request successful!")
            print(response.json())
        elif response.status_code == 429 or response.status_code == 502:
            print(f'response :: {response}')
            print("You Need To Retry this task")
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
        
        # TODO Remove the break for api call of all feeder meter
        break