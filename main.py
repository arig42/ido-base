import os
import json
import requests
import logging
from datetime import datetime
from fastapi import FastAPI
from lib import IdoBase


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


api_token = os.getenv('API_TOKEN', 'api')


class Ido(IdoBase):

    @staticmethod
    def call_api():
        """
        Calling API for prjs
        """

        url = "https://vmwppqa4er.us-east-1.awsapprunner.com/api/launch"
        payload = {}
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        logger.info(f'Call request to {url} ...')
        response = requests.request("GET", url, headers=headers, data=payload)
        prjs = {}
        if response.status_code == 200:
            data = response.json()
            for _ in data:
                prjs[_.get('routeName')] = _
                break

        logger.info(f'Received {len(prjs)}')
        return prjs
    
    @staticmethod
    def transform(raw):
        """
        Transform prj to match db schema
        """

        STATE_MAPPING = {
            'upcoming': 'upcoming',
        }
        
        logger.debug(json.dumps(raw, indent=2))
        return {
            'lpad': 'ape',
            'token': raw.get('routeName'),
            'state': STATE_MAPPING.get(raw.get('type'), raw.get('type')),
            'reg_at': datetime.fromtimestamp(raw.get('registrationEndDate')/1e3),
            'buy_at': datetime.fromtimestamp(raw.get('registrationEndDate')/1e3),
            'claim_at': None,
        }


app = FastAPI()


@app.get(f"/{api_token}/")
def read_root():
    return {"Hello": "World"}


@app.get(f"/{api_token}/prjs/")
def get_prjs():
    return Ido().get_prjs(skip_db=True)