import os
import requests
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from typing import Union

logger = logging.getLogger(__name__)

STATE_MAPPING = {
    'upcoming': 'upcoming',
}


def api_prj():
    """
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
            if (not _.get('registrationStarted')) \
                or (_.get('type') == 'completed') \
                or (_.get('registrationEndDate') and datetime.fromtimestamp(_.get('registrationEndDate')/1e3) < datetime.now()):
                continue
            prjs[_.get('routeName')] = _
    
    logger.info(f'Received {len(prjs)}')
    return prjs


def transform(raw):
    """
    {
        ...
    }
    to
    {
        "lpad": 'ape',
        "token": "befi",
        "reg_at": 123871242,
        "type": "upcoming",
        "reg_at": 1708239600000,
        "buy_at": 1708239600000,
        'claim_at': None,
    }
    """

    return {
        "lpad": 'ape',
        "token": raw.get('routeName'),
        "state": STATE_MAPPING.get(raw.get('type'), raw.get('type')),
        "reg_at": datetime.fromtimestamp(raw.get('registrationEndDate')/1e3),
        "buy_at": datetime.fromtimestamp(raw.get('registrationEndDate')/1e3),
        "claim_at": None,
    }


def pg_connect():
    """
    """
    
    conn = None
    try:
        # connect to the PostgreSQL server
        uri = os.getenv('DB_URI')
        print(f"Connecting to the PostgreSQL database... {os.getenv('DB_URI')}")
        conn = psycopg2.connect(uri)
    except Exception as error:
        print(error)

    print("Connection successful")
    return conn


def save_db(pg_conn, data):
    """
    """
    cols = data[0].keys()
    rows = [ [_.get(col) for col in cols] for _ in data ]
    # print(rows)

    _cols = ', '.join(cols)
    y_cols = ', '.join([f'news.{_}' if not _.endswith('at') else f'news.{_}::timestamptz' for _ in cols])
    query = f'''
        WITH news as (
            SELECT * FROM ( VALUES %s ) X ( {_cols} )
        ), 
        filtered as (
            SELECT 
                {y_cols}
            FROM prj
                RIGHT JOIN news using (lpad, token)
            WHERE 
                prj.state != news.state
                OR COALESCE(prj.reg_at, '1970-01-01'::timestamptz)  != COALESCE(news.reg_at::timestamptz, '1970-01-01'::timestamptz)
                OR COALESCE(prj.buy_at, '1970-01-01'::timestamptz)  != COALESCE(news.buy_at::timestamptz, '1970-01-01'::timestamptz)
                OR COALESCE(prj.claim_at, '1970-01-01'::timestamptz) != COALESCE(news.claim_at::timestamptz, '1970-01-01'::timestamptz)
        )
        INSERT INTO prj ({_cols})
        SELECT {_cols} FROM filtered
        ON CONFLICT (lpad, token) DO UPDATE
            SET 
                state = EXCLUDED.state,
                reg_at = EXCLUDED.reg_at,
                buy_at = EXCLUDED.buy_at,
                claim_at = EXCLUDED.claim_at
        RETURNING *
    '''
    # print(query)
    with pg_conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cursor:
        ret = psycopg2.extras.execute_values(cursor, query, rows, fetch=True)
        pg_conn.commit()
        # print(ret)
    
    return ret and list(ret)


def get_prjs():

    # Get project from API
    res = []
    prjs = api_prj()

    # Cleaning
    for k, v in prjs.items():
        prj = transform(v)
        res.append(prj)
    
    # Save to db
    ret = []
    if res and 0:
        conn = pg_connect()
        if conn:
            try:
                ret = save_db(conn, res)
            finally:
                conn.close()

    return ret


if __name__ == '__main__':
    
    logging.basicConfig(level=logging.INFO)

    get_prjs()