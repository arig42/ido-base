import os
import logging
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)




class IdoBase:
    
    @staticmethod
    def call_api():
        """
            Calling API for prjs
            ex:
            ```
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
            ```
        """
        raise(Exception('Not Implemented'))
    
    @staticmethod
    def transform(raw):
        """
        Transform prj to match db schema
        ex:
        ```
            STATE_MAPPING = {
                'upcoming': 'upcoming',
                'active': 'upcoming',
            }

            return {
                'lpad': 'ape',
                'token': raw.get('routeName'),
                'state': STATE_MAPPING.get(raw.get('type'), raw.get('type')),
                'reg_at': datetime.fromtimestamp(raw.get('registrationEndDate')/1e3),
                'buy_at': datetime.fromtimestamp(raw.get('registrationEndDate')/1e3),
                'claim_at': None,
            }
        ```
        """
        raise(Exception('Not Implemented'))

    @staticmethod
    def pg_connect():
        """
        Connect to DB
        """
        
        conn = None
        try:
            # connect to the PostgreSQL server
            uri = os.getenv('DB_URI')
            logger.info(f"Connecting to the PostgreSQL database... {os.getenv('DB_URI').split('@')[1]}")
            conn = psycopg2.connect(uri)
        except Exception as error:
            logger.exception(error)

        logger.info("Connection successful")
        return conn

    @staticmethod
    def save_db(pg_conn, data):
        """
        Persistent records to db
        """

        cols = data[0].keys()
        rows = [ [_.get(col) for col in cols] for _ in data ]

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
        logger.debug(query)
        with pg_conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cursor:
            ret = psycopg2.extras.execute_values(cursor, query, rows, fetch=True)
            pg_conn.commit()
        
        return ret and list(ret)

    def get_prjs(self, skip_db=False):

        # Get data from API
        res = []
        prjs = self.call_api() or {}

        # Transform data
        for k, v in prjs.items():
            prj = self.transform(v)
            res.append(prj)
        
        # Save to db
        ret = []
        if res and (not skip_db):
            conn = self.pg_connect()
            if conn:
                try:
                    ret = self.save_db(conn, res)
                finally:
                    conn.close()

        return ret if not skip_db else res
