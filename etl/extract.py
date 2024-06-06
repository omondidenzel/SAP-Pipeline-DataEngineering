import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import os
import sys
from datetime import datetime as dt
import logging 
logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv(find_dotenv())

# import db connection
utilis_path = os.getenv('UTILIS_PATH')
sys.path.append(utilis_path)

# API Credentials config
erp_username = os.getenv('USER_NAME')
erp_password = os.getenv('PASSWORD')

top = 300000
offset = 0
data_list = []

while True:
    sap_url = f"https://my356712.sapbydesign.com/sap/byd/odata/ana_businessanalytics_analytics.svc/RPZ5D003BA6D34200EA39FBF6QueryResults?$select=CCOMPANY_UUID,CCREATION_DATE,CPOSTING_DATE,CACC_DOC_UUID,COEDREF_F_ID,CGLACCT,TGLACCT,FCDEBIT_CURRCOMP,FCCREDIT_CURRCOMP&$top={top}&$skip=0&$format=json"
    print(f'{dt.now()} -- Requesting: -> ', {sap_url})
    dataset = requests.get(sap_url, auth=(erp_username, erp_password))

    if offset == 0: # len(dataset.json()['d']['results'])  to be replaced to allow pagination
        log.info('Request complete')
        # print(len(dataset.json()['d']['results']))
        data_list.extend(dataset.json()['d']['results'])        
        break

    # print(data_list)  
    offset += 100000

# print(data_list)
columns = ['CCOMPANY_UUID','CCREATION_DATE','CPOSTING_DATE','CACC_DOC_UUID','COEDREF_F_ID','CGLACCT','TGLACCT','FCDEBIT_CURRCOMP','FCCREDIT_CURRCOMP']

df = pd.DataFrame(
    data = data_list,
    columns = columns
)

# Transformation
df.rename(columns = {
    'CCOMPANY_UUID':'company_id', 
    'CCREATION_DATE':'creation_date',
    'CPOSTING_DATE':'posting_date', 
    'CACC_DOC_UUID':'journal_id',
    'COEDREF_F_ID':'sourcedoc_id', 
    'CGLACCT':'account_id',
    'TGLACCT':'account_name', 
    'FCDEBIT_CURRCOMP':'debit',
    'FCCREDIT_CURRCOMP':'credit'
}, inplace = True
)

def replaceString(value_currency):
    value_currency = str(value_currency.replace(' XOF', '')).replace(',', '').replace('.', '')
    value_currency = str(value_currency.replace(' USD', '')).replace(',', '').replace('.', '')
    value_currency = str(value_currency.replace(' KES', '')).replace(',', '').replace('.', '')
    value_currency = pd.to_numeric(value_currency, downcast='float')
    return value_currency
    
def transformDate(value_date):
    value_date = int(value_date.replace('/Date(', '').replace(')/', ''))
    value_date = pd.Timestamp.fromtimestamp(value_date / 1000)
    value_date = pd.to_datetime(value_date, format="%Y-%m-%d %H:%M:%S.%f")
    return value_date

def change_content(value_name):
    value_name = str(value_name.replace('BF1', 'West-Africa-B'))
    value_name = str(value_name.replace('CD1', 'East-Africa-D'))
    value_name = str(value_name.replace('KE2', 'East-Africa-K'))
    return value_name

df['credit'] = df['credit'].apply(replaceString)
df['debit'] = df['debit'].apply(replaceString)
df['creation_date'] = df['creation_date'].apply(transformDate)
df['posting_date'] = df['posting_date'].apply(transformDate)
df['company_id'] = df['company_id'].apply(change_content)

# adding column to capture extraction date 
df['datetime_extracted'] = dt.now()

# Drop account name
df = df.drop(columns=['account_name'], axis=1)

# print(df.head())

log.info('Data loaded')

# Loading to csv
df.to_csv('etl/dataset.csv',index=False)

log.info('Data saved in CSV')

# Save to to db 
import utilis