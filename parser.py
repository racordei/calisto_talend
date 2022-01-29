import glob
import json
import os
import shutil
import time
import warnings
from datetime import datetime, timedelta
from timeit import default_timer as timer

import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv

filename = '.env.dev' if os.getenv('PYTHON_ENV') == 'development' else '.env'
load_dotenv(find_dotenv(filename))


BASE_URL = os.getenv('BASE_URL')
PATH_MASK = os.getenv('PATH_MASK')


def dateisofromstring(datestr: str):
    if datestr == '':
        return None
    return datetime.strptime(datestr, '%d-%b-%y %H:%M:%S').isoformat()


def dateisofromdate(date: datetime):
    if date is None or not pd.notna(date):
        return None
    return date.isoformat()


def gethexid(hexid: str) -> str:
    if hexid is None or hexid == 'nan':
        return None
    return hexid[1:]


def status2code(status: str) -> int:
    return 200


def parseFile(file):
    success = True

    print(f'parsing [{os.path.basename(file)}] file...')

    df = pd.read_excel(file, engine='openpyxl')

    for col in ['Tipo produto', 'ID Adicional', 'Comentário',
                'Plástico', 'PlanningID', 'PackageID',
                'Process start', 'Process end', 'Process costtime', 'InitCMF', 'InitCMF info']:
        df[col] = df[col].fillna('')
        df[col] = df[col].astype(str)

    for col in ['Qtd Agilizada', 'Qtd Excluída', 'SequenceInPackage']:
        df[col] = df[col].fillna(0)
        df[col] = df[col].astype(int)

    rows = []
    uploaded = 0
    size = len(df)

    for rowId, row in df.iterrows():
        rows.append({
            "customername": row[0],
            "inputfilename": row[1],
            "batchname": row[2],
            "hexid": gethexid(row[3]),
            "productname": row[4],
            "productalias": row[5],
            "producttype": 2 if row[6] == "Adicional" else 1,
            "productstatus": row[7],
            "additionalhexid": gethexid(row[8]),
            "creationdate": dateisofromdate(row[9]),
            "duedate": dateisofromdate(row[10]),
            "finisheddate": dateisofromdate(row[11]),
            "shippingdate": dateisofromdate(row[13]),
            "comments": row[15],
            "wo_status": status2code(row[18]),
            "quantity": row[20],
            "priorityquantity": row[46],
            "excludedquantity": row[47],
            "mainplastic": row[21],
            "planningid": gethexid(row[22]),
            "packageid": row[23],
            "sequenceinpackage": row[24],
            "processstartdate": dateisofromstring(row[48]),
            "processenddate": dateisofromstring(row[49]),
            "processcosttime": row[50],
            "initcmfdate": dateisofromstring(row[51]),
            "initcmfinfo": row[52],
            "matchingstartdate": dateisofromdate(row[53]),
            "matchingenddate": dateisofromdate(row[54])
        })

        # print(json.dumps(rows[len(rows) - 1]))
        if (len(rows) >= 2000 or size == rowId + 1):
            start = timer()
            print(f'- uploading {len(rows):0>4d} rows... ', end='')
            resp = requests.post(f"{BASE_URL}/workorder", json=rows)
            end = timer()

            if not resp.ok:
                # print(f'Failed to update ID: {gethexid(row[3])} from: {row[0]}')
                print(resp.text)
                success = False
            else:
                uploaded = uploaded + len(rows)

            expended = timedelta(seconds=end-start)
            print(f'OK ({expended}) ({rowId + 1} objects uploaded)')
            rows = []

    return (success, uploaded)


minutes = 5
while True:
    try:
        files = glob.glob(PATH_MASK)

        start = timer()
        totalUploadedQuantity = 0
        for file in files:
            success = True
            uploadedFileQuantity = 0
            fileStart = timer()
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                (success, uploadedFileQuantity) = parseFile(file)
            if success:
                parsed = os.path.join(os.path.dirname(file), 'parsed')
                os.makedirs(parsed, exist_ok=True)
                shutil.move(file, os.path.join(parsed, os.path.basename(file)))
                totalUploadedQuantity = totalUploadedQuantity + uploadedFileQuantity
            fileEnd = timer()
            expended = timedelta(seconds=fileEnd-fileStart)
            print(
                f':: Uploaded {uploadedFileQuantity} records for {os.path.basename(file)} file in {expended}...')

        if len(files) > 0:
            end = timer()
            curtime = datetime.now().strftime("%d %H:%M:%S")
            expended = timedelta(seconds=end-start)
            print(
                f'{curtime} :: All files parsed in {expended} ({totalUploadedQuantity} records uploaded)...')
    except Exception as e:
        print(str(e))

    curtime = datetime.now().strftime("%d %H:%M:%S")
    print(f'{curtime} :: Will lookup for new files in {minutes} minutes...')
    time.sleep(minutes * 60)
