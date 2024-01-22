import io
import json
import logging
import oracledb
import oci
import os
import pandas as pd
from fdk import response

def Load_Data_from_ObjectStorage_to_ADW():
    # Configuring Resource Principal
    signer = oci.auth.signers.get_resource_principals_signer()
    # Create an Object Storage Service client
    object_storage_client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    # Define Object Storage parameters
    namespace = "gc35013"
    bucket = "Extracted_Weather_Data"
    object_name = "weather_data.csv"
    # Get the object from Object Storage
    response = object_storage_client.get_object(namespace, bucket, object_name)
    # Read the object data (assuming it is CSV)
    data = pd.read_csv(io.BytesIO(response.data.content))

    with open('Wallet_Datawarehouse/adw_credentials.txt', 'r') as file:
        lines = file.readlines()

    username = lines[0].strip()
    adw_password = lines[1].strip()
    walletpassword = lines[2].strip()

    con = oracledb.connect(user=username, password=adw_password, dsn="datawarehouse_low",
                            config_dir="Wallet_Datawarehouse",
                            wallet_location="Wallet_Datawarehouse",
                            wallet_password=walletpassword)
    # Convert pandas DataFrame to a list of tuples
    data_tuples = list(data.itertuples(index=False, name=None))
    cursor = con.cursor()
    cursor.executemany( '''
            INSERT INTO WEATHERDATA 
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, TO_DATE(:19,'YYYY-MM-DD'))
        ''',data_tuples)
    # Committing the changes
    con.commit()
    # Closing the cursor
    cursor.close()
    # Closing the connection
    con.close()

    return ("Data has been loaded successfully from Object Storage to ADW")


def handler(ctx, data: io.BytesIO = None):
    try:
        data_load=Load_Data_from_ObjectStorage_to_ADW()
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))

    logging.getLogger().info("Data Loader Function")
    return response.Response(
        ctx, response_data=json.dumps(
            {"message":data_load}),
        headers={"Content-Type": "application/json"}
    )
