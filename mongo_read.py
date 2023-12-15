from pymongo import MongoClient
import pandas as pd
import time

def connection():

    CONNECTION_STRING = "mongodb+srv://root:root@cluster0.acekv.mongodb.net/test?authSource=admin&replicaSet=atlas-svib4e-shard-0&readPreference=primary&ssl=true"
    client = MongoClient(CONNECTION_STRING)
    result = client['bi_testing']['census_data'].aggregate([{'$project': {'_id': 0}}])
    result = pd.DataFrame(result)

    result = result.fillna(0)
    # print(result)

    result=result.to_dict(orient='records')
    print(result)

    client['pipeline']['mng_tensor_stg'].insert_many(result)
    # time.sleep(15)

    client.close()

connection()