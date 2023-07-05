# from database_connect import mongo_operation as mongo

# client_url= "mongodb+srv://abhyudayadev123:abhyudayadev123@cluster0.wmnzurj.mongodb.net/?retryWrites=true&w=majority"
# database_name = "sensor"


# def upload_files_to_mongodb(
#     mongo_client_con_string,
#     database_name,
#     datasets_dir_name):
  
#   for file in os.listdir(datasets_dir_name):
#     if file.endswith('.csv'):
#       file_name = file.split('.')[0]

#       mongo_connection = mongo(
#           client_url = mongo_client_con_string,
#           database_name= database_name,
#           collection_name= file_name
#       )

#       file_path = os.path.join(datasets_dir_name, file_name)
#       mongo_connection.bulk_insert(file)
#       print(f"{file_name} is uploaded to mongodb")


# upload_files_to_mongodb(
#     mongo_client_con_string= client_url,
#     database_name = database_name,
#     datasets_dir_name= "/content"
# )



from pymongo.mongo_client import MongoClient
import json
uri = "mongodb+srv://abhyudayadev123:abhyudayadev123@cluster0.wmnzurj.mongodb.net/?retryWrites=true&w=majority"
# database_name="DEV"
# connection_name="waferfault"
# Create a new client and connect to the server
client = MongoClient(uri)
database_name="DEV"
connection_name="waferfault"
## read data
df=pd.read_csv(r"/config/workspace/notebook/wafer.csv")

df.drop("Unnamed: 0",axis=1)

json_records=list(json.loads(df.T.to_json()).values())
client[database_name][connection_name].insert_many(json_records)
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)