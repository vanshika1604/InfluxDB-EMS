import json
import uuid
import influxdb
import pandas as pd
import pytz
from CFG.ConfigHandler import ConfigHandler
from LOGS.LogsManager import Log
import influxdb_client
from influxdb_client import Point
import os
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

class DbService:
    def __init__(self):
        try:
            self.LOG = Log()
            print("\nInitializing InfluxDB Service " + str(os.path.basename(__file__)))
            self.LOG.INFO("Initializing InfluxDB Service " + str(os.path.basename(__file__)))
            config_handler = ConfigHandler()
            self.configdata = config_handler.config_read()
            self.token = os.environ.get('INFLUX_DB_API_TOKEN', self.configdata['Influx']['Influx_db_API_token'])
            self.url = os.environ.get('INFLUX_URL', self.configdata['Influx']['Influx_url'])
            self.org = os.environ.get('INFLUX_ORG', self.configdata['Influx']['Influx_org'])
            self.bucket = os.environ.get('INFLUX_BUCKET', self.configdata['Influx']['Influx_bucket'])
            self.batch_size = os.environ.get('BATCH_SIZE', self.configdata['Influx']['batch_size'])
            self.range = os.environ.get('RANGE', self.configdata['Influx']['range'])
            self.client = influxdb_client.InfluxDBClient(url=self.url, token=self.token, org=self.org, timeout=100000)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()
            self.utc_format_str = '%a, %d %b %Y %H:%M:%S %Z'
            self.local_format_str = '%Y-%m-%d %H:%M:%S%z'
            self.local_tz = pytz.timezone('Asia/Kolkata')
        except Exception as ex:
            self.LOG.ERROR("Connection to InfluxDB Failed " + str(os.path.basename(__file__)) + str(ex))
            print("\nConnection to InfluxDB Failed " + str(os.path.basename(__file__)) + str(ex))

    # def get_assetconfig(self, timerange, asset):
    #     final_output = []
    #     try:
    #         query = '''
    #                     from(bucket: \"''' + self.bucket + '''\")
    #                             |> range(start: 0)
    #                             |> filter(fn: (r) => r["_measurement"] == "ConfigData")
    #                             |> filter(fn: (r) => r["Tag1"] == "Asset")
    #                             |> filter(fn: (r) => r["Table"] == "AssetConfig")
    #                             |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    #                 '''
    #         print("\n Querying for get_assetconfig\n")
    #         # print(str(query))
    #         data_stream = self.query_api.query_data_frame(query)
    #         if len(data_stream) > 0:
    #             data_stream = data_stream.round(2)
    #             data_stream.drop(columns=['Tag1', 'Table'], inplace=True)
    #             # convert DataFrame to list of dictionaries
    #             for index, row in data_stream.iterrows():
    #                 datetime_object = row["_time"]
    #                 # datetime_object = datetime.strptime(row["_time"], self.utc_format_str)
    #                 str(datetime_object)
    #                 local_time = str(datetime_object.replace(tzinfo=pytz.utc).astimezone(self.local_tz))
    #                 datetime_object = row["_time"]
    #                 # datetime_object = datetime.strptime(row["_time"], self.utc_format_str)
    #                 str(datetime_object)
    #                 row_dict = row.to_dict()
    #                 # row_dict["read_timestamp"] = local_time
    #                 row_dict["year"] = str(datetime_object.strftime("%Y"))
    #                 row_dict["month"] = str(datetime_object.strftime("%b"))
    #                 row_dict["date"] = str(datetime_object.strftime("%d"))
    #                 for e in ['_time']:
    #                     row_dict.pop(e, 'no key found')
    #                 final_output.append(row_dict)
    #         return final_output
    #     except Exception as ex:
    #         self.LOG.ERROR(
    #             "Asset Config Data Read Operation in Influxdb Failed " + str(os.path.basename(__file__)) + str(ex))
    #         print("\nAsset Config Data Read Operation in Influxdb Failed " + str(os.path.basename(__file__)) + str(ex))
    #         return final_output

    def get_assetconfig(self):
        try:
            # print("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            # self.LOG.INFO("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            query = '''
                        import "join"
                        left = from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetConfig")
                                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                                |> keep(columns: ["asset_name", "asset_id","shop_id"])   

                        right = from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopConfig")
                                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                                |> keep(columns: ["shop_name", "shop_id","factory_id"])  

                         a= join.inner(
                               left: left ,
                               right: right,
                               on: (l, r) => l.shop_id == r.shop_id,
                               as: (l, r) => ({l with shop_id: r.shop_id ,shop_name: r.shop_name, factory_id: r.factory_id}),
                           ) 

                        left1 = from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Factory" and r["Table"] == "FactoryConfig")
                                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                                |> keep(columns: ["factory_name", "factory_id","org_id"])   

                        right2 = from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Org" and r["Table"] == "OrgConfig")
                                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                                |> keep(columns: ["org_name", "org_id"])  

                          b=join.inner(
                               left: left1 ,
                               right: right2,
                               on: (l, r) => l.org_id == r.org_id,
                               as: (l, r) => ({l with org_id: r.org_id , org_name : r.org_name}),
                           )

                        join.inner(
                               left: a ,
                               right: b,
                               on: (l, r) => l.factory_id == r.factory_id,
                               as: (l, r) => ({l with factory_id: r.factory_id , org_id : r.org_id, factory_name: r.factory_name, org_name : r.org_name}),
                        )
                    '''
            # print("\n Querying for getConfigData\n")
            # print(str(query))
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            df_list = df.to_dict('records')
            json_string = json.dumps(df_list)
            # print(data_frame.head())
            return json_string
        except Exception as ex:
            print("\nFailed to read asset config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read asset config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def get_assetattributes(self):
        try:
            # print("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            # self.LOG.INFO("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            query = '''
                        import "join"

                         l = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["asset_name", "asset_id"])
  
                         r = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetAttributeMapping")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["assetattribute_id", "asset_id"])   

                        a=join.inner(
                                    left: l ,
                                    right: r,
                                    on: (l, r) => l.asset_id == r.asset_id,
                                    as: (l, r) => ({l with assetattribute_id: r.assetattribute_id}),
                                 ) 

                         b = from(bucket: \"''' + self.bucket + '''\")
                           |> range(start: 0)
                           |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetAttributes")               
                           |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                           |> keep(columns: ["assetattribute_id","attribute_name"]) 
                         
                         join.inner(
                                     left: a ,
                                     right: b,
                                     on: (l, r) => l.assetattribute_id == r.assetattribute_id,
                                     as: (l, r) => ({l with attribute_name: r.attribute_name}),
                                 )   
                    '''
            # print("\n Querying for getConfigData\n")
            # print(str(query))
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            df_list = df.to_dict('records')
            json_string = json.dumps(df_list)
            # print(data_frame.head())
            return json_string
        except Exception as ex:
            print("\nFailed to read asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass
        
    def get_shopattributes(self):
        try:
            # print("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            # self.LOG.INFO("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            query = '''
                        import "join"

                         l = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["shop_name", "shop_id"])
  
                         r = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopAttributeMapping")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["shopattribute_id", "shop_id"])   

                        a=join.inner(
                                    left: l ,
                                    right: r,
                                    on: (l, r) => l.shop_id == r.shop_id,
                                    as: (l, r) => ({l with shop_id: r.shop_id,
                                                    shopattribute_id: r.shopattribute_id}),
                                 ) 

                         b = from(bucket: \"''' + self.bucket + '''\")
                           |> range(start: 0)
                           |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopAttributes")                  
                           |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                           |> keep(columns: ["shopattribute_id","attribute_name"]) 
                         
                           
                         join.inner(
                                     left: a ,
                                     right: b,
                                     on: (l, r) => l.shopattribute_id == r.shopattribute_id,
                                     as: (l, r) => ({l with shopattribute_id: r.shopattribute_id ,
                                                     attribute_name: r.attribute_name}),
                                 )
                    '''
            # print("\n Querying for getConfigData\n")
            # print(str(query))
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            df_list = df.to_dict('records')
            json_string = json.dumps(df_list)
            # print(data_frame.head())
            return json_string
        except Exception as ex:
            print("\nFailed to read shop attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read shop attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass
        

    def get_assetfaultruleconfig(self):
        try:
            # print("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            # self.LOG.INFO("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            query = '''
                        import "join"

                         l = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["asset_name", "asset_id"])
  
                         r = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetRuleMapping")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["asset_id", "rule_id"])   

                        a= join.inner(
                                    left: l ,
                                    right: r,
                                    on: (l, r) => l.asset_id == r.asset_id,
                                    as: (l, r) => ({l with asset_id: r.asset_id,
                                                    rule_id: r.rule_id}),
                                 ) 

                         b = from(bucket: \"''' + self.bucket + '''\")
                           |> range(start: 0)
                           |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetAlertRules")              
                           |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                           |> keep(columns: ["condition","alert","action","rule_id"]) 
                         
                           
                         join.inner(
                                     left: a ,
                                     right: b,
                                     on: (l, r) => l.rule_id == r.rule_id,
                                     as: (l, r) => ({l with condition: r.condition ,
                                                     alert: r.alert, action: r.action}),
                                 )
                    '''
            
            # print("\n Querying for getConfigData\n")
            # print(str(query))
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            df_list = df.to_dict('records')
            json_string = json.dumps(df_list)
            # print(data_frame.head())
            return json_string
        except Exception as ex:
            print("\nFailed to read asset fault rule config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read asset fault rule config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass
    
    def get_assetmlconfig(self):
        try:
            # print("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            # self.LOG.INFO("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            query = '''
                        import "join"

                         l = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetMLModelConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["model_path", "ml_attributes", "asset_id"])
  
                         r = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetMLModelOp")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["std_dev", "asset_id", "mean", "type", "upper_limit", "lower_limit"])   

                        a=join.inner(
                                    left: l ,
                                    right: r,
                                    on: (l, r) => l.asset_id == r.asset_id,
                                    as: (l, r) => ({l with std_dev: r.std_dev,
                                                    mean: r.mean,
                                                    type: r.type,
                                                    upper_limit : r.upper_limit,
                                                    lower_limit : r.lower_limit}),           
                                 )

                        b= from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["asset_name", "asset_id"])  

                          join.inner(
                                    left: a ,
                                    right: b,
                                    on: (l, r) => l.asset_id == r.asset_id,
                                    as: (l, r) => ({l with asset_name: r.asset_name}),           
                                 )
                    '''
            # print("\n Querying for getConfigData\n")
            # print(str(query))
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            df_list = df.to_dict('records')
            json_string = json.dumps(df_list)
            # print(data_frame.head())
            return json_string
        except Exception as ex:
            print("\nFailed to read asset ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read asset ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def get_shopmlconfig(self):
        try:
            # print("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            # self.LOG.INFO("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            query = '''
                        import "join"

                         l = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopMLModelConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["model_path", "ml_attributes", "shop_id"])
  
                         r = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopMLModelOp")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["std_dev", "shop_id", "mean", "type", "upper_limit", "lower_limit", "shop_id"])   

                        a=join.inner(
                                    left: l ,
                                    right: r,
                                    on: (l, r) => l.shop_id == r.shop_id,
                                    as: (l, r) => ({l with std_dev: r.std_dev,
                                                    mean: r.mean,
                                                    type: r.type,
                                                    upper_limit : r.upper_limit,
                                                    lower_limit : r.lower_limit}),           
                                 )

                        b= from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["shop_name", "shop_id"])  

                          join.inner(
                                    left: a ,
                                    right: b,
                                    on: (l, r) => l.shop_id == r.shop_id,
                                    as: (l, r) => ({l with shop_name: r.shop_name}),           
                                 )
                    '''
            # print("\n Querying for getConfigData\n")
            # print(str(query))
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            df_list = df.to_dict('records')
            json_string = json.dumps(df_list)
            # print(data_frame.head())
            return json_string
        except Exception as ex:
            print("\nFailed to read shop ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read shop ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass


    def post_assetconfig(self, data):
        try:
            self.points = []
            asset_name = data["asset_name"]
            shop_name = data["shop_name"]
            factory_name = data["factory_name"]
            org_name = data["org_name"]
            org_id = str(uuid.uuid4())
            factory_id = str(uuid.uuid4())
            shop_id = str(uuid.uuid4())
            asset_id = str(uuid.uuid4())

            #Writing the org name from OrgConfig
            point = Point("ConfigData")
            point.tag("Table", "OrgConfig")
            point.tag("Tag1", "Org")
            point.field("org_name", org_name)
            point.field("org_id", org_id)
            point.field("synced", int(0))
            print(point)
            self.points.append(point)

            #Writing the factory name from FactoryConfig
            point = Point("ConfigData")
            point.tag("Table", "FactoryConfig")
            point.tag("Tag1", "Factory")
            point.field("factory_name", factory_name)
            point.field("factory_id", factory_id)
            point.field("org_id", org_id)
            point.field("synced", int(0))
            print(point)
            self.points.append(point)

            #Writing the shop name from ShopConfig
            point = Point("ConfigData")
            point.tag("Table", "ShopConfig")
            point.tag("Tag1", "Shop")
            point.field("shop_name", shop_name)
            point.field("shop_id", shop_id)
            point.field("factory_id", factory_id)
            point.field("synced", int(0))
            print(point)
            self.points.append(point)
        
            #Writing the asset name from AssetConfig
            point = Point("ConfigData")
            point.tag("Table", "AssetConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            point.field("asset_id", asset_id)
            point.field("shop_id", shop_id)
            point.field("synced", int(0))
            print(point)
            self.points.append(point)
            
            self.write_api.write(bucket=self.bucket, org=self.org, record= self.points)
            return "Insertion successful"
            
        except Exception as ex:
            print("\nFailed to write asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            pass


    def post_assetattributes(self, data):
        try:
            self.points = []
            asset_name = data["asset_name"]
            attribute_name = data["attribute_name"]
            assetattribute_id = str(uuid.uuid4())

            #Getting asset id from the asset name from AssetConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_name"] == \"''' + asset_name + '''\")                                        
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check == 0):
                return "Provided asset name does not exist."
            else:
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                asset_id = df.loc[df['asset_name']==asset_name, 'asset_id'].values[0]
                
            # Checking and Writing the attribute name from AssetAttributes
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetAttributes")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["attribute_name"] == \"''' + attribute_name + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check==0):
                point = Point("ConfigData")
                point.tag("Table", "AssetAttributes")
                point.tag("Tag1", "Asset")
                point.field("attribute_name", attribute_name)
                point.field("assetattribute_id", assetattribute_id)
                point.field("synced", int(0))
                self.points.append(point)    

                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
            else:
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                assetattribute_id = df.loc[df['attribute_name']==attribute_name, 'assetattribute_id'].values[0]
                # print(assetattribute_id)
            
            #Using Asset Attribute Mapping
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetAttributeMapping")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\" and r["assetattribute_id"] == \"''' + assetattribute_id + '''\")  
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check==0):
                point = Point("ConfigData")
                point.tag("Table", "AssetAttributeMapping")
                point.tag("Tag1", "Asset")
                point.field("asset_id", asset_id)
                point.field("assetattribute_id", assetattribute_id)
                point.field("synced", int(0))
                print(point)
                self.points.append(point)
                print(self.points)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion successful."
            
            else:
                return "You are trying to insert duplicate values."
        
        except Exception as ex:
            print("\nFailed to write asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass


    def post_shopattributes(self, data):
        try:
            self.points = []
            shop_name = data["shop_name"]
            attribute_name = data["attribute_name"]
            shopattribute_id = str(uuid.uuid4())

            #Getting shop id from the shop name from ShopConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_name"] == \"''' + shop_name + '''\")                                        
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check == 0):
                return "Provided shop name does not exist."
            else:
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                shop_id = df.loc[df['shop_name']==shop_name, 'shop_id'].values[0]
                
            # Checking and Writing the attribute name from ShopAttributes
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopAttributes")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["attribute_name"] == \"''' + attribute_name + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check==0):
                point = Point("ConfigData")
                point.tag("Table", "ShopAttributes")
                point.tag("Tag1", "Shop")
                point.field("attribute_name", attribute_name)
                point.field("shopattribute_id", shopattribute_id)
                point.field("synced", int(0))
                self.points.append(point)    

                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
            else:
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                shopattribute_id = df.loc[df['attribute_name']==attribute_name, 'shopattribute_id'].values[0]
                # print(shopattribute_id)
            
            #Using Shop Attribute Mapping
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopAttributeMapping")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\" and r["shopattribute_id"] == \"''' + shopattribute_id + '''\")  
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check==0):
                point = Point("ConfigData")
                point.tag("Table", "ShopAttributeMapping")
                point.tag("Tag1", "Shop")
                point.field("shop_id", shop_id)
                point.field("shopattribute_id", shopattribute_id)
                point.field("synced", int(0))
                self.points.append(point)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion successful."
            else:
                return "You are trying to insert duplicate values."
        
        except Exception as ex:
            print("\nFailed to write shop attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write shop attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def post_assetfaultruleconfig(self, data):
        try:
            self.points = []
            asset_name = data["asset_name"]
            condition = data["condition"]
            alert = data["alert"]
            action = data["action"]
            rule_id = str(uuid.uuid4())

            #Getting asset id from the asset name from AssetConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_name"] == \"''' + asset_name + '''\")                                        
                    '''
            
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check == 0):
                return "Provided asset name does not exist."
            else:
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                asset_id = df.loc[df['asset_name']==asset_name, 'asset_id'].values[0]

            # Checking and Writing the attribute name from AssetAlertRules
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetAlertRules")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["condition"] == \"''' + condition + '''\" and r["alert"] == \"''' + alert + '''\" and r["action"] == \"''' + action + '''\" )                                      
                    '''
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check==0):
                point = Point("ConfigData")
                point.tag("Table", "AssetAlertRules")
                point.tag("Tag1", "Asset")
                point.field("condition", condition)
                point.field("alert", alert)
                point.field("action", action)
                point.field("rule_id", rule_id)
                point.field("synced", int(0))
                
                self.points.append(point)    

                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
            else:
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                rule_id = df.loc[df['condition']==condition, 'rule_id'].values[0]
                # print(assetattribute_id)
           
            #Writing in AssetRuleMapping
            query = '''
                        from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetRuleMapping")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
                        |> filter(fn: (r) => r["rule_id"] == \"''' + rule_id + '''\")  
                    '''
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check==0):
                point = Point("ConfigData")
                point.tag("Table", "AssetRuleMapping")
                point.tag("Tag1", "Asset")
                point.field("asset_id", asset_id)
                point.field("rule_id", rule_id)
                point.field("synced", int(0))
                self.points.append(point)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion successful."
            else:
                return "You are trying to insert duplicate values."
        except Exception as ex:
            print("\nFailed to write asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def post_assetmlconfig(self, data):
        try:
            self.points = []
            asset_name = data["asset_name"]
            model_path = data["model_path"]
            ml_attributes = data["ml_attributes"]
            std_dev = data["std_dev"]
            mean = data["mean"]
            type = data["type"]

            asset_id = str(uuid.uuid4())

            #Writing the asset name from AssetConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_name"] == \"''' + asset_name + '''\")
                        |> count(column: "asset_name")                                           
                    '''
            
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check == 0):
                point = Point("ConfigData")
                point.measurement("ConfigData")
                point.tag("Table", "AssetConfig")
                point.tag("Tag1", "Asset")
                point.field("asset_name", asset_name)
                point.field("asset_id", asset_id)
                point.field("synced", int(0))
                self.points.append(point)

            #Write asset_id, ml_attribute, model_path
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")
                        |> count(column: "asset_id")                                           
                    '''

            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)

            if(check == 0):
                point = Point("ConfigData")
                point.measurement("ConfigData")
                point.tag("Table", "AssetMLModelConfig")
                point.tag("Tag1", "Asset")
                point.field("model_path", model_path)
                point.field("ml_attributes", ml_attributes)
                point.field("std_dev", std_dev)
                point.field("mean", mean)
                point.field("type", type)
                point.field("asset_id", asset_id)
                point.field("synced", int(0))
                self.points.append(point)

            #Write asset_id, type, mean
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")
                        |> count(column: "asset_id")                                           
                    '''

            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)

            if(check == 0):
                point = Point("ConfigData")
                point.measurement("ConfigData")
                point.tag("Table", "AssetMLModelOp")
                point.tag("Tag1", "Asset")
                point.field("mean", mean)
                point.field("type", type)
                point.field("asset_id", asset_id)
                point.field("synced", int(0))
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion succesful"
            else:
                print("You are trying to insert duplicate values.")
        except Exception as ex:
            print("\nFailed to write asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def post_assetdisablemlconfig(self, data):
        try:
            self.points = []
            asset_name = data["asset_name"]
            upper_limit = data["upper_limit"]
            lower_limit = data["lower_limit"]
            mean = data["mean"]
            type = data["type"]

            asset_id = str(uuid.uuid4())

            #Writing the asset name from AssetConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_name"] == \"''' + asset_name + '''\")
                        |> count(column: "asset_name")                                           
                    '''
            
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check == 0):
                point = Point("ConfigData")
                point.measurement("ConfigData")
                point.tag("Table", "AssetConfig")
                point.tag("Tag1", "Asset")
                point.field("asset_name", asset_name)
                point.field("asset_id", asset_id)
                point.field("synced", int(0))
                self.points.append(point)

            #Write 
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")
                        |> count(column: "asset_id")                                           
                    '''

            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)

            if(check == 0):
                point = Point("ConfigData")
                point.measurement("ConfigData")
                point.tag("Table", "AssetMLModelOp")
                point.tag("Tag1", "Asset")
                point.field("upper_limit", upper_limit)
                point.field("lower_limit", lower_limit)
                point.field("mean", mean)
                point.field("type", type)
                point.field("asset_id", asset_id)
                point.field("synced", int(0))
                self.points.append(point)

                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion succesful"
            else:
                print("You are trying to insert duplicate values.")
        except Exception as ex:
            print("\nFailed to write asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_assetconfig(self, data):
        try:
            self.points = []
            points = self.points

            asset_name = data["asset_name"]
            asset_id = data["asset_id"]
            shop_name = data["shop_name"]
            shop_id = data["shop_id"]
            factory_name = data["factory_name"]
            factory_id = data["factory_id"]
            org_name = data["org_name"]
            org_id = data["org_id"]

            # Updating the asset name from AssetConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['asset_id']==asset_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "AssetConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            point.time(time)
            print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            # Updating the shop name from ShopConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['shop_id']==shop_id, '_time'].values[0]
            time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "ShopConfig")
            point.tag("Tag1", "Shop")
            point.field("shop_name", shop_name)
            point.time(time)
            print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            # Updating the factory name from FactoryConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "FactoryConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Factory")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["factory_id"] == \"''' + factory_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['factory_id']==factory_id, '_time'].values[0]
            time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "FactoryConfig")
            point.tag("Tag1", "Factory")
            point.field("factory_name", factory_name)
            point.time(time)
            print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            # Updating the org name from OrgConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "OrgConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Org")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["org_id"] == \"''' + org_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['org_id']==org_id, '_time'].values[0]
            time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "OrgConfig")
            point.tag("Tag1", "Org")
            point.field("org_name", org_name)
            point.time(time)
            print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)
            return points
        except Exception as ex:
            print("\nFailed to update asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_assetattributes(self, data):
        try:
            self.points = []
            asset_name = data["asset_name"]
            asset_id = data["asset_id"]
            attribute_name = data["attribute_name"]
            attribute_id = data["attribute_id"]

            # Updating the asset name from AssetConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['asset_id']==asset_id, '_time'].values[0]
            time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "AssetConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            point.time(time)
            # print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            # Updating the attribute name from AssetAttributes
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetAttributes")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["attribute_id"] == \"''' + attribute_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['attribute_id']==attribute_id, '_time'].values[0]
            time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "AssetAttributes")
            point.tag("Tag1", "Asset")
            point.field("attribute_name", attribute_name)
            point.time(time)
            # print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)
        
            return self.points
        except Exception as ex:
            print("\nFailed to update asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass


    def put_shopattributes(self, data):
        try:
            self.points = []
            shop_name = data["shop_name"]
            shop_id = data["shop_id"]
            attribute_name = data["attribute_name"]
            attribute_id = data["attribute_id"]

            # Updating the shop name from ShopConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['shop_id']==shop_id, '_time'].values[0]
            time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "ShopConfig")
            point.tag("Tag1", "Shop")
            point.field("shop_name", shop_name)
            point.time(time)
            # print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            # Updating the attribute name from ShopAttributes
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopAttributes")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["attribute_id"] == \"''' + attribute_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['attribute_id']==attribute_id, '_time'].values[0]
            time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "ShopAttributes")
            point.tag("Tag1", "Shop")
            point.field("attribute_name", attribute_name)
            point.time(time)
            # print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)
        
            return self.points
        except Exception as ex:
            print("\nFailed to update asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_assetfaultruleconfig(self, data):
        try:
            self.points = []
            asset_name = data["asset_name"]
            condition = data["condition"]
            alert = data["alert"]
            action = data["action"]
            rule_id = data["rule_id"]

            # Updating the condition, alert, action, asset_name from AssetFaultRuleConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetFaultRuleConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["rule_id"] == \"''' + rule_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['attribute_id']==attribute_id, '_time'].values[0]
            time = df.loc[0, "_time"]
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "AssetFaultRuleConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            point.field("condition", condition)
            point.field("alert", alert)
            point.field("action", action)
            point.time(time)
            # print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)
        
            return self.points
        except Exception as ex:
            print("\nFailed to update asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def delete_assetconfig(self, data):
        try:

            asset_id = data["asset_id"]
            shop_id = data["shop_id"]
            factory_id = data["factory_id"]
            org_id = data["org_id"]

            # Deleting the asset name from AssetConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['asset_id']==asset_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

            # Deleting the shop name from ShopConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['shop_id']==shop_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

            # Deleting the factory name from FactoryConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "FactoryConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Factory")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["factory_id"] == \"''' + factory_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['factory_id']==factory_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

            # Deleting the org name from OrgConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "OrgConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Org")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["org_id"] == \"''' + org_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['org_id']==org_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
        except Exception as ex:
            print("\nFailed to delete asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to delete asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            pass

    def delete_assetattributes(self, data):
        try:
            asset_id = data["asset_id"]
            attribute_id = data["attribute_id"]

            # Deleting the asset name from AssetConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['asset_id']==asset_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

            # Deleting the attribute name from AssetAttributes
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetAttributes")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["attribute_id"] == \"''' + attribute_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['attribute_id']==attribute_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
        
        except Exception as ex:
            print("\nFailed to delete asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to delete asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def delete_shopattributes(self, data):
        try:
            shop_id = data["shop_id"]
            attribute_id = data["attribute_id"]

            # Deleting the shop name from ShopConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['shop_id']==shop_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

            # Deleting the attribute name from ShopAttributes
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopAttributes")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["attribute_id"] == \"''' + attribute_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['attribute_id']==attribute_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
        
        except Exception as ex:
            print("\nFailed to delete asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to delete asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def delete_assetfaultruleconfig(self, data):
        try:
            rule_id = data["rule_id"]

            # Deleting the condition, alert, action, asset_name from AssetFaultRuleConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetFaultRuleConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["rule_id"] == \"''' + rule_id + '''\")
                        '''
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
            # time = df.loc[0, "_time"]
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
        
        except Exception as ex:
            print("\nFailed to delete asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to delete asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

