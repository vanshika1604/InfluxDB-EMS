import json
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

                         l = from(bucket: "59830e07-71a6-4ff0-9531-1f9fc4813fe0")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Asset" and r["Table"] == "AssetConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["asset_name", "asset_id"])
  
                         r = from(bucket: "59830e07-71a6-4ff0-9531-1f9fc4813fe0")
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

                         b = from(bucket: "59830e07-71a6-4ff0-9531-1f9fc4813fe0")
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

    def post_assetconfig(self, data):
        try:
            # def serialize_point_to_json(point):
            #     json_point = {
            #         "tag": point.tag,
            #         "field": point.field
            #      }
            #     return json.dumps(json_point)

            self.points = []
            points = self.points
            asset_name = data["asset_name"]
            shop_name = data["shop_name"]
            factory_name = data["factory_name"]
            org_name = data["org_name"]

            #Writing the asset name from AssetConfig
            point = Point("ConfigData")
            point.tag("Table", "AssetConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            print(point)
            # json_point = serialize_point_to_json(point)
            # print(json_point)
            points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            #Writing the shop name from ShopConfig
            point = Point("ConfigData")
            point.tag("Table", "ShopConfig")
            point.tag("Tag1", "Shop")
            point.field("shop_name", shop_name)
            print(point)
            points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            #Writing the factory name from FactoryConfig
            point = Point("ConfigData")
            point.tag("Table", "FactoryConfig")
            point.tag("Tag1", "Factory")
            point.field("factory_name", factory_name)
            print(point)
            points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            #Writing the org name from OrgConfig
            point = Point("ConfigData")
            point.tag("Table", "OrgConfig")
            point.tag("Tag1", "Org")
            point.field("org_name", org_name)
            print(point)
            points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)
            print(points)
            return points
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

            #Writing the asset name from AssetConfig
            point = Point("ConfigData")
            point.measurement("ConfigData")
            point.tag("Table", "AssetConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            self.points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

            #Writing the attribute name from AssetAttributes
            point = Point("ConfigData")
            point.measurement("ConfigData")
            point.tag("Table", "AssetAttributes")
            point.tag("Tag1", "Asset")
            point.field("attribute_name", attribute_name)
            self.points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
        
            return self.points
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

            #Writing the asset name from AssetConfig
            point = Point("ConfigData")
            point.measurement("ConfigData")
            point.tag("Table", "ShopConfig")
            point.tag("Tag1", "Shop")
            point.field("shop_name", shop_name)
            self.points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

            #Writing the attribute name from AssetAttributes
            point = Point("ConfigData")
            point.measurement("ConfigData")
            point.tag("Table", "ShopAttributes")
            point.tag("Tag1", "Shop")
            point.field("attribute_name", attribute_name)
            self.points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
            return self.points
        
        except Exception as ex:
            print("\nFailed to write shop attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write shop attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def post_assetfaultruleconfig(self, data):
        try:
            self.points = []
            asset_name = data["asset_name"]
            rule_name = data["rule_name"]
            condition = data["condition"]
            alert = data["alert"]
            action = data["action"]

            #Writing the asset name from AssetConfig
            point = Point("ConfigData")
            point.measurement("ConfigData")
            point.tag("Table", "AssetConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            self.points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

            #Writing the attribute name from AssetAttributes
            point = Point("ConfigData")
            point.measurement("ConfigData")
            point.tag("Table", "AssetAlertRules")
            point.tag("Tag1", "Asset")
            point.field("rule_id", rule_name)
            point.field("condition", condition)
            point.field("alert", alert)
            point.field("action", action)
            self.points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
        
            return self.points
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
            shop_name = data["shop_name"]
            factory_name = data["factory_name"]
            org_name = data["org_name"]

            #Writing the asset name from AssetConfig
            point = Point("ConfigData")
            point.tag("Table", "AssetConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            point.time( )
            print(point)
            # json_point = serialize_point_to_json(point)
            # print(json_point)
            points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            #Writing the shop name from ShopConfig
            point = Point("ConfigData")
            point.tag("Table", "ShopConfig")
            point.tag("Tag1", "Shop")
            point.field("shop_name", shop_name)
            point.time( )
            print(point)
            points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            #Writing the factory name from FactoryConfig
            point = Point("ConfigData")
            point.tag("Table", "FactoryConfig")
            point.tag("Tag1", "Factory")
            point.field("factory_name", factory_name)
            point.time( )
            print(point)
            points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)

            #Writing the org name from OrgConfig
            point = Point("ConfigData")
            point.tag("Table", "OrgConfig")
            point.tag("Tag1", "Org")
            point.field("org_name", org_name)
            point.time( )
            print(point)
            points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= point)
            print(points)
            return points
        except Exception as ex:
            print("\nFailed to update asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            pass