import json
import uuid
import influxdb
from numpy import double
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
                                    as: (l, r) => ({l with shopattribute_id: r.shopattribute_id}),
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

    
    def get_shifconfig(self):
        try:
            query = '''
                        from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "ShiftConfig")
                                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                                |> keep(columns: ["shift_id", "shift_title","from", "to"])   
                    '''
            print(query)
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
            print("\nFailed to read shift config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read shift config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def get_kpiconfig(self):
        try:
            query = '''
                        from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "KPIColorConfig")
                                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                                |> keep(columns: ["color", "id","range"])   
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
            print("\nFailed to read kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def get_whatsappconfig(self):
        try:
            # print("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            # self.LOG.INFO("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            query = '''
                        import "join"
                         l = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "WhatsappNotificationConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["whatsapp_id", "priority","enable", "shop_id"])
  
                         r = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["shop_id", "shop_name"])   

                        join.inner(
                                    left: l ,
                                    right: r,
                                    on: (l, r) => l.shop_id == r.shop_id,
                                    as: (l, r) => ({l with shop_id: r.shop_id,
                                                    shop_name: r.shop_name}),
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
            print("\nFailed to whatsapp notification config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read whatsapp notification config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def get_emailconfig(self):
        try:
            # print("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            # self.LOG.INFO("\nReading asset and shop config details from influx" + str(os.path.basename(__file__)))
            query = '''
                        import "join"

                         l = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "EmailNotificationConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["email_id", "priority","enable", "shop_id"])
  
                         r = from(bucket: \"''' + self.bucket + '''\")
                          |> range(start: 0)
                          |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Shop" and r["Table"] == "ShopConfig")
                          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                          |> keep(columns: ["shop_id", "shop_name"])   

                        join.inner(
                                    left: l ,
                                    right: r,
                                    on: (l, r) => l.shop_id == r.shop_id,
                                    as: (l, r) => ({l with shop_id: r.shop_id,
                                                    shop_name: r.shop_name}),
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
            print("\nFailed to email notification config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to read email notification config details from influx" + str(os.path.basename(__file__)) + str(ex))
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
                self.points = []
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
                self.points = []
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
                self.points = []
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
                self.points = []
                point = Point("ConfigData")
                point.tag("Table", "ShopAttributeMapping")
                point.tag("Tag1", "Shop")
                point.field("shop_id", shop_id)
                point.field("shopattribute_id", shopattribute_id)
                point.field("synced", int(0))
                print(point)
                self.points.append(point)
                print(self.points)
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

            # Checking and Writing the attribute name from AssetAlertRules
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetAlertRules")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["condition"] == \"''' + condition + '''\" and r["alert"] == \"''' + alert + '''\" and r["action"] == \"''' + action + '''\" )                                      
                    '''
            
            print(query)
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
                point.field("consecutive", "false")
                point.field("delay", int(0))
                point.field("synced", int(0))
                
                self.points.append(point)    
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                
            else:
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                rule_id = df.loc[df['alert']==alert, 'rule_id'].values[0]
                # print(assetattribute_id)
           
            #Writing in AssetRuleMapping
            self.points =[]
            query = '''
                        from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetRuleMapping")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
                        |> filter(fn: (r) => r["rule_id"] == \"''' + rule_id + '''\")  
                    '''
            print(query)
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
            model = data["model"]
            
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

            #If model is not present
            if(model == "no"):
                upper_limit = data["upper_limit"]
                lower_limit = data["lower_limit"]
                type = data["type"]

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["upper_limit"] == ''' + str(upper_limit) + ''' and r["lower_limit"] == ''' + str(lower_limit) + ''' and r["type"] == \"''' + type + '''\" )                                     
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                check = len(data_frame)
                print(check)
                # If unique values are provided, write points to table
                if(check == 0):
                    # Write model = no against newly formed asset_id in ML model config
                    self.points =[]
                    point = Point("ConfigData")
                    point.tag("Table", "AssetMLModelConfig")
                    point.tag("Tag1", "Asset")
                    point.field("asset_id", asset_id)
                    point.field("model", "no")
                    self.points.append(point)
                    print(point)
                    print(self.points)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

                    #Write asset_id, type, upper_limit and lower_limit in ML model Op
                    self.points =[]
                    point = Point("ConfigData")
                    point.tag("Table", "AssetMLModelOp")
                    point.tag("Tag1", "Asset")
                    point.field("upper_limit", int(upper_limit))
                    point.field("lower_limit", int(lower_limit))
                    point.field("type", type)
                    point.field("asset_id", asset_id)
                    point.field("mean", float(0))
                    # point.field("range_end", int(0))
                    # point.field("range_start", int(0))
                    point.field("std_dev", float(0))
                    point.field("synced", int(0))
                    self.points.append(point)
                    print(point)
                    # print(self.points)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                    return "Insertion successful"
                else:
                    return "You are trying to insert duplicate values."
                
            # If model is present
            elif(model == "yes"):
                #Checking and writing ml_attributes and model_path from AssetMLModelConfig
                model_path = data["model_path"]
                type = data["type"]
                std_dev = data["std_dev"]
                mean = data["mean"]
                ml_attributes = data["ml_attributes"]

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["model_path"] == \"''' + model_path + '''\" and r["ml_attributes"] == \"''' + ml_attributes + '''\" and r["mean"] == ''' + str(mean) + ''' and r["std_dev"] == ''' + str(std_dev) + ''')                                     
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                check = len(data_frame)
                print(check)

                # First check and write model_path and model_attributes
                if(check == 0):
                    self.points =[]
                    point = Point("ConfigData")
                    point.measurement("ConfigData")
                    point.tag("Table", "AssetMLModelConfig")
                    point.tag("Tag1", "Asset")
                    point.field("ml_attributes", ml_attributes)
                    point.field("model_path", model_path)
                    point.field("model", "yes")
                    point.field("multiplier", "[1,2,3]")
                    point.field("pickle_path", model_path)
                    point.field("asset_id", asset_id)
                    point.field("synced", int(0))
                    print(point)
                    self.points.append(point)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

                    #Write type, mean, std_dev from AssetMLModelOp
                    self.points =[]
                    point = Point("ConfigData")
                    point.measurement("ConfigData")
                    point.tag("Table", "AssetMLModelOp")
                    point.tag("Tag1", "Asset")
                    point.field("upper_limit", int(0))
                    point.field("lower_limit", int(0))
                    point.field("type", type)
                    point.field("mean", float(mean))
                    point.field("asset_id", asset_id)
                    point.field("range_end", int(0))
                    point.field("range_start", int(0))
                    point.field("std_dev", float(std_dev))
                    point.field("synced", int(0))
                    print(point)
                    self.points.append(point)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                    return "Insertion successful"
                else:
                    return "You are trying to insert duplicate values."
            else:
                return "ERROR: Case Sensitive, Please write either yes or no only for model field."

        except Exception as ex:
                print("\nFailed to write asset ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
                self.LOG.ERROR(
                "\nFailed to write asset ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
        pass


    def post_shopmlconfig(self, data):
        try:
            self.points = []
            shop_name = data["shop_name"]
            model = data["model"]
            
            #Getting shop id from the shop name from ShopConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_name"] == \"''' + shop_name + '''\")                                        
                    '''
            
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check == 0):
                return "Provided shop name does not exist."
            else:
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                shop_id = df.loc[df['shop_name']==shop_name, 'shop_id'].values[0]

            #If model is not present
            if(model == "no"):
                upper_limit = data["upper_limit"]
                lower_limit = data["lower_limit"]
                type = data["type"]

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["upper_limit"] == ''' + str(upper_limit) + ''' and r["lower_limit"] == ''' + str(lower_limit) + ''' and r["type"] == \"''' + type + '''\" )                                     
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                check = len(data_frame)
                print(check)
                # If unique values are provided, write points to table
                if(check == 0):
                    # Write model = no against newly formed shop_id in ML model config
                    self.points =[]
                    point = Point("ConfigData")
                    point.tag("Table", "ShopMLModelConfig")
                    point.tag("Tag1", "Shop")
                    point.field("shop_id", shop_id)
                    point.field("model", "no")
                    self.points.append(point)
                    print(point)
                    print(self.points)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

                    #Write shop_id, type, upper_limit and lower_limit in ML model Op
                    self.points =[]
                    point = Point("ConfigData")
                    point.tag("Table", "ShopMLModelOp")
                    point.tag("Tag1", "Shop")
                    point.field("upper_limit", int(upper_limit))
                    point.field("lower_limit", int(lower_limit))
                    point.field("type", type)
                    point.field("shop_id", shop_id)
                    point.field("mean", float(0))
                    # point.field("range_end", int(0))
                    # point.field("range_start", int(0))
                    point.field("std_dev", float(0))
                    point.field("synced", int(0))
                    self.points.append(point)
                    print(point)
                    # print(self.points)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                    return "Insertion successful"
                else:
                    return "You are trying to insert duplicate values."
                
            # If model is present
            elif(model == "yes"):
                #Checking and writing ml_attributes and model_path from ShopMLModelConfig
                model_path = data["model_path"]
                type = data["type"]
                std_dev = data["std_dev"]
                mean = data["mean"]
                ml_attributes = data["ml_attributes"]

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopMLModelConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["model_path"] == \"''' + model_path + '''\" and r["ml_attributes"] == \"''' + ml_attributes + '''\" and r["mean"] == ''' + str(mean) + ''' and r["std_dev"] == ''' + str(std_dev) + ''')                                     
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                check = len(data_frame)
                print(check)

                # First check and write model_path and model_attributes
                if(check == 0):
                    self.points =[]
                    point = Point("ConfigData")
                    point.measurement("ConfigData")
                    point.tag("Table", "ShopMLModelConfig")
                    point.tag("Tag1", "Shop")
                    point.field("ml_attributes", ml_attributes)
                    point.field("model_path", model_path)
                    point.field("model", "yes")
                    point.field("multiplier", "[1,2,3]")
                    point.field("pickle_path", model_path)
                    point.field("shop_id", shop_id)
                    point.field("synced", int(0))
                    print(point)
                    self.points.append(point)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

                    #Write type, mean, std_dev from ShopMLModelOp
                    self.points =[]
                    point = Point("ConfigData")
                    point.measurement("ConfigData")
                    point.tag("Table", "ShopMLModelOp")
                    point.tag("Tag1", "Shop")
                    point.field("upper_limit", int(0))
                    point.field("lower_limit", int(0))
                    point.field("type", type)
                    point.field("mean", float(mean))
                    point.field("shop_id", shop_id)
                    point.field("range_end", int(0))
                    point.field("range_start", int(0))
                    point.field("std_dev", float(std_dev))
                    point.field("synced", int(0))
                    print(point)
                    self.points.append(point)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                    return "Insertion successful"
                else:
                    return "You are trying to insert duplicate values."
            else:
                return "ERROR: Case Sensitive, Please write either yes or no only for model field."

        except Exception as ex:
                print("\nFailed to write shop ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
                self.LOG.ERROR(
                "\nFailed to write shop ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
        pass

    def post_shiftconfig(self, data):
        try:
            shift_schedule = data["shift_schedule"]
            _from = data["from"]
            _to = data["to"]
            shift_title = data["shift_title"]
            shift_id = str(uuid.uuid4())

            #Getting shift id from the shift name from ShiftConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "ShiftConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
                        |> filter(fn: (r) => r["from"] == ''' + str(_from) + ''' and r["to"] == ''' + str(_to) + ''' and r["shift_title"] == \"''' + shift_title + '''\" and r["shift_schedule"] == ''' + str(shift_schedule) + ''')                                        
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check==0):
                self.points=[]
                point = Point("ConfigData")
                point.tag("Table", "ShiftConfig")
                point.tag("Tag1", "Common")
                point.field("shift_schedule", int(shift_schedule))
                point.field("shift_title", shift_title)
                point.field("shift_id", shift_id)
                point.field("from", int(_from))
                point.field("to", int(_to))
                point.field("synced", int(0))
                self.points.append(point)    
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion successful."
            else:
                return "You are trying to insert duplicate values."
        
        except Exception as ex:
            print("\nFailed to write shift config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write shift config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def post_kpiconfig(self, data):
        try:
            range = data["range"]
            color = data["color"]
            id = str(uuid.uuid4())
                
            # Checking and Writing the range and color from KPIColorConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "KPIColorConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["range"] == \"''' + range + '''\" and r["color"] == \"''' + color + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            if(check==0):
                self.points = []
                point = Point("ConfigData")
                point.tag("Table", "KPIColorConfig")
                point.tag("Tag1", "Common")
                point.field("range", range)
                point.field("color", color)
                point.field("id", id)
                point.field("synced", int(0))
                self.points.append(point)    
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion successful."
            
            else:
                return "You are trying to insert duplicate values."
        
        except Exception as ex:
            print("\nFailed to write kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass
    
    def post_emailconfig(self, data):
        try:
            # shop_no = data["shop_no"]
            enable = data["enable"]
            email_id = data["email_id"]
            priority = data["priority"]
            shop_id = str(uuid.uuid4())

            if(enable=="Enable"):
                value = "yes"
            elif(enable=="Disable"):
                value = "no"
            else:
                return "You can only write either Enable or Disable"
                
            # Checking and Writing the enable, email_id, priority from EmailNotificationConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "EmailNotificationConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["enable"] == \"''' + value + '''\" and r["email_id"] == \"''' + email_id + '''\" and r["priority"] == \"''' + priority + '''\" )                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            
            if(check==0):
                self.points = []
                point = Point("ConfigData")
                point.tag("Table", "EmailNotificationConfig")
                point.tag("Tag1", "Common")
                point.field("email_id", email_id)
                point.field("enable", value)
                point.field("priority", priority)
                point.field("shop_id", shop_id)
                point.field("synced", int(0))
                self.points.append(point)    
                print(self.points)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion successful."
            
            else:
                return "You are trying to insert duplicate values."
        
        except Exception as ex:
            print("\nFailed to write email config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write email config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def post_whatsappconfig(self, data):
        try:
            enable = data["enable"]
            whatsapp_id = data["whatsapp_id"]
            priority = data["priority"]

            shop_id = str(uuid.uuid4())

            if(enable=="Enable"):
                value = "yes"
            elif(enable=="Disable"):
                value = "no"
            else:
                return "You can only write either Enable or Disable"
                
            # Checking and Writing the enable, whatsapp_id, priority from WhatsappNotificationConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "WhatsappNotificationConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["enable"] == \"''' + value + '''\" and r["whatsapp_id"] == \"''' + whatsapp_id + '''\" and r["priority"] == \"''' + priority + '''\" )                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            check = len(data_frame)
            print(check)
            
            if(check==0):
                self.points = []
                point = Point("ConfigData")
                point.tag("Table", "WhatsappNotificationConfig")
                point.tag("Tag1", "Common")
                point.field("whatsapp_id", whatsapp_id)
                point.field("enable", value)
                point.field("priority", priority)
                point.field("shop_id", shop_id)
                point.field("synced", int(0))
                self.points.append(point)    
                print(self.points)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Insertion successful."
            
            else:
                return "You are trying to insert duplicate values."
        
        except Exception as ex:
            print("\nFailed to write whatsapp config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to write whatsapp config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_assetconfig(self, data):
        try:
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
            # time = df.loc[df['asset_id']==asset_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            self.points = []
            point = Point("ConfigData")
            point.tag("Table", "AssetConfig")
            point.tag("Tag1", "Asset")
            point.field("asset_name", asset_name)
            point.time(time)
            print(point)
            self.points.append(point)    
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= self.points)
            print("Asset value updated")

            # Updating the shop name from ShopConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")
                        '''
            print()
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['shop_id']==shop_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            self.points = []
            point = Point("ConfigData")
            point.tag("Table", "ShopConfig")
            point.tag("Tag1", "Shop")
            point.field("shop_name", shop_name)
            point.time(time)
            print(point)
            self.points.append(point)    
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= self.points)
            print("Shop value updated")

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
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            print(time)
            self.points = []
            point = Point("ConfigData")
            point.tag("Table", "FactoryConfig")
            point.tag("Tag1", "Factory")
            point.field("factory_name", factory_name)
            point.time(time)
            print(point)
            self.points.append(point)    
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= self.points)

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
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            self.points = []
            point = Point("ConfigData")
            point.tag("Table", "OrgConfig")
            point.tag("Tag1", "Org")
            point.field("org_name", org_name)
            point.time(time)
            print(point)
            self.points.append(point)    
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= self.points)
            return "Values updated successfully"
        except Exception as ex:
            print("\nFailed to update asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update asset config details from influx: " + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_assetattributes(self, data):
        try:
            self.points = []
            attribute_name = data["attribute_name"]
            assetattribute_id = data["assetattribute_id"]

            # Updating the attribute name from AssetAttributes
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetAttributes")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["assetattribute_id"] == \"''' + assetattribute_id + '''\")
                        '''
            print(query)
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # temptime = df.loc[df['assetattribute_id'] == assetattribute_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            print(time)
            self.points = []
            point = Point("ConfigData")
            point.tag("Table", "AssetAttributes")
            point.tag("Tag1", "Asset")
            point.field("attribute_name", attribute_name)
            point.time(time)
            self.points.append(point)    
            # print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= self.points)
            return "Values updated successfully"
        
        except Exception as ex:
            print("\nFailed to update asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update asset attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_shopattributes(self, data):
        try:
            self.points = []
            attribute_name = data["attribute_name"]
            shopattribute_id = data["shopattribute_id"]

            # Updating the attribute name from ShopAttributes
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopAttributes")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shopattribute_id"] == \"''' + shopattribute_id + '''\")
                        '''
            print(query)
            
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # temptime = df.loc[df['shopattribute_id'] == shopattribute_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            print(time)
            self.points = []
            point = Point("ConfigData")
            point.tag("Table", "ShopAttributes")
            point.tag("Tag1", "Shop")
            point.field("attribute_name", attribute_name)
            point.time(time)
            self.points.append(point)    
            # print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= self.points)
            return "Values updated successfully"
        
        except Exception as ex:
            print("\nFailed to update shop attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update shop attributes details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_assetfaultruleconfig(self, data):
        try:
            self.points=[]
            condition = data["condition"]
            alert = data["alert"]
            action = data["action"]
            rule_id = data["rule_id"]

            # Updating the condition, alert, action, asset_name from AssetFaultRuleConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetAlertRules")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["rule_id"] == \"''' + rule_id + '''\")
                        '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            point = Point("ConfigData")
            point.tag("Table", "AssetFaultRuleConfig")
            point.tag("Tag1", "Asset")
            point.time(time)
            point.field("condition", condition)
            point.field("alert", alert)
            point.field("action", action)
            self.points.append(point)    
            print(point)
            # points.append(point)
            self.write_api.write(bucket=self.bucket, org=self.org, record= self.points)
            return "Values updated successfully"
        except Exception as ex:
            print("\nFailed to update asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_assetmlconfig(self, data):
        try:
            self.points = []
            asset_id = data["asset_id"]
            model = data["model"]

            #If model is not present
            if(model == "no"):
                upper_limit = data["upper_limit"]
                lower_limit = data["lower_limit"]
                type = data["type"]

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")                                       
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
                temptime = df.loc[0, "_time"]
                time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")

                #Update asset_id, type, upper_limit and lower_limit in ML model Op
                self.points =[]
                point = Point("ConfigData")
                point.tag("Table", "AssetMLModelOp")
                point.tag("Tag1", "Asset")
                point.field("upper_limit", int(upper_limit))
                point.field("lower_limit", int(lower_limit))
                point.field("type", type)
                point.time(time)
                self.points.append(point)
                print(point)
                # print(self.points)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Values updated successfully"
                
            # If model is present
            elif(model == "yes"):

                #Updating ml_attributes and model_path from AssetMLModelConfig
                model_path = data["model_path"]
                type = data["type"]
                std_dev = data["std_dev"]
                mean = data["mean"]
                ml_attributes = data["ml_attributes"]

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")                                        
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
                temptime = df.loc[0, "_time"]
                time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")

                # Update model_path and model_attributes
                self.points =[]
                point = Point("ConfigData")
                point.tag("Table", "AssetMLModelConfig")
                point.tag("Tag1", "Asset")
                point.field("ml_attributes", ml_attributes)
                point.field("model_path", model_path)
                point.time(time)
                print(point)
                self.points.append(point)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

                #Update type, mean, std_dev from AssetMLModelOp
                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\")                                        
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
                temptime = df.loc[0, "_time"]
                time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
                self.points =[]
                point = Point("ConfigData")
                point.tag("Table", "AssetMLModelOp")
                point.tag("Tag1", "Asset")
                point.field("type", type)
                point.field("mean", float(mean))
                point.field("std_dev", float(std_dev))
                point.time(time)
                print(point)
                self.points.append(point)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Values updated successfully"
            else:
                return "ERROR: Case Sensitive, Please write either yes or no only for model field."

        except Exception as ex:
                print("\nFailed to write asset ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
                self.LOG.ERROR(
                "\nFailed to write asset ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
        pass

    def put_shopmlconfig(self, data):
        try:
            self.points = []
            shop_id = data["shop_id"]
            model = data["model"]

            #If model is not present
            if(model == "no"):
                upper_limit = data["upper_limit"]
                lower_limit = data["lower_limit"]
                type = data["type"]

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")                                       
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
                temptime = df.loc[0, "_time"]
                time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")

                #Update shop_id, type, upper_limit and lower_limit in ML model Op
                self.points =[]
                point = Point("ConfigData")
                point.tag("Table", "ShopMLModelOp")
                point.tag("Tag1", "Shop")
                point.field("upper_limit", int(upper_limit))
                point.field("lower_limit", int(lower_limit))
                point.field("type", type)
                point.time(time)
                self.points.append(point)
                print(point)
                # print(self.points)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Values updated successfully"
                
            # If model is present
            elif(model == "yes"):

                #Updating ml_attributes and model_path from ShopMLModelConfig
                model_path = data["model_path"]
                type = data["type"]
                std_dev = data["std_dev"]
                mean = data["mean"]
                ml_attributes = data["ml_attributes"]

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopMLModelConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")                                        
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
                temptime = df.loc[0, "_time"]
                time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")

                # Update model_path and model_attributes
                self.points =[]
                point = Point("ConfigData")
                point.tag("Table", "ShopMLModelConfig")
                point.tag("Tag1", "Shop")
                point.field("ml_attributes", ml_attributes)
                point.field("model_path", model_path)
                point.time(time)
                print(point)
                self.points.append(point)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)

                #Update type, mean, std_dev from ShopMLModelOp
                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")                                        
                    '''
                print(query)
                data_frame = self.query_api.query_data_frame(query)
                data_frame.drop(['result', 'table'], axis=1, inplace=True)
                df = pd.DataFrame(data_frame)
                # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
                temptime = df.loc[0, "_time"]
                time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
                self.points =[]
                point = Point("ConfigData")
                point.tag("Table", "ShopMLModelOp")
                point.tag("Tag1", "Shop")
                point.field("type", type)
                point.field("mean", float(mean))
                point.field("std_dev", float(std_dev))
                point.time(time)
                print(point)
                self.points.append(point)
                self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
                return "Values updated successfully"
            else:
                return "ERROR: Case Sensitive, Please write either yes or no only for model field."

        except Exception as ex:
                print("\nFailed to write shop ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
                self.LOG.ERROR(
                "\nFailed to write shop ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
        pass

    def put_shiftconfig(self, data):
        try:
            _from = data["from"]
            _to = data["to"]
            shift_id = data["shift_id"]

            #Getting shift id from the shift name from ShiftConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "ShiftConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
                        |> filter(fn: (r) => r["shift_id"] == \"''' + shift_id + '''\")                                        
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.loc[df['shift_id']==shift_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            self.points=[]
            point = Point("ConfigData")
            point.tag("Table", "ShiftConfig")
            point.tag("Tag1", "Common")
            point.field("from", int(_from))
            point.field("to", int(_to))
            point.field("synced", int(0))
            point.time(time)
            self.points.append(point)    
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
            return "Values updated successful."
        
        except Exception as ex:
            print("\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass
        
    def put_kpiconfig(self, data):
        try:
            self.points = []
            id = data["id"]
            range = data["range"]
            color = data["color"]
                
            # Checking and Writing the range and color from KPIColorConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "KPIColorConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["id"] == \"''' + id + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.loc[df['id']==id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            point = Point("ConfigData")
            point.tag("Table", "KPIColorConfig")
            point.tag("Tag1", "Common")
            point.time(time)
            point.field("range", range)
            point.field("color", color)  
            self.points.append(point)    
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
            return "Values updated successful."
            
        except Exception as ex:
            print("\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_emailconfig(self, data):
        try:
            self.points = []
            enable = data["enable"]
            email_id = data["email_id"]
            priority = data["priority"]
            shop_id = data["shop_id"]
                
            # Checking and Writing the range and color from EmailNotificationConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "EmailNotificationConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.loc[df['shop_id']==shop_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")

            point = Point("ConfigData")
            point.tag("Table", "EmailNotificationConfig")
            point.tag("Tag1", "Common")
            point.time(time)
            point.field("email_id", email_id) 
            point.field("enable", enable) 
            point.field("priority", priority)
            self.points.append(point)    
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
            return "Values updated successful."
            
        except Exception as ex:
            print("\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def put_whatsappconfig(self, data):
        try:
            self.points = []
            enable = data["enable"]
            whatsapp_id = data["whatsapp_id"]
            priority = data["priority"]
            shop_id = data["shop_id"]
                
            # Checking and Writing the range and color from WhatsappNotificationConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "WhatsappNotificationConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.loc[df['shop_id']==shop_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")

            point = Point("ConfigData")
            point.tag("Table", "WhatsappNotificationConfig")
            point.tag("Tag1", "Common")
            point.time(time)
            point.field("whatsapp_id", whatsapp_id) 
            point.field("enable", enable) 
            point.field("priority", priority)
            self.points.append(point)    
            self.write_api.write(bucket=self.bucket, org=self.org, record=self.points)
            return "Values updated successful."
            
        except Exception as ex:
            print("\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
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
            # time = df.loc[df['asset_id']==asset_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
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
            # time = df.loc[df['shop_id']==shop_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
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
            # time = df.loc[df['factory_id']==factory_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
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
            # time = df.loc[df['org_id']==org_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop= time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
            return "Values deleted successfully"
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
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
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
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
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
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
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
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
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
                        |> filter(fn: (r) => r["Table"] == "AssetAlertRules")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["rule_id"] == \"''' + rule_id + '''\")
                        '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
            return "Values deleted successfully"
        
        except Exception as ex:
            print("\nFailed to delete asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to delete asset fault rule details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def delete_assetmlconfig(self, data):
        try:
            self.points = []
            asset_id = data["asset_id"]
            model = data["model"]

            #If model is not present
            if(model == "no"):

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\" )                                     
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # time = df.loc[0, "_time"]
            # print(time)
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

            #Delete asset_id, type, upper_limit and lower_limit in ML model Op
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "AssetMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Asset")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["asset_id"] == \"''' + asset_id + '''\" )                                     
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # time = df.loc[0, "_time"]
            # print(time)
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

        except Exception as ex:
                print("\nFailed to write asset ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
                self.LOG.ERROR(
                "\nFailed to write asset ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
        pass

    def delete_shopmlconfig(self, data):
        try:
            self.points = []
            shop_id = data["shop_id"]
            model = data["model"]

            #If model is not present
            if(model == "no"):

                query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopMLModelConfig")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\" )                                     
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # time = df.loc[0, "_time"]
            # print(time)
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

            #Delete shop_id, type, upper_limit and lower_limit in ML model Op
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                        |> filter(fn: (r) => r["Table"] == "ShopMLModelOp")
                        |> filter(fn: (r) => r["Tag1"] == "Shop")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\" )                                     
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # time = df.loc[0, "_time"]
            # print(time)
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)

        except Exception as ex:
            print("\nFailed to write shop ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
            "\nFailed to write shop ml config details from influx" + str(os.path.basename(__file__)) + str(ex))
        pass

    def delete_shiftconfig(self, data):
        try:
            shift_id = data["shift_id"]

            #Getting shift id from the shift name from ShiftConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "ShiftConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
                        |> filter(fn: (r) => r["shift_id"] == \"''' + shift_id + '''\")                                        
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
            return "Values deleted successfully"
        
        except Exception as ex:
            print("\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass
        

    def delete_kpiconfig(self, data):
        try:
            self.points = []
            id = data["id"]
                
            # Checking and Writing the range and color from KPIColorConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "KPIColorConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["id"] == \"''' + id + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
            return "Values deleted successfully"
            
        except Exception as ex:
            print("\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def delete_emailconfig(self, data):
        try:
            self.points = []
            shop_id = data["shop_id"]
                
            # Checking and Writing the range and color from EmailNotificationConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "EmailNotificationConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
            return "Values deleted successfully"
            
        except Exception as ex:
            print("\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass

    def delete_whatsappconfig(self, data):
        try:
            self.points = []
            shop_id = data["shop_id"]
                
            # Checking and Writing the range and color from WhatsappNotificationConfig
            query = '''from(bucket: \"''' + self.bucket + '''\")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "ConfigData" and r["Tag1"] == "Common" and r["Table"] == "WhatsappNotificationConfig")
                        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> filter(fn: (r) => r["shop_id"] == \"''' + shop_id + '''\")                                      
                    '''
            print(query)
            data_frame = self.query_api.query_data_frame(query)
            data_frame.drop(['result', 'table'], axis=1, inplace=True)
            df = pd.DataFrame(data_frame)
            # time = df.get("_time")
            # time = df.loc[df['rule_id']==rule_id, '_time'].values[0]
            temptime = df.loc[0, "_time"]
            time = datetime.strftime(temptime, "%Y-%m-%dT%H:%M:%SZ")
            # print(time)
            # current_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            delete_api= self.client.delete_api()
            delete_api.delete(start = time, stop=time, predicate='_measurement = "ConfigData"',bucket=self.bucket, org=self.org)
            return "Values deleted successfully"
            
        except Exception as ex:
            print("\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            self.LOG.ERROR(
                "\nFailed to update kpi config details from influx" + str(os.path.basename(__file__)) + str(ex))
            pass
