import pytz
from CFG.ConfigHandler import ConfigHandler
from LOGS.LogsManager import Log
import influxdb_client
import os
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime

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

    def get_assetconfig(self, timerange, asset):
        final_output = []
        try:
            query = '''
                        import "join"
                        left = from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                                |> filter(fn: (r) => r["Tag1"] == "Asset")
                                |> filter(fn: (r) => r["Table"] == "AssetConfig")
                                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                                |> keep(columns: ["asset_name", "asset_id","shop_id"])   

                        right = from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                                |> filter(fn: (r) => r["Tag1"] == "Shop")
                                |> filter(fn: (r) => r["Table"] == "ShopConfig")
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
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                                |> filter(fn: (r) => r["Tag1"] == "Factory")
                                |> filter(fn: (r) => r["Table"] == "FactoryConfig")
                                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                                |> keep(columns: ["factory_name", "factory_id","org_id"])   

                        right2 = from(bucket: \"''' + self.bucket + '''\")
                                |> range(start: 0)
                                |> filter(fn: (r) => r["_measurement"] == "ConfigData")
                                |> filter(fn: (r) => r["Tag1"] == "Org")
                                |> filter(fn: (r) => r["Table"] == "OrgConfig")
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
            print("\n Querying for get_assetconfig\n")
            # print(str(query))
            data_stream = self.query_api.query_data_frame(query)
            if len(data_stream) > 0:
                data_stream = data_stream.round(2)
                data_stream.drop(columns=['result', 'table'], inplace=True)
                # convert DataFrame to list of dictionaries
                for index, row in data_stream.iterrows():
                    datetime_object = datetime.strptime(row["read_timestamp"], self.utc_format_str)
                    local_time = str(datetime_object.replace(tzinfo=pytz.utc).astimezone(self.local_tz))
                    datetime_object = datetime.strptime(local_time, self.local_format_str)
                    row_dict = row.to_dict()
                    # row_dict["read_timestamp"] = local_time
                    row_dict["year"] = str(datetime_object.strftime("%Y"))
                    row_dict["month"] = str(datetime_object.strftime("%b"))
                    row_dict["date"] = str(datetime_object.strftime("%d"))
                    for e in ['read_timestamp']:
                        row_dict.pop(e, 'no key found')
                    final_output.append(row_dict)
            return final_output
        except Exception as ex:
            self.LOG.ERROR(
                "Asset Config Data Read Operation in Influxdb Failed " + str(os.path.basename(__file__)) + str(ex))
            print("\nAsset Config Data Read Operation in Influxdb Failed " + str(os.path.basename(__file__)) + str(ex))
            return final_output