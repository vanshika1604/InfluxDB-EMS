import json
from waitress import serve
from DB.DbService import DbService
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from LOGS.LogsManager import Log
import redis
from CFG.ConfigHandler import ConfigHandler
import os

app = Flask(__name__, template_folder='')
CORS(app)
cors = CORS(app, resources={
    r"/*": {
        "origin": "*"
    }
})

dbService = DbService()
LOG = Log()
config_handler = ConfigHandler()
configdata = config_handler.config_read()
# host = os.environ.get('REDIS_HOST', configdata['Redis']['redis_host'])
# port = os.environ.get('REDIS_PORT', configdata['Redis']['redis_port'])
# cache = redis.Redis(host=host, port=port)

@app.route('/emsadminapi/v1/get_assetconfig', methods=['GET'])
def get_assetconfig():
    try:
        if 'range' in request.args:
            timerange = request.args.get("range")
        else:
            timerange = "-7d"
        if 'asset' in request.args:
            asset = request.args.get("asset")
        else:
            asset = ""
        cache_key = f"get_assetconfig:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.get_assetconfig(timerange, asset)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/get_assetconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_assetconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_assetconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_assetconfig: " + str(ex))
        return ex

# @app.route('/emsadminapi/v1/get_assetattributes', methods=['GET'])
# def get_assetattributes():
#     try:
#         if 'range' in request.args:
#             timerange = request.args.get("range")
#         else:
#             timerange = "-7d"
#         if 'asset' in request.args:
#             asset = request.args.get("asset")
#         else:
#             asset = ""
#         cache_key = f"get_assetattributes:{timerange}:{asset}"
#         cached_data = cache.get(cache_key)
#         print(cache_key)
#         if cached_data:
#             return jsonify(json.loads(cached_data)), 200
#         output = dbService.get_assetattributes(timerange, asset)
#         cache.set(cache_key, json.dumps(output), ex=900)
#         print("Response /emsadminapi/v1/get_assetattributes ")
#         LOG.INFO("Response /emsadminapi/v1/get_assetattributes ")
#         return jsonify(output), 200
#     except Exception as ex:
#         print("Exception /emsadminapi/v1/get_assetattributes" + str(ex))
#         LOG.ERROR("Exception /emsadminapi/v1/get_assetattributes: " + str(ex))
#         return ex
    
if __name__ == '__main__':
    serve(app, host="localhost", port=3005)
