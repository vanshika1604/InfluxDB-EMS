import json
from influxdb_client import Point, WritePrecision
import pandas as pd
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
        # cache_key = f"get_assetconfig:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.get_assetconfig()
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/get_assetconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_assetconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_assetconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_assetconfig: " + str(ex))
        return ex


@app.route('/emsadminapi/v1/get_assetattributes', methods=['GET'])
def get_assetattributes():
    try:
        # cache_key = f"get_assetattributes:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.get_assetattributes()
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/get_assetattributes ")
        LOG.INFO("Response /emsadminapi/v1/get_assetattributes ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_assetattributes" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_assetattributes: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/get_shopattributes', methods=['GET'])
def get_shopattributes():
    try:
        # cache_key = f"get_shopattributes:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.get_shopattributes()
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/get_shopattributes ")
        LOG.INFO("Response /emsadminapi/v1/get_shopattributes ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_shopattributes: " + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_shopattributes: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/get_assetfaultruleconfig', methods=['GET'])
def get_assetfaultruleconfig():
    try:
        # cache_key = f"get_assetfaultruleconfig:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.get_assetfaultruleconfig()
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/get_assetfaultruleconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_assetfaultruleconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_assetfaultruleconfig: " + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_assetfaultruleconfig " + str(ex))
        return ex

@app.route('/emsadminapi/v1/post_assetconfig', methods=['POST'])
def post_assetconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = f"post_assetconfig:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_assetconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_assetconfig ")
        LOG.INFO("Response /emsadminapi/v1/post_assetconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_assetconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_assetconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/post_assetattributes', methods=['POST'])
def post_assetattributes():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = f"post_assetattributes:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_assetattributes(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_assetattributes ")
        LOG.INFO("Response /emsadminapi/v1/post_assetattributes ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_assetattributes" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_assetattributes: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/post_shopattributes', methods=['POST'])
def post_shopattributes():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = f"post_shopattributes:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_shopattributes(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_shopattributes ")
        LOG.INFO("Response /emsadminapi/v1/post_shopattributes ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_shopattributes" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_shopattributes: " + str(ex))
        return ex 

@app.route('/emsadminapi/v1/post_assetfaultruleconfig', methods=['POST'])
def post_assetfaultruleconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = f"post_assetfaultruleconfig:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_assetfaultruleconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_assetfaultruleconfig ")
        LOG.INFO("Response /emsadminapi/v1/post_assetfaultruleconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_assetfaultruleconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_assetfaultruleconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/put_assetconfig', methods=['PUT'])
def put_assetconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        print(data)
        # dict_data2 = request.get_json()
        # temp = json.dumps(dict_data2)
        # data2 = json.loads(temp)
        
        # cache_key = f"put_assetconfig:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.put_assetconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/put_assetconfig")
        LOG.INFO("Response /emsadminapi/v1/put_assetconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/put_assetconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/put_assetconfig: " + str(ex))
        return ex
    

@app.route('/emsadminapi/v1/put_assetattributes', methods=['PUT'])
def put_assetattributes():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        print(data)
        # dict_data2 = request.get_json()
        # temp = json.dumps(dict_data2)
        # data2 = json.loads(temp)
        
        # cache_key = f"put_assetattributes:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.put_assetattributes(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/put_assetattributes")
        LOG.INFO("Response /emsadminapi/v1/put_assetattributes ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/put_assetattributes: " + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/put_assetattributes: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/put_shopattributes', methods=['PUT'])
def put_shopattributes():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        print(data)
        
        # cache_key = f"put_shopattributes:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.put_shopattributes(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/put_shopattributes")
        LOG.INFO("Response /emsadminapi/v1/put_shopattributes ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/put_shopattributes: " + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/put_shopattributes: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/put_assetfaultruleconfig', methods=['PUT'])
def put_assetfaultruleconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        print(data)
        
        # cache_key = f"put_assetfaultruleconfig:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.put_assetfaultruleconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/put_assetfaultruleconfig")
        LOG.INFO("Response /emsadminapi/v1/put_assetfaultruleconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/put_assetfaultruleconfig: " + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/put_assetfaultruleconfig: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/delete_assetconfig', methods=['DELETE'])
def delete_assetconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        print(data)
        # dict_data2 = request.get_json()
        # temp = json.dumps(dict_data2)
        # data2 = json.loads(temp)
        
        # cache_key = f"delete_assetconfig:{timerange}:{asset}"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
        output = dbService.delete_assetconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/delete_assetconfig")
        LOG.INFO("Response /emsadminapi/v1/delete_assetconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/delete_assetconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/delete_assetconfig: " + str(ex))
        return ex
    
if __name__ == '__main__':
    serve(app, host="localhost", port=3005)

