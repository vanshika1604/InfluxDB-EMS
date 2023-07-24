import json
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
host = os.environ.get('REDIS_HOST', configdata['Redis']['redis_host'])
port = os.environ.get('REDIS_PORT', configdata['Redis']['redis_port'])
cache = redis.Redis(host=host, port=port)

@app.route('/emsadminapi/v1/get_assetconfig', methods=['GET'])
def get_assetconfig():
    try:
        cache_key = "get_assetconfig"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_assetconfig()
        cache.set(cache_key, json.dumps(output), ex=5)
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
        cache_key = "get_assetattributes"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_assetattributes()
        cache.set(cache_key, json.dumps(output), ex=5)
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
        cache_key = f"get_shopattributes"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_shopattributes()
        cache.set(cache_key, json.dumps(output), ex=5)
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
        cache_key = "get_assetfaultruleconfig"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_assetfaultruleconfig()
        cache.set(cache_key, json.dumps(output), ex=5)
        print("Response /emsadminapi/v1/get_assetfaultruleconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_assetfaultruleconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_assetfaultruleconfig: " + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_assetfaultruleconfig " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/get_assetmlconfig', methods=['GET'])
def get_assetmlconfig():
    try:
        cache_key = "get_assetmlconfig"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_assetmlconfig()
        cache.set(cache_key, json.dumps(output), ex=5)
        print("Response /emsadminapi/v1/get_assetmlconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_assetmlconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_assetmlconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_assetmlconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/get_shopmlconfig', methods=['GET'])
def get_shopmlconfig():
    try:
        cache_key = "get_shopmlconfig"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_shopmlconfig()
        cache.set(cache_key, json.dumps(output), ex=5)
        print("Response /emsadminapi/v1/get_shopmlconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_shopmlconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_shopmlconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_shopmlconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/get_shiftconfig', methods=['GET'])
def get_shifconfig():
    try:
        cache_key = "get_shiftconfig"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_shifconfig()
        cache.set(cache_key, json.dumps(output), ex=5)
        print("Response /emsadminapi/v1/get_shiftconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_shiftconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_shiftconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_shiftconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/get_kpiconfig', methods=['GET'])
def get_kpiconfig():
    try:
        cache_key = "get_kpiconfig"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_kpiconfig()
        cache.set(cache_key, json.dumps(output), ex=5)
        print("Response /emsadminapi/v1/get_kpiconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_kpiconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_kpiconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_kpiconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/get_whatsappconfig', methods=['GET'])
def get_whatsappconfig():
    try:
        cache_key = "get_whatsappconfig"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_whatsappconfig()
        cache.set(cache_key, json.dumps(output), ex=5)
        print("Response /emsadminapi/v1/get_whatsappconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_whatsappconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_whatsappconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_whatsappconfig: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/get_emailconfig', methods=['GET'])
def get_emailconfig():
    try:
        cache_key = "get_emailconfig"
        cached_data = cache.get(cache_key)
        print(cache_key)
        if cached_data:
            return jsonify(json.loads(cached_data)), 200
        output = dbService.get_emailconfig()
        cache.set(cache_key, json.dumps(output), ex=5)
        print("Response /emsadminapi/v1/get_emailconfig ")
        LOG.INFO("Response /emsadminapi/v1/get_emailconfig ")
        return jsonify(output), 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/get_emailconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/get_emailconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/post_assetconfig', methods=['POST'])
def post_assetconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = "post_assetconfig"
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
        # cache_key = f"post_assetattributes"
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
        # cache_key = f"post_shopattributes"
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
        # cache_key = "post_assetfaultruleconfig"
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

@app.route('/emsadminapi/v1/post_assetmlconfig', methods=['POST'])
def post_assetmlconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = "post_assetmlconfig"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_assetmlconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_assetmlconfig ")
        LOG.INFO("Response /emsadminapi/v1/post_assetmlconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_assetmlconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_assetmlconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/post_shiftconfig', methods=['POST'])
def post_shiftconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = "post_shiftconfig"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_shiftconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_shiftconfig ")
        LOG.INFO("Response /emsadminapi/v1/post_shiftconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_shiftconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_shiftconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/post_kpiconfig', methods=['POST'])
def post_kpiconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = "post_kpiconfig"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_kpiconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_kpiconfig ")
        LOG.INFO("Response /emsadminapi/v1/post_kpiconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_kpiconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_kpiconfig: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/post_whatsappconfig', methods=['POST'])
def post_whatsappconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = "post_whatsappconfig"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_whatsappconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_whatsappconfig ")
        LOG.INFO("Response /emsadminapi/v1/post_whatsappconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_whatsappconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_whatsappconfig: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/post_emailconfig', methods=['POST'])
def post_emailconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # cache_key = "post_emailconfig"
        # cached_data = cache.get(cache_key)
        # print(cache_key)
        # if cached_data:
        #     return jsonify(json.loads(cached_data)), 200
       
        output = dbService.post_emailconfig(data)
        # cache.set(cache_key, json.dumps(output), ex=900)
        print("Response /emsadminapi/v1/post_emailconfig ")
        LOG.INFO("Response /emsadminapi/v1/post_emailconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/post_emailconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/post_emailconfig: " + str(ex))
        return ex

@app.route('/emsadminapi/v1/put_assetconfig', methods=['PUT'])
def put_assetconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        # print(data)
        # dict_data2 = request.get_json()
        # temp = json.dumps(dict_data2)
        # data2 = json.loads(temp)
        output = dbService.put_assetconfig(data)
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
        
        output = dbService.put_assetattributes(data)
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
        
        output = dbService.put_shopattributes(data)
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
        
        output = dbService.put_assetfaultruleconfig(data)
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
        
        output = dbService.delete_assetconfig(data)
        print("Response /emsadminapi/v1/delete_assetconfig")
        LOG.INFO("Response /emsadminapi/v1/delete_assetconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/delete_assetconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/delete_assetconfig: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/delete_assetattributes', methods=['DELETE'])
def delete_assetattributes():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        print(data)
        # dict_data2 = request.get_json()
        # temp = json.dumps(dict_data2)
        # data2 = json.loads(temp)
        
        output = dbService.delete_assetattributes(data)
        print("Response /emsadminapi/v1/delete_assetattributes ")
        LOG.INFO("Response /emsadminapi/v1/delete_assetattributes ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/delete_assetattributes" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/delete_assetattributes: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/delete_shopattributes', methods=['DELETE'])
def delete_shopattributes():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        print(data)
        # dict_data2 = request.get_json()
        # temp = json.dumps(dict_data2)
        # data2 = json.loads(temp)
        
        output = dbService.delete_shopattributes(data)
        print("Response /emsadminapi/v1/delete_shopattributes")
        LOG.INFO("Response /emsadminapi/v1/delete_shopattributes ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/delete_shopattributes" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/delete_shopattributes: " + str(ex))
        return ex
    
@app.route('/emsadminapi/v1/delete_assetfaultruleconfig', methods=['DELETE'])
def delete_assetfaultruleconfig():
    try:
        dict_data = request.get_json()
        temp = json.dumps(dict_data)
        data = json.loads(temp)
        print(data)
        # dict_data2 = request.get_json()
        # temp = json.dumps(dict_data2)
        # data2 = json.loads(temp)
        
        output = dbService.delete_assetfaultruleconfig(data)
        print("Response /emsadminapi/v1/delete_assetfaultruleconfig")
        LOG.INFO("Response /emsadminapi/v1/delete_assetfaultruleconfig ")
        return output, 200
    except Exception as ex:
        print("Exception /emsadminapi/v1/delete_assetfaultruleconfig" + str(ex))
        LOG.ERROR("Exception /emsadminapi/v1/delete_assetfaultruleconfig: " + str(ex))
        return ex
        
if __name__ == '__main__':
    serve(app, host="localhost", port=3005)


