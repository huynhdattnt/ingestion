# -*- coding: utf-8 -*-

from datetime import datetime
import os, random, string, subprocess32
from ..extensions import cache, mdb
from flask import Blueprint, current_app, request, render_template, jsonify, url_for, abort
from tasks import encode_video_task
from mongokit import ObjectId, Document
from string import Template
import StringIO, ConfigParser, codecs
from ..config import DefaultConfig
from kombu import BrokerConnection
import requests, json


ingestion = Blueprint('ingestion', __name__, url_prefix='/ingestion', template_folder='templates')
@ingestion.route('/encode_video', methods=['GET', 'POST'])
def encode_video():
    '''
    encoding video when user clicked encode from inside site
    input: none
    returns status of encoding process
    '''
    if not request.json or not 'origin_id' in request.json or not 'user' in request.json:
        return abort(400)
    origin_id = request.json.get('origin_id')
    valid = ObjectId.is_valid(origin_id)
    if valid == False:
        return jsonify({'result':'False', 'info':'origin id is not valid, cannot encode video!', 'error': int(0)})
    sub_url = request.json.get('sub_url')
    logo_url = request.json.get('logo_url')
    user = request.json.get('user')
    profiles_id = request.json.get('profiles_id') #if profiles_id not null, follow on this
    priority = request.json.get('priority')
    '''
    for croping video
    '''
    crop = request.json.get('crop')
    crop_arr = []
    if crop:
        crop_arr = [int(c) for c in crop.split(',')]
        
    profile_array = []
    is_new_profiles = int(0)
    if profiles_id:
        profile_array =  [p.strip(' ') for p in profiles_id.split(',')]
        for profile_id in profile_array:
            valid = ObjectId.is_valid(profile_id)
            if valid == False:
                return jsonify({'result':'False', 'info':'profile id is not valid, cannot add origin video!', 'error': int(1)})
        is_new_profiles = int(1)
    
    origin_video = mdb.OriginVideo.get_origin_video(origin_id)
    
    if origin_video:
        if len(crop_arr) != 4:
            crop_arr = origin_video['crop']
        #prevent spam form bad user
        #if origin_video['status_encode'] == 1:
            #return jsonify({'result':'False', 'info': 'cannot encode video, because the same process encoding is working', 'error': int(2)})
        if priority:
            origin_video['priority'] = int (priority)
        
        if origin_video['priority']:
            conn = BrokerConnection(current_app.config['CELERY_BROKER_URL_PRI'], heartbeat=int(10))
        else:
            conn = BrokerConnection(current_app.config['CELERY_BROKER_URL'], heartbeat=int(10))

        update_origin_video(origin_id, 5)
        encode_video_task.apply_async([origin_video['video_url'], sub_url, logo_url, profile_array if profiles_id else origin_video['profiles_id'], origin_video['priority'], user, origin_id, is_new_profiles, crop_arr],  connection=conn)
        conn.release()
        
        if profiles_id:
            if not mdb.EncodingJob.get_jobs_with_origin_video_id(origin_id).count():
                origin_video['profiles_id'] = profile_array
            else:
                profiles_id_old = origin_video['profiles_id']
                for p in profile_array:
                    
                    if p not in profiles_id_old:
                        profiles_id_old.append(p)
                origin_video['profiles_id'] = profiles_id_old
        
        origin_video['status_encode'] = int(5) #video have been pushed to queue
        origin_video['sub_url'] = sub_url if sub_url else ''
        origin_video['logo_url'] = logo_url if logo_url else ''
        origin_video['user'] = user
        origin_video['priority'] = int(priority) if priority else int(0)
        origin_video['crop'] = crop_arr
        origin_video.save()
        return jsonify({'result':'Done', 'info': 'encoding video successfull!'})
    return jsonify({'result':'False', 'info': 'cannot encode video, origin id not found', 'error': int(4)})

@ingestion.route('/encoding_status', methods=['GET'])
def encoding_status():
    '''
    get status of job encoded.
    input:
        - job_id: a string object
    returns the status of job
    '''
    if not request.json or not 'job_id' in request.json:
        return abort(400)
    job_id = request.json.get('job_id')
    valid = ObjectId.is_valid(job_id)
    if valid == False:
        return jsonify({'result':'False', 'info':'job id is not valid, cannot get status of video!'})
    job = mdb.EncodingJob.get_job(job_id)
    if job:
        return jsonify({'result': 'Done', 'status': job['status'], 'progressive': job['progressive']})
    return jsonify({'result': 'False', 'info': 'job not found'})

@ingestion.route('/check_exist_file', methods=['POST'])
def check_exist_file():
    '''
    check file existed on system
    input:
        - file_path: a string object
    returns the result code (1: existed, 0: not found)
    '''
    if not request.json or not 'file_path' in request.json:
        return abort(400)
    file_path = request.json.get('file_path')
    if os.path.isfile(file_path):
        return jsonify({'result': 'True', 'code': int(1)})
    return jsonify({'result': 'False', 'code': int(0)})

@ingestion.route('/move_video_to_production', methods=['POST'])
def move_video_to_production():
    '''
    when video have been encoded. call this function to move all files (origin video, out videos) to stores
    input:
        -src: a string object, the source path of video
        -des: a string object, the destiny path of video
    returns the status of move video
    '''
    if not request.json or not 'src' in request.json or not 'des' in request.json:
        return abort(400)
    src = request.json.get('src')
    des = request.json.get('des')
    if os.path.isfile(src):
        mv_command = 'mv ' + src + ' ' + des
        os.system(mv_command)
        return jsonify({'result':'Done', 'info': 'moving video successfull!'})
    return jsonify({'result':'False', 'info': 'cannot moved video to production!'})

def update_origin_video(origin_id, status_encode):
    '''
    change status of video from encoded status to queueing status, then call to iapi
    input:
        - origin_id: a string object
        - status_encode: a integer object
    returns the result code (1: successfull, 0: failed)
    '''
    try:
        iapi =   current_app.config['IAPI_SERVER'] + 'ingestion/update_video_encode'
        headers = {'content-type': 'application/json'}
        payload = {
            "origin_id" : str(origin_id),
            "status"    : int(status_encode)
        }
        request = requests.post(iapi, data=json.dumps(payload), headers=headers)
        if request.status_code == 200:
            return 1
        return 0
    except Exception as error:
        return 0
