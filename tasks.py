# -*- coding: utf-8 -*-

from ..tasks import celery
from celery import current_app
from ..extensions import cache, mdb
import requests
from mongokit import ObjectId, Document
from datetime import datetime
import requests, json
import subprocess as sp
import traceback
import subprocess32, os
from string import Template
import StringIO, ConfigParser, codecs
from ..config import DefaultConfig
from slugify import slugify
import hashlib
import socket

import math, lxml.etree

ffmpeg_base_args = '$ffmpegcmd -i "$inputfile" -g 120 -c:v:0 libx264 -vprofile $vprofile -level $profilelevel -preset fast -filter_complex scale="${vwidh}:trunc(ow/a/2)*2" -b:v:0 $vbitrate -maxrate $vmaxrate  -bufsize $vmaxrate -refs 5 -bf 3 -trellis 2 -b_strategy 2 -me_range 24 -subq 9 -sc_threshold 0 -map_metadata -1 -x264opts deblock=0/0:sar=1/1:direct=auto:no-dct-decimate:rc-lookahead=60:no-fast-pskip:b-pyramid=normal:psy-rd=1.0:aq-strength=0.5:aq-mode=2:qpmin=3:qcomp=0.6 -c:a:0 aac -ac 2 -b:a:0 $abitrate -af volume=2 -movflags +faststart -c:a:1 ac3 -ac 6 -b:a:1 $abitrate -strict -2 -metadata title=$videoname -f mp4 -y "$outputfile" 2> "$logconsolefile"'

ffmpeg_base_with_logo_args = '$ffmpegcmd -i "$inputfile" -i $logo -g 120 -c:v:0 libx264 -vprofile $vprofile -level $profilelevel -preset fast -filter_complex scale="${vwidh}:trunc(ow/a/2)*2",overlay=main_w-overlay_w-10:10 -b:v:0 $vbitrate -maxrate $vmaxrate  -bufsize $vmaxrate -refs 5 -bf 3 -trellis 2 -b_strategy 2 -me_range 24 -subq 9 -sc_threshold 0 -map_metadata -1 -x264opts deblock=0/0:sar=1/1:direct=auto:no-dct-decimate:rc-lookahead=60:no-fast-pskip:b-pyramid=normal:psy-rd=1.0:aq-strength=0.5:aq-mode=2:qpmin=3:qcomp=0.6 -c:a:0 aac -ac 2 -b:a:0 $abitrate -af volume=2 -movflags +faststart -c:a:1 ac3 -ac 6 -b:a:1 $abitrate -strict -2 -metadata title=$videoname -f mp4 -y "$outputfile" 2> "$logconsolefile"'

ffmpeg_base_with_sub_args = '$ffmpegcmd -i "$inputfile" -g 120 -c:v:0 libx264 -vprofile $vprofile -level $profilelevel -preset fast -filter_complex scale="${vwidh}:trunc(ow/a/2)*2",subtitles="$subtitlefile" -b:v:0 $vbitrate -maxrate $vmaxrate -bufsize $vmaxrate -refs 5 -bf 3 -trellis 2 -b_strategy 2 -me_range 24 -subq 9 -sc_threshold 0 -map_metadata -1 -x264opts deblock=0/0:sar=1/1:direct=auto:no-dct-decimate:rc-lookahead=60:no-fast-pskip:b-pyramid=normal:psy-rd=1.0:aq-strength=0.5:aq-mode=2:qpmin=3:qcomp=0.6 -c:a:0 aac -ac 2 -b:a:0 $abitrate -af volume=2 -movflags +faststart -c:a:1 ac3 -ac 6 -b:a:1 $abitrate -strict -2 -metadata title=$videoname -f mp4 -y "$outputfile" 2> "$logconsolefile"'

ffmpeg_base_with_sub_and_logo_args = '$ffmpegcmd -i "$inputfile" -i $logo -g 120 -c:v:0 libx264 -vprofile $vprofile -level $profilelevel -preset fast -filter_complex scale="${vwidh}:trunc(ow/a/2)*2",overlay=main_w-overlay_w-10:10,subtitles="$subtitlefile" -b:v:0 $vbitrate -maxrate $vmaxrate -bufsize $vmaxrate -refs 5 -bf 3 -trellis 2 -b_strategy 2 -me_range 24 -subq 9 -sc_threshold 0 -map_metadata -1 -x264opts deblock=0/0:sar=1/1:direct=auto:no-dct-decimate:rc-lookahead=60:no-fast-pskip:b-pyramid=normal:psy-rd=1.0:aq-strength=0.5:aq-mode=2:qpmin=3:qcomp=0.6 -c:a:0 aac -ac 2 -b:a:0 $abitrate -af volume=2 -movflags +faststart -c:a:1 ac3 -ac 6 -b:a:1 $abitrate -strict -2 -metadata title=$videoname -f mp4 -y "$outputfile" 2> "$logconsolefile"'

ffmpegcmd = current_app.conf.get('FFMPEG_CMD')
ffprobecmd = current_app.conf.get('FFPROBE_CMD')

#@cache.memoize(1000)
def get_profile_data(profiles_id):
    all_profile_data = mdb.Profile.get_all_profiles()
    return [profile_data for profile_data in all_profile_data if str(profile_data['_id']) in profiles_id]


#@cache.memoize(600)
def ffmpeg_task(video_url, sub_url, logo_url, profile_data, jobs_id, user, origin_video_id, is_new_profiles, des_path, product_path, product_url, crop):
    '''
    encoding function, then update status and progressive in job
    input:
        - video_url     : a string object
        - sub_url       : a string object
        - logo_url      : a string object
        - profile_data  : a string object
        - jobs_id       : a string object
        - user          : a string object
        - origin_video_id: a string object
        - is_new_profiles: a integer object
        - des_path      : a string object
        - product_path  : a string object
        - product_url   : a string object
        - crop          : a array object
    returns: status of encode process (1: successful, 0: failed)
    '''
    input = video_url
    if not os.path.isfile(input):
        return 0
    video_name_without_ext = video_url.rsplit('.', 1)[0].split('/')[-1]
    logconsolefile = des_path + "log"
    
    '''
    Create new smill file
    '''
    smil    = lxml.etree.Element('smil', attrib = { 'title' : '' })
    body    = lxml.etree.SubElement(smil, 'body')
    switch  = lxml.etree.SubElement(body, 'switch')
    
    file_out = product_path + hash_video_name(video_name_without_ext) + '.smil'
    url_of_file_out = product_url + hash_video_name(video_name_without_ext) + '.smil'
    width_old = []
    if is_new_profiles:
        if os.path.isfile(file_out):
            xml_root = lxml.etree.parse(file_out).getroot()
            switchs_old = xml_root[0][0]
            for s_old in switchs_old:
                lxml.etree.SubElement(switch, 'video', attrib = s_old.attrib)
                width_old.append(int(s_old.attrib['width']))
   
    profile_id = [str(profile['_id']) for profile in profile_data]
    
    try:
        for p in profile_data:
        '''
        encoding video depends the profiles that video have
        '''
            output = des_path + slugify(hash_video_name(video_name_without_ext) + '_' + p['standard']) + '.mp4'
            
            #crop_out = reference_ratio(crop, p, origin_video_id, profile_data[-1])
            
            if (sub_url == '') & (logo_url == ''):
                encode          = Template(ffmpeg_base_args).substitute(ffmpegcmd = ffmpegcmd, inputfile = input, vprofile = p['data']['vprofile'], profilelevel = p['data']['profilelevel'], vwidh = p['data']['width'] , vbitrate = p['data']['vbitrate'], vmaxrate = p['data']['vmaxrate'], abitrate = p['data']['abitrate'], outputfile = output, logconsolefile = logconsolefile, videoname = video_name_without_ext + '.mp4')
                encode_result   = (subprocess32.check_output(encode, shell = True)).strip()
            if (sub_url != '') & (logo_url == ''):
                encode          = Template(ffmpeg_base_with_sub_args).substitute(ffmpegcmd = ffmpegcmd, inputfile = input, vprofile = p['data']['vprofile'], profilelevel = p['data']['profilelevel'], vwidh = p['data']['width'] , vbitrate = p['data']['vbitrate'], vmaxrate = p['data']['vmaxrate'], abitrate = p['data']['abitrate'], outputfile = output, logconsolefile = logconsolefile, subtitlefile = sub_url, videoname = video_name_without_ext + '.mp4')
                encode_result = (subprocess32.check_output(encode, shell = True)).strip()
            if (sub_url == '') & (logo_url != ''):
                encode          = Template(ffmpeg_base_with_logo_args).substitute(ffmpegcmd = ffmpegcmd, inputfile = input, vprofile = p['data']['vprofile'], profilelevel = p['data']['profilelevel'], vwidh = p['data']['width'] , vbitrate = p['data']['vbitrate'], vmaxrate = p['data']['vmaxrate'], abitrate = p['data']['abitrate'], outputfile = output, logconsolefile = logconsolefile, logo = logo_url, videoname = video_name_without_ext + '.mp4')
                encode_result = (subprocess32.check_output(encode, shell = True)).strip()
            if (sub_url != '') & (logo_url != ''):
                encode          = Template(ffmpeg_base_with_sub_and_logo_args).substitute(ffmpegcmd = ffmpegcmd, inputfile = input, vprofile = p['data']['vprofile'], profilelevel = p['data']['profilelevel'], vwidh = p['data']['width'] , vbitrate = p['data']['vbitrate'], vmaxrate = p['data']['vmaxrate'], abitrate = p['data']['abitrate'], outputfile = output, logconsolefile = logconsolefile, subtitlefile = sub_url, logo = logo_url, videoname = video_name_without_ext + '.mp4')
                encode_result = (subprocess32.check_output(encode, shell = True)).strip()
            
            #move_video_to_production(output, product_path)
            if os.path.isfile(output):
                mv_command = 'mv ' + output + ' ' + product_path
                os.system(mv_command)
            
            mdb.EncodingJob.update_job(jobs_id[profile_data.index(p)], int(1), int(100))
            
            system_bitrate = int(p['data']['vbitrate'].replace('k','000', 1)) + int(p['data']['abitrate'].replace('k','000', 1))
            attributes_dict = {
                'src'               : slugify(hash_video_name(video_name_without_ext) + '_' + p['standard']) + '.mp4',
                'width'             : '%s' % (p['data']['width']),
                'height'            : '%s' % (p['data']['height']),
                'system-bitrate'    : "%s" % (system_bitrate),
            }
            
            if p['data']['width'] not in width_old:
                lxml.etree.SubElement(switch, 'video', attrib = attributes_dict)
        xml_text = lxml.etree.tostring(smil, encoding="utf-8", method="xml", pretty_print = True)
        if generate_smil_xml_file(file_out, xml_text, profile_id, user, origin_video_id, url_of_file_out):
            return 1
        return 0
    except Exception as error:
        traceback.print_exc()
        return 0

def reference_ratio(crop, profile, origin_video_id, profile_origin):
    '''
    using when user want to crop video
    input:
        - crop      : a array object
        - profile   : a string object
        - origin_video_id   : a string object
        - profile_origin    : a string object
    returns a array about rest size of video if successful else 0
    '''
    origin_video = mdb.OriginVideo.get_origin_video(origin_video_id)
    if origin_video:
        crop_out = []
        width_out = int(profile['data']['width'] * crop[0] / int(profile_origin['data']['width']))
        height_out = int(profile['data']['height'] * crop[1] / int(profile_origin['data']['height']))
        if (width_out > int(profile['data']['width'])) | (height_out > int(profile['data']['height'])):
            width_out = int(profile['data']['width'])
            height_out = int(profile['data']['height'])
            
        crop_out.append(width_out)
        crop_out.append(height_out)
        
        crop_out.append(crop[2])
        crop_out.append(crop[3])
        return crop_out
    return 0
    
def move_video_to_production(src, des):
    '''
    call to ingestion controller to move origin video to store
    input:
        - src: a string object, path of video
        - des: a string object, path on production
    returns 1 (successful) or 0 (failed)
    '''
    try:
        ingestion = current_app.conf.get('API_MOVE_VIDEO')
        headers = {'content-type': 'application/json'}
        payload = {
            "src" : str(src),
            "des" : str(des)
        }
        request = requests.post(ingestion, data=json.dumps(payload), headers=headers)
        if request.status_code == 200:
            return 1
        return 0
    except Exception as error:
        return 0
      
def generate_smil_xml_file(file_out, xml_text, profile_id, user, origin_video_id, url_of_file_out):
    '''
    gennerate smil file and insert job to db
    input:
        - file_out: a string object, absolute path of smill file
        - xml_text: a string object, the content of smill file
        - profile_id: a string object
        - user: a string object
        - origin_video_id: a string object
        - url_of_file_out: a string object, reference to file_out parameter
    returns 1 (successful) or 2 (failed)
    '''
    file_pointer = codecs.open(file_out, "w", "utf-8")
    file_pointer.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    file_pointer.write(xml_text)
    file_pointer.close()
    hls = url_of_file_out + '/playlist.m3u8'
    index = mdb.Profile.get_hightest_of_index() + 1
    smil_obj = mdb.EncodingJob.get_smill_job(origin_video_id, index)
    try:
        if smil_obj:
            for p in profile_id:
                if p not in smil_obj['profile_id']:
                    smil_obj['profile_id'].append(p)
            smil_obj.save()
            return 1
        return mdb.EncodingJob.insert_job(url_of_file_out, profile_id, int(1), int(100), datetime.now(), user, origin_video_id, hls, '', '', 'auto_vip', int(index))
    except Exception as error:
        traceback.print_exc()
        return 0
        
def create_job(video_url, profile, user, origin_video_id, sub, logo, index, des_path, product_url):
    '''
    created job before start encoding video
    input:
        - video_url: a string object, absolute path of video
        - profile: a string object
        - user: a string object
        - origin_video_id: a string object
        - sub: a string object
        - logo: a string object
        - index: a integer object
        - des_path: a string object
        - product_url: a string object
    returns none
    '''
    video_name_without_ext = video_url.rsplit('.', 1)[0].split('/')[-1]

    #file_out = des_path + slugify(video_name_without_ext + '_' + profile['standard']) + '.mp4'
    url_of_file_out = product_url + slugify(hash_video_name(video_name_without_ext) + '_' + profile['standard']) + '.mp4'
    hls = url_of_file_out + '/playlist.m3u8'
    status = int(0)
    progressive = int (0)
    date = datetime.now()
    profile_id = str(profile['_id'])
    return mdb.EncodingJob.insert_job(url_of_file_out, profile_id, status, progressive, date, user, origin_video_id, hls, sub, logo, profile['standard'], index)

@celery.task(name = 'ingestion.encode_video_task', rate_limit='5/s')
def encode_video_task(video_url, sub, logo, profiles_id, priority, user, origin_video_id, is_new_profiles, crop):
    '''
    task encoding video when user called api encode_video
    input:
        - video_url: a string object
        - sub: a string object
        - logo: a string object
        - profiles_id: a string object
        - priority: a integer object
        - user: a string object
        - origin_video_id: a string object
        - is_new_profiles: a string object
        - crop: a array object included the size of video
    returns done or failed
    '''
    mdb.OriginVideo.update_server_encode(origin_video_id, str(socket.gethostname()))
    profile_data = get_profile_data(profiles_id)
    
    if not profile_data:
        update_origin_video(origin_video_id, 2) #encode error
        return 'False, profiles data not found'
        
    #get environment variable
    sub_url = ""
    logo_url = ""
    if sub:
        sub_url        = sub
    if logo:
        logo_url       = logo
    jobs_id = []
    
    #delete the same jobs have exist in db
    mdb.EncodingJob.delete_same_jobs(origin_video_id, [p['index'] for p in profile_data])
    
    des_path = current_app.conf.get('PATH_OF_OUTPUT_VIDEO') + datetime.now().strftime('%Y%m%d') + '/'
    product_path = current_app.conf.get('PATH_OF_PRODUCT') + datetime.now().strftime('%Y%m%d') + '/'
    product_url = current_app.conf.get('URL_OF_PRODUCT') + datetime.now().strftime('%Y%m%d') + '/'
    mkdir_des_command = 'mkdir ' + des_path + ' -p'
    mkdir_product_command = 'mkdir ' + product_path + ' -p'
    
    if not os.path.exists(des_path):
        os.system(mkdir_des_command)
    if not os.path.exists(product_path):
        os.system(mkdir_product_command)
    for profile in profile_data:
        jobs_id.append(create_job(video_url, profile, user, origin_video_id, sub_url, logo_url, profile['index'], des_path, product_url))
    #start encoding
    update_origin_video(origin_video_id, 1)
    result = ffmpeg_task(video_url, sub_url, logo_url, profile_data, jobs_id, user, origin_video_id, is_new_profiles, des_path, product_path, product_url, crop)
    origin_video = mdb.OriginVideo.get_origin_video(origin_video_id)

    if result:
        update_origin_video(origin_video_id, 3)
        return 'Done'

    else:
        for i in range(0,2):
            if ffmpeg_task(video_url, sub_url, logo_url, profile_data, jobs_id, user, origin_video_id, is_new_profiles, des_path, product_path, product_url, crop):
                update_origin_video(origin_video_id, 3)
                return 'Done'
        update_origin_video(origin_video_id, 2) #encode error
        return 'False'

def update_origin_video(origin_id, status_encode):
    '''
    update video status when encoded done
    input:
        - origin_id: a string object
        - status_encode: a integer object
    returns 1 (successful), 0 (failed)
    '''
    origin_video = mdb.OriginVideo.get_origin_video(origin_id)
    if origin_video:
        if int(status_encode) == 3:
            src_path = current_app.conf.get('PATH_OF_SRC') + datetime.now().strftime('%Y%m%d')
            if not os.path.exists(src_path):
                mkdir_src_command = 'mkdir ' + src_path + ' -p'
                os.system(mkdir_src_command)
            move_video_to_production(origin_video['video_url'], src_path)
            origin_video['video_url'] = current_app.conf.get('PATH_OF_SRC') + datetime.now().strftime('%Y%m%d') + '/' + origin_video['video_url'].rsplit('/', 1)[1]
        mdb.OriginVideo.update_video_url_and_status(origin_id, origin_video['video_url'], status_encode)
    else:
        return 0
    try:
        iapi =   current_app.conf.get('IAPI_SERVER') + 'ingestion/update_video_encode'
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
        
def hash_video_name(video_name):
    '''
    hashing the video name for user cannot guess the next video name
    input:
        - video_name: the path of video
    returns the hash string
    '''
    video_hash = hashlib.md5()
    video_hash.update(video_name)
    return video_hash.hexdigest()
