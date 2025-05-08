# ---------------------------------------------------------------
# Copyright (c) 2025. All rights reserved.
#
# Authored by Chen Liu (yenanliu36@gmail.com) on July 5, 2025.
# ---------------------------------------------------------------

num_threads = 4

import os
import csv
import json
import random
import logging
import yt_dlp
import subprocess
import pandas as pd
from tqdm import tqdm
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import contextlib
from io import StringIO

############  Logging  ########################
def setup_logger(log_file):
    logger = logging.getLogger('download_logger')
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

############  About Reading Data  #############
def reading_vid_from_json(json_file, main_save_dir):
    """
    Reading vid info from provided json files.
    Args:
        json_file: the json data saving path.
        main_save_dir: main saving directory.
    Return: 
        dict, key is youtube video id, value is the video saving directory.
    """
    with open(json_file, 'r') as jfile:
        data = json.load(jfile)
    
    v_path_dict = {}
    vinfo_dicts = data["videos"]
    for cate_name, cate_v_list in vinfo_dicts.items():
        sub_save_dir = os.path.join(main_save_dir, cate_name)
        os.makedirs(sub_save_dir, exist_ok=True)
        for v_name in cate_v_list:
            v_path_dict[v_name] = sub_save_dir
    
    return v_path_dict

# def read_csv(csv_path):
#     df = pd.read_csv(csv_path)
#     vid_list = df["video_id"].tolist()[1:]
#     return list(set(vid_list))  # return existing video ids

def read_csv(csv_path):
    vid_list = []
    if not os.path.exists(csv_path):
        return vid_list
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            vid_list.append(row['video_id'])
    return list(set(vid_list))

def add_data_csv(csv_path, data, head=['video_id', 'start_time', 'end_time', 'category']):
    """
    Note: If you need to record million level data, this recording function is not an optimal choice, please using other efficient way.
    data_format: 
        [{'video_id': 'v004', 'start_time': 30, 'end_time': 40, 'category': 'bird'},
        {'video_id': 'v005', 'start_time': 30, 'end_time': 40, 'category': 'bird'},
        ...]
    """
    if isinstance(data, pd.DataFrame):
        df_new = data
    else:
        df_new = pd.DataFrame(data)

    df_new = df_new[head]

    if not os.path.exists(csv_path):
        df_new.to_csv(csv_path, index=False, header=head)
    else:
        df_new.to_csv(csv_path, mode='a', index=False, header=False)
        print(f"added {len(data)} video items successfully!")
        
############  About Downloading Videos Data  #############
def ffmpeg_extract_segment(input_file, output_file, start_time, end_time):
    cmd = ['ffmpeg', '-loglevel', 'quiet', '-i', input_file, '-ss', str(start_time), '-to', str(end_time), output_file]
    subprocess.run(cmd)

def convert_to_mp4(input_file, output_file):
    cmd = ['ffmpeg', '-loglevel', 'quiet', '-i', input_file, output_file]
    subprocess.run(cmd)

def download_one_video(vid, save_dir, csv_path, trimmed_length=6):
    video_url = "https://www.youtube.com/watch?v={}".format(vid)
    options = {
        'format': 'bestvideo[height<=480]+bestaudio/best[height<=480]',  # 限制最大分辨率为 480p
        'merge_output_format': 'mp4',  
        'outtmpl': f'{save_dir}/%(id)s.%(ext)s',  
        'quiet': True,   
        'noplaylist': True,  
        'nocheckcertificate': True,  
    }
    stderr_redirect = StringIO()
    try:
        with contextlib.redirect_stderr(stderr_redirect):
            ydl = yt_dlp.YoutubeDL(options)
            # Download the video
            with ydl:
                result = ydl.extract_info(video_url, download=True)
                downloaded_file = ydl.prepare_filename(result)
                video_duration = result.get('duration', None)
    except Exception as e:
        return stderr_redirect.getvalue(), vid  # Return the error message
    
    inputfile = downloaded_file
    ori_video_save_dir = os.path.join(save_dir, "original_video")
    segment_save_dir = os.path.join(save_dir, "trimmed_videos")
    os.makedirs(ori_video_save_dir, exist_ok=True)
    os.makedirs(segment_save_dir, exist_ok=True)

    # random generate trimmed start time position
    latest_start_time = round(video_duration - trimmed_length, 2)
    start_time = round(random.uniform(0.0, latest_start_time), 2)
    end_time = start_time + trimmed_length
    category = save_dir.split("/")[-1]
    trimmed_v_info = {'video_id': vid, 'start_time': start_time, 'end_time': end_time, 'category': category}

    v_seg_name = f"{vid}__s__{start_time}__e__{end_time}__cate__{end_time}.mp4"
    ori_video_save_path = os.path.join(ori_video_save_dir, vid + ".mp4")
    segment_save_path = os.path.join(segment_save_dir, v_seg_name)    

    convert_to_mp4(inputfile, ori_video_save_path)
    ffmpeg_extract_segment(inputfile, segment_save_path, start_time, end_time)

    add_data_csv(csv_path, [trimmed_v_info])
    os.remove(inputfile)

def downloadMusic(json_file, main_save_dir, logger):
    os.makedirs(main_save_dir, exist_ok=True)

    # attaining the data required to be downloaded
    video_info_dict = reading_vid_from_json(json_file, main_save_dir)
    csv_path = json_file.replace(".json", ".csv")
 
    has_down_vlist = read_csv(csv_path)
    ori_video_list = list(video_info_dict.keys())
    required_down_vlist = list(set(ori_video_list) - set(has_down_vlist)) # required to be downloaded

    # trimmed recording info will save into a csv file
    logger.info(f"{len(has_down_vlist)} videos have been downloaded, {len(required_down_vlist)} will be downloaded")
    origin_sysout = sys.stdout
    with tqdm(total=len(required_down_vlist)) as pbar:
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                        executor.submit(download_one_video, vid, save_path, csv_path)
                        for vid, save_path in video_info_dict.items()
                        if vid in required_down_vlist
            ]
            for future in concurrent.futures.as_completed(futures):
                error_message, video_id = future.result()
                if error_message:
                    with contextlib.redirect_stdout(origin_sysout):
                        logger.info(error_message)
                        if 'ERROR' in error_message:
                            video_id_str = "[youtube] {}: ".format(video_id)
                            Error_reason = error_message[error_message.find(video_id_str) + len(video_id_str):].strip()
                            if Error_reason.find("\n") != -1:
                                Error_reason = Error_reason[:Error_reason.find("\n")]
                            if not any(error in Error_reason for error in ['Too Many Requests', 'Internal Server Error', 'Read timed out']):
                                logger.info("{},\"{}\"\n".format(video_id, Error_reason.replace('\"', '')))
                pbar.update(1)
                        
if __name__ == "__main__":
    log_dir = "E://DATA//VGGSoundCode//Music21//down_data//download_logs"
    os.makedirs(log_dir, exist_ok=True)
    main_save_dir = "E://DATA//VGGSoundCode//Music21//down_data"
    json_paths = ["E://DATA//VGGSoundCode//Music21//vid_files//MUSIC_solo_videos.json", "E://DATA//VGGSoundCode//Music21//vid_files//MUSIC21_solo_videos.json"]

    for j_path in json_paths:
        logger_name = os.path.basename(j_path).split(".")[0] + ".txt"

        logger_path = os.path.join(log_dir, logger_name)
        logger = setup_logger(logger_path)

        downloadMusic(j_path, main_save_dir, logger)
