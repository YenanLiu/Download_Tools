import json
import os
import pandas as pd
import glob 

def stat_vggss(json_path):
    with open(json_path, 'r') as file:
        data = json.load(file)
        print("ok") # 5158


def split_csv(file_path, output_dir, chunk_size=500):
    df = pd.read_csv(file_path, header=None)
    total_rows = len(df)
    
    num_files = (total_rows // chunk_size) + (1 if total_rows % chunk_size != 0 else 0)

    os.makedirs(output_dir, exist_ok=True)
    
    for i in range(num_files):
        start_row = i * chunk_size
        end_row = start_row + chunk_size
        if end_row < total_rows:
            chunk_df = df.iloc[start_row:end_row]
        else:
            chunk_df = df.iloc[start_row:total_rows+1]
        
        file_suffix = f"{i:03d}"
        output_file = os.path.join(output_dir, f"chunk_{file_suffix}.csv")
        chunk_df.to_csv(output_file, index=False, header=False)

def unfiying_video_names(save_dir="E:/DATA/VGGSound/*/*/*.mp4"): # E:/DATA/VGGSound/*/*/*.mp4 train/categories/vname
    video_list = glob.glob(save_dir)
    for v_path in video_list:
        v_mpath, v_ori_name = v_path.rsplit("/", 1)
        v_ori_name = v_ori_name.replace("v", "").replace("_out", "") # {video_id}_{start_time}_{end_time}
        start_time = v_ori_name.split('_')[-2]
        video_id = v_ori_name.rsplit('_', 2)[0]

        start_time_str = str(start_time).zfill(6)
        new_v_name = video_id + "_" + start_time_str + ".mp4"
        new_v_save_path = os.path.join(v_mpath, new_v_name)

        os.rename(v_path, new_v_save_path)

if __name__ == "__main__":
    # json_path = "/root/AVLCode/DataProcess/VGGSound/metadata/vggss.json"
    # stat_vggss(json_path)

    # file_path = "/root/AVLCode/DataProcess/VGGSound/vggsound.csv"
    # output_dir = "/root/AVLCode/DataProcess/VGGSound/split_downfiles"
    # chunk_size=500
    # split_csv(file_path, output_dir, chunk_size)

    # save_dir="E:/DATA/VGGSound/code_test/*/*.mp4"
    # print(glob.glob(save_dir))

    unfiying_video_names()