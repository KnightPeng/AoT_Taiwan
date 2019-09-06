import datetime
import glob
import itertools
import os
import shutil
import tarfile
from io import StringIO
from HTMLWriter import HTMLWriter
import pandas as pd
import requests
import urllib3

import MappingTable


def console_format(message):
    print("[{0}]: {1}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message))


def main():
    extract_prefix = "AoT_Taiwan.complete"
    extract_tarpath = "{0}.{1}".format(extract_prefix, datetime.datetime.now().strftime("%Y-%m-%d"))
    clear_data(extract_tarpath)
    writer = HTMLWriter()
    urllib3.disable_warnings()
    temp_filename = "temp.tar"
    target_url = "https://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/AoT_Taiwan.complete.recent.tar"

    if not os.path.isdir(extract_tarpath):
        download_record(target_url, temp_filename)

    with tarfile.open(temp_filename) as tf:
        tf.extract(extract_tarpath + "/data.csv.gz")
        tf.extract(extract_tarpath + "/nodes.csv")
    console_format("Extract Done!")
    console_format("Processing...")

    node_list = ['0CC', '110', '0FD']
    # node_list = ['0FD']
    df_nodes = pd.read_csv(extract_tarpath + "/nodes.csv")
    df_nodes = df_nodes[df_nodes['vsn'].isin(node_list)]
    df_nodes = df_nodes[['node_id', 'vsn']]

    sensor_param = {'co': ["concentration"],
                    'h2s': ["concentration"],
                    'no2': ["concentration"],
                    'o3': ["concentration"],
                    'so2': ["concentration"],
                    'pms7003': ["1um_particle", "2_5um_particle", "10um_particle"],
                    'bmp180': ["pressure", "temperature"],
                    'hih4030': ["humidity"],
                    'hih6130': ["humidity", "temperature"],
                    'htu21d': ["humidity", "temperature"],
                    'lps25h': ["temperature"],
                    'pr103j2': ["temperature"],
                    'tsys01': ["temperature"],
                    'tmp421': ["temperature"],
                    'tmp112': ["temperature"],
                    'mma8452q': ["acceleration_x", "acceleration_y", "acceleration_z"],
                    'bmi160': ["acceleration_x", "acceleration_y", "acceleration_z",
                               "orientation_x", "orientation_y", "orientation_z"],
                    'hmc5883l': ["magnetic_field_x", "magnetic_field_y", "magnetic_field_z"],
                    'tsl260rd': ["intensity"],
                    'ml8511': ["intensity"],
                    'mlx75305': ["intensity"],
                    'si1145': ["intensity", "ir_intensity", "uv_intensity", "visible_light_intensity"],
                    'tsl250rd': ["intensity"],
                    'apds_9006_020': ["intensity"],
                    'spv1840lr5h_b': ["intensity"],
                    'image_detector': ["car_total", "person_total"]
                    }

    flatten_param = list(dict.fromkeys(itertools.chain(*sensor_param.values())))
    df_data = pd.read_csv(extract_tarpath + "/data.csv.gz", compression='gzip')
    df_data = df_data[df_data['sensor'].isin(list(sensor_param.keys()))]
    # df_data = df_data[df_data['parameter'].isin(flatten_param)].reset_index()
    # df_data = df_data.drop(columns='index')
    drop_rows = list()
    for index, row in df_data.iterrows():
        if not row['parameter'] in sensor_param[row['sensor']]:
            drop_rows.append(index)
    df_data = df_data.drop(index=drop_rows)

    df_data = pd.merge(df_nodes, df_data, on='node_id')
    df_data['timestamp'] = pd.to_datetime(df_data['timestamp'])
    df_data = df_data.groupby(['node_id', 'vsn', 'subsystem', 'sensor', 'parameter']).last().reset_index()
    df_data['value_raw'] = df_data['value_raw'].astype('float64')
    df_data['value_hrf'] = df_data['value_hrf'].astype('float64')

    cols = df_data.columns.tolist()
    cols.insert(0, cols.pop(cols.index('vsn')))
    df_data = df_data.reindex(columns=cols)

    output_filename = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    df_data.to_excel(output_filename + ".xlsx")
    writer.write(df_data, output_filename + ".html")
    # clear_temp()
    console_format("Done!")


def download_record(url, filename):
    console_format("Downloading...")
    # request.urlretrieve(url, filename)
    response = requests.get(url, verify=False, stream=True)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.raw.read())
    console_format("Download Done!")


def clear_data(extrace_path=""):
    file_list = glob.glob("*.tar")
    file_list.extend(glob.glob("Waggle*"))
    file_list.extend(glob.glob("*.xlsx"))
    file_list.extend(glob.glob("*.html"))
    if os.path.isdir(extrace_path):
        file_list.append(extrace_path)
    for filename in file_list:
        try:
            shutil.rmtree(filename)
        except NotADirectoryError:
            os.remove(filename)
        # except FileNotFoundError:
        #     continue


def clear_temp():
    file_list = glob.glob("*.tar")
    file_list.extend(glob.glob("Waggle*"))
    for filename in file_list:
        try:
            shutil.rmtree(filename)
        except NotADirectoryError:
            os.remove(filename)


if __name__ == "__main__":
    main()
