# Yaml Test
# Description : Code is used for automation of export / import process of Bigeye's Table level metrics
# Created by : Abhishek Dixit
# Date Created : 15 Apr 2024 Date Modifie : 20 Mar 2026
# Tech : Python, Bieye SDK, CSV files
# Create list of table id's in CSV for which export / import is inteded.
# Create bigeye cred file in json format and store it in Scripts folders.

import os
import yaml
import bigeye_sdk.authentication.api_authentication as bigeye_auth
import bigeye_sdk.client.datawatch_client as bigeye_client
import shutil
import subprocess
import time
import csv

source_table_list = []
table_list_file_path = "C:\\Users\\username\\bigeye_cli\\Scripts\\source_table_list.csv"
export_path = "C:\\Users\\username\\bigeye_cli\\Scripts\\bigeye_python_script_export"
directory_path = "C:\\Users\\username\\bigeye_cli\\Scripts"
filename = 'raw_mds_dim_business_function_copy.bigconfig.yml'


def get_table_list(table_list_file_path):
    with open(table_list_file_path, mode='r') as file:
        csvFile = csv.reader(file)
        header = next(csvFile)
        print("Table List Below")
        for table in csvFile:
            print(str(table[0]))
            source_table_list.append(str(table[0]))


def get_yml_file_export():
    for table in source_table_list:
        path = shutil.which("bigeye", path="C:\\Users\\username\\bigeye_cli\\Scripts")

        # cmd = [path, "bigconfig", "--help"]
        cmd = [path, "bigconfig", "export", "--table_id", table]
        p = subprocess.Popen(cmd)
        output, error = p.communicate()
        print(output)
        print(error)
        time.sleep(15)
        p.terminate()
        p.wait()


def create_yml_upsert_file(directory_path):
    # for filename in os.listdir(directory_path):
    #     file_path = os.path.join(directory_path,filename)
    #     if os.path.isfile(file_path):
    file_path = "C:\\Users\\username\\bigeye_cli\\Scripts\\raw_mds_dim_business_function_copy.bigconfig.yml"
    with open(file_path, 'r') as file:  # file_path, 'r', encoding='utf8'

        file_contents = yaml.safe_load(file)  # file.read()
        # print(file_contents)
        keyword = "Finance 360 (Dev)"
        replacement = "Finance 360 (QA)"

        # print(file_contents)

        def search_and_replace(file_contents, keyword, replacement):
            if isinstance(file_contents, dict):
                for key, value in file_contents.items():
                    if isinstance(value, (dict, list)):
                        search_and_replace(value, keyword, replacement)
                    elif isinstance(value, str):
                        file_contents[key] = value.replace(keyword, replacement)
            elif isinstance(file_contents, list):
                for index, item in enumerate(file_contents):
                    if isinstance(item, (dict, list)):
                        search_and_replace(item, keyword, replacement)
                    elif isinstance(item, str):
                        file_contents[index] = item.replace(keyword, replacement)

        search_and_replace(file_contents, keyword, replacement)
        # print(file_contents) #Replaced file contents

    with open(file_path, 'w', encoding='utf8') as file:
        yaml.safe_dump(file_contents, file, sort_keys=False, default_flow_style=False,
                       allow_unicode=True)  # default_flow_style=False, allow_unicode=True,
        # file.write(ruamel.yaml.load(file_contents,Loader=ruamel.yaml.RoundTripLoader))
        # print (yaml.dump(yaml.load(file_contents), default_flow_style=False))


def plan_and_apply_metrics():
    # This will not work for QA as need to set QA worspace first and schemas / tables not ye available.
    # For DEV the plan command works fine and no errors reported.
    path = shutil.which("bigeye", path="C:\\Users\\username\\bigeye_cli\\Scripts")
    file_path = "C:\\Users\\username\\bigeye_cli\\Scripts\\raw_mds_dim_business_function_copy.bigconfig.yml"
    # cmd = [path, "bigconfig", "--help"]
    cmd = [path, "bigconfig", "plan", "--input_path", file_path]
    p = subprocess.Popen(cmd)  # (cmd, stdout=subprocess.PIPE , stderr=subprocess.PIPE)
    output, error = p.communicate()  # communicate is used to read the command output stored in pipe.
    print(output)
    print(error)
    # print (p.returncode) # 0 for success ** this can be used to further proceed with apply.
    time.sleep(15)
    p.terminate()
    p.wait()


def create_template_metrics():
    # authenticate with Bigeye either via credential file
    api_auth = bigeye_auth.BasicAPIAuth.load_from_file("C:\\Users\\username\\bigeye_cli\\Scripts\\bigeye_cred_file")

    client = bigeye_client.datawatch_client_factory(api_auth)

    table_list_file_path = "C:\\Users\\username\\bigeye_cli\\Scripts\\source_table_list.csv"

    get_table_list(table_list_file_path)

    tab_id = client.get_table_ids(schema_id=131335, table_name='RAW_MDS_DIM_BUSINESS_FUNCTION_COPY')  # 6360644
    print(tab_id)
    # print(client.get_tables(warehouse_id = 1705, schema_id = 131335))
    # SNAP_RAW_US_CSKS : 6199595

    # get_yml_file_export()
    # create_yml_upsert_file(directory_path)
    plan_and_apply_metrics()  # This will not work for QA as need to set QA worspace first and schemas / tables not ye available.


if __name__ == '__main__':
    create_template_metrics()
