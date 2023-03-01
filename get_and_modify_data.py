## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)
import os
from logging_module import startlogging
import pandas as pd
import datetime
from datetime import *

# Find if the file has been scanned and dealt with early on:
def whether_file_already_imported(file_name):
    files_recorded = pd.read_csv("./Mapping_csvs/file_fetched.csv", encoding='utf8', header=0, squeeze=true)
    files_already_worked_on = files_recorded.loc[(datetime.datetime.strptime(files_recorded['date_of_import'],'%Y-%b-%d') < date.today()), "File_name"].values.tolist()
    maryland_fiels_to_work_on = []
    # Counter for the entry to file used in file_fetched.csv 0=old, 1=new
    file_new = -1
    
    if file_name in files_already_worked_on:
        file_new = 0
    else:
        file_new = 1
        
    return file_new

## using file name convention as *_PKDUML
def get_maryland_data_from_sftp(umd_base_data_dir):
    startlogging(level='debug')
    objs_from_umd = os.scandir(umd_base_data_dir)
    maryland_fiels_to_work_on = []
    
    for entry in objs_from_umd:
        if (entry.is_file()) and (str(entry.name).endswith(".csv")) and (whether_file_already_imported(entry.name) == 1 and whether_file_already_imported(entry.name) != -1):
            entry_name_split_1 = entry.name.split(".")[0]
            entry_name_split_2 = entry_name_split_1.split("_")
            length_entry_name_split_2 = len(entry_name_split_2)
            if ((entry_name_split_2[length_entry_name_split_2-1]) == "PKDUML"):
                maryland_fiels_to_work_on.append((umd_base_data_dir + "/" + str(entry.name)))
    objs_from_umd.close()
    
    counter_df_appended = 0
    
    for items in maryland_fiels_to_work_on:
        temp_return_df = items.read_csv(items, encoding='utf8', header=0, squeeze=true)
        if counter_df_appended == 0:
            return_df = temp_return_df
        else:
            return_df.append(temp_return_df)
        counter_df_appended = counter_df_appended + 1
        
return return_df

##TODO: Add the pps-client to get token and append in .env file 
def redcap_call(redcap_api_url, datacofig, startlogging, post):
    try:
        log_error_str = """
            redcap rest call was unsuccessful
            or target server is down/ check configuration
            %s
            """ % (data)
        response = post(redcap_api_url, data=datacofig)
        if response.status_code == 200:
            return response.content
        else:
            logging.error('%s : status_code: %s' %
                          (log_error_str, response.status_code))

    except Exception as e:
        startlogging.error('log_error_str : %s' % (e))
    
def mkdirp(newpath):
    if not os.path.exists(newpath):
        os.makedirs(newpath)

def main(config_file, pid_titles, logging, post, join, environ, Path, redcap_api_url, where_to_save):
    startlogging(level="debug")
    error_list = []

    dataconfig = os.environ['data_specs']
    # read children national config file
    
    dataconfig_chld['token'] = os.environ['token_chld']
    dataconfig_chld.update(dataconfig)
    chld_redcap_api_url = os.environ['chld_redcap_api_url']

    # read kumc config file
    dataconfig_kumc['token'] = os.environ['token_kumc']
    dataconfig_kumc.update(dataconfig)
    kumc_redcap_api_url = os.environ['kumc_redcap_api_url']


    # parse config
    # redcap_api_url = config._sections['global']['redcap_api_url']
    if pid_titles == 'ALL':
        pid_titles = [section for section in config.sections()]
    else:
        pid_titles = [pid_titles]

    for pid_title in pid_titles:
        request_payload = dict(config.items(pid_title))

        # reading key from environment variable and replace string with key
        # request_payload['token'] = environ[request_payload['token']]

        # send request to redcap
        data_string = make_redcap_api_call(
            redcap_api_url, request_payload, logging, post)

        record_id = request_payload['record_id']
        title = request_payload['title']

        # creating export path and filename
        file_name = request_payload['export_filename']
        local_export_path = request_payload['local_export_path']
        shared_export_path = request_payload['export_path']

        if data_string == None:
            # API called failed
            error_list.append(record_id)
            break
        
        mkdirp(local_export_path)
        save_file(local_export_path, file_name,
                  data_string, join, Path, logging, record_id, title)

        try:

            if where_to_save == "local_and_pdrive":
                save_file(shared_export_path, file_name,
                          data_string, join, Path, logging, record_id, title)

        except FileNotFoundError as e:
            error_str = "Issue saving file to shared location: %s  and excpetion is: %s" % (
                shared_export_path, e)

            logging.error(error_str)
            error_list.append(record_id)

    if len(error_list) > 0:
        logging.error("""All files are saved local location and shared location , EXCEPT following files with following recording id:
        %s
        """ % (error_list))
        raise()


if __name__ == "__main__":

    def _main_ocap():
        '''
        # https://www.madmode.com/2019/python-eng.html
        '''

        import logging
        from requests import post
        from os import environ
        from os.path import join
        from sys import argv
        from pathlib2 import Path

        startlogging(level="debug")

        if len(argv) != 5:
            logging.error("""Wrong format or arguments :
             please try like 'python download_recap_data.py config_file pid""")

        [umd_base_data_dir] = argv[1:]
        main(umd_base_data_dir, startlogging, post,
             join, environ, Path)

    _main_ocap()
