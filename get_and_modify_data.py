## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)
import os
import logging
import pandas as pd
import datetime
from datetime import *
from io import StringIO

# Find if the file has been scanned and dealt with early on:
def whether_file_already_imported(file_name,datetime,date):
    files_recorded = pd.read_csv("./logs/maryland_file_log.csv", encoding='utf8', header=0, squeeze='true')
    files_recorded['DATE_IMPRT'] = pd.to_datetime(files_recorded['DATE_IMPRT'])
    files_recorded['DATE_IMPRT'] = files_recorded['DATE_IMPRT'].dt.date
    files_already_worked_on = files_recorded.loc[(files_recorded['DATE_IMPRT'] < date.today()), "FILE_NAME"].values.tolist()
    maryland_fiels_to_work_on = []
    # Counter for the entry to file used in file_fetched.csv 0=old, 1=new
    file_new = -1
    
    if file_name in files_already_worked_on:
        file_new = 0
    else:
        file_new = 1
        
    return file_new

## using file name convention as *_PKDUML
def get_maryland_data_from_sftp(umd_base_data_dir,scandir,logging,date,datetime):
    objs_from_umd = scandir(umd_base_data_dir)
    maryland_fiels_to_work_on = []
    
    for entry in objs_from_umd:
        print(entry.name)
        if (entry.is_file()) and (str(entry.name).endswith(".csv")) and (whether_file_already_imported(entry.name,datetime,date) == 1 and whether_file_already_imported(entry.name,datetime,date) != -1):
            entry_name_split_1 = entry.name.split(".")[0]
            entry_name_split_2 = entry_name_split_1.split("_")
            length_entry_name_split_2 = len(entry_name_split_2)
            if ((entry_name_split_2[length_entry_name_split_2-1]) == "PKDUML"):
                maryland_fiels_to_work_on.append((umd_base_data_dir + "/" + str(entry.name)))
            file_read_earlier = pd.read_csv("./logs/maryland_file_log.csv", encoding='utf8', header=0, squeeze='true')
            new_row = {'FILE_NAME':entry.name, 'DATE_IMPRT':date.today()}
            file_read_earlier.append(new_row, ignore_index=True)
            file_read_earlier.to_csv("./logs/maryland_file_log.csv")
            
    objs_from_umd.close()
    
    counter_df_appended = 0
    return_df = pd.DataFrame()
    
    for items in maryland_fiels_to_work_on:
        temp_return_df = pd.read_csv(items, encoding='utf8', header=0, squeeze='true')
        if counter_df_appended == 0:
            return_df = temp_return_df
        else:
            return_df.append(temp_return_df)
        counter_df_appended = counter_df_appended + 1
    
    return return_df

## Using code referrence from: 
## https://github.com/kumc-bmi/redcapex/blob/29b49d21836a7337beebc1a8b8c7de3dbc541691/download_redcap_data.py#L8
## TODO: Add the pps-client to get token and append in .env file 
def redcap_call(redcap_api_url, token, os, logging, post):
    try:
        dataconfig = dict()
        dataconfig["token"] = token
        for key, value in os.environ.items():
            if key.startswith("API_data_"):
                key_entry = key.split("API_data_")[1]
                dataconfig[key_entry] = value

        logging.info(os.environ.items())
        # Referrenced from code in 
        # https://github.com/kumc-bmi/redcapex/blob/29b49d21836a7337beebc1a8b8c7de3dbc541691/download_redcap_data.py#L10
        log_error_str = """
            redcap rest call was unsuccessful
            or target server is down/ check configuration
            %s
            """ % (dataconfig)
        response = post(redcap_api_url, data=dataconfig)
        print(response)
        if response.status_code == 200:
            print(type(response.content))
            return response.content
        else:
            logging.error(response.content)

    except Exception as e:
        logging.error('log_error_str : %s' % (e))

#def mapping_kumc(df_kumc,mapping_csv,logging,os,post):
#def mapping_UAB(df_chld,mapping_csv,logging,os,post):
#def mapping_UMB(df_maryland,mapping_csv,logging,os,post):
  
def main(umd_base_data_dir, logging, post, scandir, os, StringIO,date,datetime):
    error_list = []
    
    # read children national config file
    data_token_chld = os.getenv('token_chld')
    chld_redcap_api_url = os.getenv('chld_redcap_api_url')

    # read kumc config file
    data_token_kumc = os.getenv('token_kumc')
    kumc_redcap_api_url = os.getenv('kumc_redcap_api_url')

    # redcap calls-KUMC
    df_kumc = redcap_call(kumc_redcap_api_url, data_token_kumc, os, logging, post)
    temp_df_kumc = str(df_kumc,'utf-8')
    data_kumc = StringIO(temp_df_kumc)
    data_df_kumc = pd.read_csv(data_kumc, header=0, encoding='utf-8')
    #print(data_df_kumc)

    # redcap calls-CHLD
    df_chld = redcap_call(chld_redcap_api_url, data_token_chld, os, logging, post)
    temp_df_chld = str(df_chld,'utf-8')
    data_chld = StringIO(temp_df_chld)
    data_df_chld = pd.read_csv(data_chld, header=0, encoding='utf-8')
    #print(data_df_chld)
    
    # redcap calls-MARYLAND
    data_df_maryland = get_maryland_data_from_sftp(umd_base_data_dir, scandir, logging,date,datetime)
    
    # Mapping data:
    mapping_kumc = pd.read_csv("./Mapping_csvs/mapping_KUMC_KUMC.csv", encoding='utf8', header=0)
    print(mapping_kumc.head)
    #logging.error(error_str)
    #error_list.append(record_id)

    #if len(error_list) > 0:
    #    logging.error("""All files are saved local location and shared location , EXCEPT following files with following recording id:
    #    %s
    #    """ % (error_list))
    #    raise()


if __name__ == "__main__":

    def _main_ocap():
        '''
        # https://www.madmode.com/2019/python-eng.html
        '''

        import logging
        from requests import post
        from os.path import join
        import sys
        import pandas as pd
        from pathlib2 import Path
        from os import scandir
        from datetime import date
        from dotenv import load_dotenv
        from datetime import datetime
        import os
        from io import StringIO

        load_dotenv()

        logging.basicConfig(filename="./logs/logging.log", level="DEBUG", format=os.getenv('log_format'), datefmt='%Y-%m-%d %H:%M:%S')
        
        logging.info("""System Arguments sent: {}""".format(sys.argv))
        
        if len(sys.argv) != 2:
            logging.info("""Wrong format or arguments :
             please try like 'python download_recap_data.py config_file pid""")

        [umd_base_data_dir] = sys.argv[1:]
        main(umd_base_data_dir, logging, post,
             scandir, os, StringIO,date,datetime)

    _main_ocap()
