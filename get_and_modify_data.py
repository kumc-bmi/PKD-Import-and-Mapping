"""
Authors:
- Mary Penne Mays
- Siddharth Satyakam
- Sravani Chandakaq
- Lav Patel
"""

import configparser
import logging
import pandas as pd
import os
import requests
import csv
import pysftp
from sys import argv
from os import path as os_path
from builtins import open as openf
from itertools import groupby

log_details = logging.getLogger(__name__)

# function for missing values
def missing(x):
    if x == 'unk':
        return '888'
    elif x == 'not applicable':
        return '555'
    elif x == 'unknown':
        return '88'
    else:
        return x

def mapped_csvs():
    # based on site convert src_var to trg_var
    # site, src_var, trg_var => (sites, column headers, target column header)
    # TRGCALCFIELD => calculated field
    
    # moving mapping csv into dataframe
    mapping_df = pd.read_csv('./csvs/mapping.csv', skip_blank_lines=True, dtype=str)

    # ensure all values are lowercase
    col_header_df = mapping_df[['src_var', 'site', 'trg_var', 'trg_logic']].apply(lambda val: val.str.lower() if val.dtype == 'object' else val)

    # drop columns with logic
    col_header_df = col_header_df[~(col_header_df['trg_logic'] == 'y')]

    # select unique columns from all sites (KUMC, MARYLAND, ALABAMA) where master target columns are not null
    unique_header_cols = col_header_df.loc[col_header_df['trg_var'].notnull(), ['src_var', 'site', 'trg_var']].drop_duplicates(subset=['src_var', 'site', 'trg_var'], keep='first')

    # drop calculated field for later custom logic function
    unique_header_cols_df = unique_header_cols[~unique_header_cols['src_var'].str.contains('trgcalcfield', na=True)]

     # empty array to store dataframes
    site_csv_list = []

    # fetch defined variables from sys
    vaiables = main(os_path, openf, argv)
    directory = './export/temp/raw_data/'
    import_directory = './import/temp/'

    # empty array to store site names
    site_names = []
    
    # extract site name for csv filename
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            site_names.append(os.path.splitext(filename)[0])
    
    for site in site_names:
        # declare variables
        site_col_headers = site + '_col_headers'
        site_src_val = site + '_src_val'
        site_data_df = site + '_data_df'
        site_column_mapping = site + '_column_mapping'
        site_data_col_renamed_df = site + '_data_col_renamed_df'
        site_source_mapping = site + '_source_mapping'
        site_column_mapping = site + '_column_mapping'

        # get headers for each site
        site_col_headers = unique_header_cols_df[unique_header_cols_df['site'] == site]

        # source values from mapping file
        source_df = mapping_df[['site', 'src_val_raw', 'src_val_lbl', 'trg_var', 'trg_val', 'trg_lbl', 'trg_form_name', 'trg_logic']].apply(lambda val: val.str.lower() if val.dtype == 'object' else val)

        # combine src_val_raw and src_val_lbl into a single column
        source_df['source_val_combined'] = source_df['src_val_raw'].fillna(source_df['src_val_lbl'])

        # drop rows with no src_val_raw or src_val_lbl
        source_df = source_df.dropna(subset=['source_val_combined'])

        # select unique lable variables from all sites (KUMC, MARYLAND, ALABAMA)
        unique_source_values = source_df.loc[source_df['trg_val'].notnull(), ['source_val_combined', 'site', 'src_val_raw', 'src_val_lbl', 'trg_var', 'trg_val', 'trg_lbl']].drop_duplicates(subset=['source_val_combined', 'site', 'src_val_raw', 'src_val_lbl', 'trg_var', 'trg_val'], keep='first')

        # drop records where trg_val is lower string nan
        unique_source_values = unique_source_values[unique_source_values['trg_val'].str.lower() != 'nan']

        # get source value labels for site
        site_src_val = unique_source_values[unique_source_values['site'] == site]

        if site == 'umb':
            # read umb site raw data to dataframe
            site_data_df = pd.read_csv(directory + site + '.csv', skip_blank_lines=True, dtype=str)

            # create a new DataFrame to hold the transposed data
            new_umb_df = pd.DataFrame(columns=['pid', 'cr7', 'race___1', 'race___2', 'race___3', 'race___4', 'race___5', 'race___6'])

            # Iterate over each row in the original DataFrame
            for index, row in site_data_df.iterrows():
                # get the value from the 'cr7' column for the current row
                cr_value = row['cr7']
                        
                # create a new dictionary to hold the values for the current row
                new_row = {'pid': row['pid'], 'cr7': cr_value}
                        
                # Set the value for the corresponding column based on the 'cr7' value
                if cr_value == '1':
                    new_row['race___5'] = '1'
                elif cr_value == '2':
                    new_row['race___1'] = '1'
                elif cr_value == '3':
                    new_row['race___2'] = '1'
                elif cr_value == '4':
                    new_row['race___3'] = '1'
                elif cr_value == '5':
                    new_row['race___4'] = '1'
                elif cr_value == '6':
                    new_row['race___6'] = '1'
            
                # create a new DataFrame from the new_row dictionary
                new_umb_row_df = pd.DataFrame.from_dict(new_row, orient='index').T
            
                # concatenate the new DataFrame to the new_umb_df DataFrame
                new_umb_df = pd.concat([new_umb_df, new_umb_row_df], ignore_index=True, sort=True)

            # remove string nan on dataframe
            new_umb_df =  new_umb_df.fillna('')

            # append race columns to umb dataframe
            site_data_df = pd.merge(site_data_df, new_umb_df[['pid', 'cr7', 'race___1', 'race___2', 'race___3', 'race___4', 'race___5', 'race___6']], on=['pid', 'cr7'], how='left')

            # drop the 'cr7' column
            site_data_df = site_data_df.drop('cr7', axis=1)
        else:
            # read site raw data to dataframe
            site_data_df = pd.read_csv(directory + site + '.csv', skip_blank_lines=True, dtype=str)

        # ensure all values are in lower case
        site_data_df = site_data_df.apply(lambda val: val.str.lower() if val.dtype == 'object' else val)

        # create a dictionary that maps the corrected column names to the original names
        site_column_mapping = dict(zip(site_col_headers['src_var'], site_col_headers['trg_var']))
        
        # create a new dictionary with the updated keys
        updated_column_mapping = {}
        for key, value in site_column_mapping.items():
            if value in site_data_df.columns:
                updated_column_mapping[site_data_df[value].name] = value
            else:
                updated_column_mapping[key] = value

        # match column names if correct in dataframe     
        for col_name in site_data_df.columns:
            if col_name in site_column_mapping and site_column_mapping[col_name] == col_name:
                site_data_df[site_column_mapping[col_name]] = site_data_df[col_name].astype(str)
            else:
                site_data_df[col_name] = site_data_df[col_name].astype(str)
           
        # rename the columns using the dictionary
        site_data_col_renamed_df = site_data_df.rename(columns=updated_column_mapping).drop(columns=[col for col in site_data_df.columns if col not in updated_column_mapping])

        # remove string nan on dataframe
        site_data_col_renamed_df =  site_data_col_renamed_df.replace('nan', '')

        if 'redcap_event_name' not in site_data_col_renamed_df.columns:
            # empty dataframe if event is not present
            site_final_df = pd.DataFrame(columns=[''])
        else:
            # convert corresponding source row values to target source value
            site_source_mapping = site_src_val[site_src_val['site'] == site]

            # create a dictionary that maps the target source values to the original source site value
            site_source_dict = {col: dict(zip(group['source_val_combined'], group['trg_val'])) for col, group in site_source_mapping.groupby('trg_var')}

            # create new empty dataframe for storage
            df_mapped = {}
            # iterate over columns in mapping dictionary
            for col, mapping in site_source_dict.items():
                # check if column is in site data frame
                if col in site_data_col_renamed_df.columns:
                    # If it does exist, map values using source mapping dictionary
                    df_mapped[col] = [mapping.get(val, val) for val in site_data_col_renamed_df[col]]
                else:
                    # If it does not exist, set all values to None
                    df_mapped[col] = pd.Series([None]*len(site_data_col_renamed_df))

            # create new dataframe and apply the mapping to the column with values to be converted
            for col in site_data_col_renamed_df.columns:
                if col not in site_source_dict.keys():
                    df_mapped[col] = site_data_col_renamed_df[col].tolist()

            # create new DataFrame
            site_df_mapped = pd.DataFrame(df_mapped)

            # attach site name to studyid
            site_df_mapped['studyid'] = site_df_mapped['studyid'].apply(lambda x: site + '_' + str(x))

            # ensure studyid and redcap_event_name are first in df
            initial_cols = ['studyid', 'redcap_event_name']

            # reorder the columns
            site_final_df = site_df_mapped.reindex(columns=initial_cols + [col for col in site_df_mapped.columns if col not in initial_cols])

            # make sure all NaN and None values in dataframe are replaced with empty strings
            site_final_df.fillna('', inplace=True)

            # group the dataframe by studyid and redcap_event_name and merge the rows
            site_final_df = site_final_df.groupby(['studyid', 'redcap_event_name']).agg(lambda x: ''.join(x)).reset_index()

            # event dictionary
            event_dict = dict(zip(site_src_val['trg_val'], site_src_val['source_val_combined']))

            # remove unknown event name records
            site_final_df = site_final_df[site_final_df['redcap_event_name'].isin(event_dict.keys())]

            # apply missing function
            site_final_df = site_final_df.applymap(missing)
            
        # export site dataframe to csv
        site_final_df.to_csv(import_directory + site + '/' + site + '.csv', index=False, float_format=None)

        # final converted site raw data
        site_csv_list.append(site_final_df)
    
    # merge all the sites csvs
    merge_site_cvs = pd.concat(site_csv_list, axis=0, ignore_index=True, sort=False)

    # export merged csv file to temporary directory called merged 
    merge_site_cvs.to_csv(import_directory + 'merged/merged.csv', index=False, float_format=None)

    # return merged file
    return merge_site_cvs

# set up connection to REDCap API Export
def redcap_export_api():
    vaiables = main(os_path, openf, argv)
    api_url = str(vaiables['kumc_redcap_api_url'])
    token_kumc = str(vaiables['token_kumc'])
    token_chld = str(vaiables['token_chld'])
    log_details.debug('API URL: %s', api_url)
    export_directory = './export/temp/raw_data/'
    kumc_project_id = str(vaiables['kumc_project_id'])
    chld_project_id = str(vaiables['chld_project_id'])

    kumc_sftp_host = str(vaiables['kumc_sftp_host'])
    kumc_sftp_username = str(vaiables['kumc_sftp_username'])
    kumc_sftp_pwd = str(vaiables['kumc_sftp_pwd'])
    sftp_remote_path = str(vaiables['sftp_remote_path'])
    sftp_port = 22

    # folders for exported files
    folders = ['kumc', 'uab', 'umb']

    for folder in folders:
        # site csv file
        filename = export_directory + folder + '.csv'

        # assign token and project_id
        if folder == 'kumc':
            token = token_kumc
            project_id = kumc_project_id
            print(kumc_project_id)
            print(token_kumc)
        elif folder == 'uab':
            token = token_chld
            project_id = chld_project_id
            print(chld_project_id)
            print(token_chld)
        elif folder == 'umb':
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            # create connection
            with pysftp.Connection(kumc_sftp_host, username=kumc_sftp_username, password=kumc_sftp_pwd, port=sftp_port, cnopts=cnopts) as sftp:

                # remote directory
                sftp.cwd(sftp_remote_path)
                
                # Maryland source file name
                umb_file = folder + '.csv'

                print(sftp_remote_path)
                print(umb_file)

                # download file
                sftp.get(umb_file, export_directory + umb_file)
                print("umb csv file download complete")

                # close session
                sftp.close()

        # data parameters
        data_param = {
            'token': token,
            'content': 'record',
            'action': 'export',
            'format': 'csv',
            'type': 'flat',
            'csvDelimiter': ',',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'false',
            'exportDataAccessGroups': 'false',
            'project_id': project_id,
            'returnContent': 'count',
            'returnFormat': 'json'
        }

        # make the API call to export records
        response = requests.post(api_url, data=data_param)

        print(response)
        
        if response.ok:
            # print the response status from API call
            print('HTTP Status: ' + str(response.status_code))

            records = response.text

            with open(filename, 'w') as f:
                f.write(records)
            # print success message for site
            print(response.text + ' ' + folder + ' data exported successfully') 
        else:
            # print error result for unsucessful export
            print('Error exporting ' + folder + ' data: ', response.text)

# set up connection to REDCap API Import
def redcap_import_api():
    vaiables = main(os_path, openf, argv)
    api_url = str(vaiables['kumc_redcap_api_url'])
    api_import_token = str(vaiables['import_token'])
    log_details.debug('API URL: %s', api_url)
    import_directory = './import/temp/'
    project_id = str(vaiables['project_id'])

    # folders for imported files
    folders = ['kumc','umb','uab']

    for folder in folders:
        # site csv file
        filename = import_directory + folder + '/' + folder + '.csv'

        with open(filename, 'r') as f:
            reader = csv.reader(f)
            data_records = "\r\n".join([",".join(row) for row in reader])

        # data parameters
        data_param = {
            'token': api_import_token,
            'content': 'record',
            'action': 'import',
            'format': 'csv',
            'type': 'flat',
            'overwriterBehavior': 'normal',
            'forceAutoNumber': 'false',
            'data': data_records,
            'dateFormat': 'MDY',
            'project_id': project_id,
            'returnContent': 'count',
            'returnFormat': 'json'
        }
        
        # make the API call to import records
        response = requests.post(api_url, data=data_param)
        
        if response.ok:
            # print the response status from API call
            print('HTTP Status: ' + str(response.status_code))
            # print success message for site
            print(response.text + ' ' + folder + ' records imported successfully')
        else:
            # print error result for unsucessful import
            print('Error importing ' + folder + '.csv file: ', response.text)

def main(os_path, openf, argv):
    def get_config():
        [config_fn, pid] = argv[1:3]
        config = configparser.SafeConfigParser()
        config_fp = openf(config_fn)
        config.readfp(config_fp, filename=config_fn)

        values = {}

        # default attributes
        values['import_dir'] = config.get('default', 'import_dir')
        values['export_dir'] = config.get('default', 'export_dir')
        values['raw_data'] = config.get('default', 'raw_data')
        values['data_content'] = config.get('default', 'data_content')
        values['data_event'] = config.get('default', 'data_event')
        values['data_metadata'] = config.get('default', 'data_metadata')
        values['data_action_export'] = config.get('default', 'data_action_export')
        values['data_action_import'] = config.get('default', 'data_action_import')
        values['data_format'] = config.get('default', 'data_format')
        values['data_type'] = config.get('default', 'data_type')
        values['data_delimiter'] = config.get('default', 'data_delimiter')
        values['data_label'] = config.get('default', 'data_label')
        values['data_true'] = config.getboolean('default', 'data_true')
        values['data_return_count'] = config.get('default', 'data_return_count')
        values['data_return_format'] = config.get('default', 'data_return_format')
        values['data_overwrite'] = config.get('default', 'data_overwrite')
        values['data_date_format'] = config.get('default', 'data_date_format')
        
        # api
        values['kumc_redcap_api_url'] = config.get('api', 'kumc_redcap_api_url')
        values['chld_redcap_api_url'] = config.get('api', 'chld_redcap_api_url')
        values['verify_ssl'] = config.getboolean('api', 'verify_ssl')
        
        # tokens
        values['import_token'] = config.get(pid, 'import_token')
        values['export_token'] = config.get(pid, 'export_token')
        values['token_kumc'] = config.get(pid, 'token_kumc')
        values['token_chld'] = config.get(pid, 'token_chld')
        values['proj_token'] = config.get(pid, 'proj_token')
        values['file_dest'] = config.get(pid, 'file_dest')
        values['project_id'] = config.get(pid, 'project_id')
        values['kumc_project_id'] = config.get(pid, 'kumc_project_id')
        values['chld_project_id'] = config.get(pid, 'chld_project_id')
        values['test_project_id'] = config.get(pid, 'test_project_id')
        values['kumc_sftp_host'] = config.get(pid, 'kumc_sftp_host')
        values['kumc_sftp_username'] = config.get(pid, 'kumc_sftp_username')
        values['kumc_sftp_pwd'] = config.get(pid, 'kumc_sftp_pwd')
        values['sftp_remote_path'] = config.get(pid, 'sftp_remote_path')

        return values 
    return get_config()
        

if __name__ == "__main__":
    # export site(s) csvs from redcap through API
    redcap_export_api()
    # map sites redcap projects and export to csvs
    mapped_csvs()
    # import all converted site(s) csvs into redcap through API
    redcap_import_api()
