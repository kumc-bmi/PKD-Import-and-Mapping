## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

import configparser
import logging
import pandas as pd
import os
from sys import argv
from os import path as os_path
from __builtin__ import open as openf

log_details = logging.getLogger(__name__)

def mapped_headers():
    # based on site convert src_var to trg_var
    # site, src_var, trg_var => (sites, column headers, target column header)
    # TRGCALCFIELD => calculated field
    
    # moving mapping csv into dataframe
    mapping_df = pd.read_csv('./csvs/mapping.csv', skip_blank_lines=True)

    # ensure all values are lowercase
    col_header_df = mapping_df[['src_var', 'site', 'trg_var']].apply(lambda val: val.str.lower() if val.dtype == 'object' else val)

    # select unique columns from all sites (KUMC, MARYLAND, ALABAMA) where master target columns are not null
    unique_header_cols = col_header_df.loc[col_header_df['trg_var'].notnull(), ['src_var', 'site', 'trg_var']].drop_duplicates(subset=['src_var', 'site', 'trg_var'], keep='first')

    # drop calculated field for later custom logic function
    unique_header_cols_df = unique_header_cols[~unique_header_cols['src_var'].str.contains('trgcalcfield', na=True)]

    site_csv_list = []

    vaiables = main(os_path, openf, argv)
    directory = str(vaiables['raw_data'])
    site_names = []
    
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
        source_df = mapping_df[['site', 'src_val_raw', 'src_val_lbl', 'trg_var', 'trg_val', 'trg_lbl', 'trg_form_name']].apply(lambda val: val.str.lower() if val.dtype == 'object' else val)

        # combine src_val_raw and src_val_lbl into a single column
        source_df['source_val_combined'] = source_df['src_val_raw'].fillna(source_df['src_val_lbl'])

        # drop rows with no src_val_raw or src_val_lbl
        source_df = source_df.dropna(subset=['source_val_combined'])

        # select unique lable variables from all sites (KUMC, MARYLAND, ALABAMA)
        unique_source_values = source_df.loc[source_df['source_val_combined'].notnull(), ['source_val_combined', 'site', 'trg_var', 'trg_val', 'trg_lbl']].drop_duplicates(subset=['source_val_combined', 'site', 'trg_var', 'trg_val'], keep='first')

        # get source value labels for site
        site_src_val = unique_source_values[unique_source_values['site'] == site]

        # raw data for site
        site_data_df = pd.read_csv(directory + site + '.csv', skip_blank_lines=True)

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

        # convert corresponding source row values to target source value
        site_source_mapping = site_src_val[site_src_val['site'] == site]

        # create a dictionary that maps the target source values to the original source site value
        site_column_mapping = dict(zip(site_source_mapping['source_val_combined'], site_source_mapping['trg_val']))

        # create alternate dictionary that maps the target source values if original is not present
        alt_site_column_mapping = dict(zip(site_source_mapping['trg_val'], site_source_mapping['trg_val']))

        # apply the mapping to the column with values to be converted
        site_data_col_renamed_df['redcap_event_name'] = site_data_col_renamed_df['redcap_event_name'].map(site_column_mapping).fillna(site_data_col_renamed_df['redcap_event_name'].map(alt_site_column_mapping))

        # attach site name to studyid
        site_data_col_renamed_df['studyid'] = site_data_col_renamed_df['studyid'].apply(lambda x: site + '_' + str(x))

        print(site_data_col_renamed_df)

        # final converted site raw data
        site_csv_list.append(site_data_col_renamed_df)

    
    # merge all the sites csvs
    merge_site_cvs = pd.concat([site_csv_list[0], site_csv_list[1], site_csv_list[2]], sort=False)

    # return merged file
    return merge_site_cvs

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
        values['token_kumc'] = config.get(pid, 'token_kumc')
        values['token_chld'] = config.get(pid, 'token_chld')
        values['proj_token'] = config.get(pid, 'proj_token')
        values['file_dest'] = config.get(pid, 'file_dest')

        return values 
    return get_config()
        

if __name__ == "__main__":

    def _main_ocap():
        from sys import argv
        from os import path as os_path
        from __builtin__ import open as openf

        config_values = main(os_path, openf, argv)
        print(config_values)

        site_headers = mapped_headers()
        print(site_headers)

    _main_ocap()
