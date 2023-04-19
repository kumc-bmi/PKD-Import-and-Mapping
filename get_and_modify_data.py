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
    mapping_df = pd.read_csv('./csvs/mapping.csv', skip_blank_lines=True, dtype=str)

    # ensure all values are lowercase
    col_header_df = mapping_df[['src_var', 'site', 'trg_var']].apply(lambda val: val.str.lower() if val.dtype == 'object' else val)

    # select unique columns from all sites (KUMC, MARYLAND, ALABAMA) where master target columns are not null
    unique_header_cols = col_header_df.loc[col_header_df['trg_var'].notnull(), ['src_var', 'site', 'trg_var']].drop_duplicates(subset=['src_var', 'site', 'trg_var'], keep='first')

    # drop calculated field for later custom logic function
    unique_header_cols_df = unique_header_cols[~unique_header_cols['src_var'].str.contains('trgcalcfield', na=True)]

    site_csv_list = []

    vaiables = main(os_path, openf, argv)
    directory = str(vaiables['raw_data'])
    export_directory = './export/temp/'
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
        source_df['source_val_combined'] = source_df['src_val_lbl'].fillna(source_df['src_val_raw'])

        # drop rows with no src_val_raw or src_val_lbl
        source_df = source_df.dropna(subset=['source_val_combined'])

        # select unique lable variables from all sites (KUMC, MARYLAND, ALABAMA)
        unique_source_values = source_df.loc[source_df['trg_val'].notnull(), ['source_val_combined', 'site', 'trg_var', 'trg_val', 'trg_lbl']].drop_duplicates(subset=['source_val_combined', 'site', 'trg_var', 'trg_val'], keep='first')

        # drop records where trg_val is lower string nan
        unique_source_values = unique_source_values[unique_source_values['trg_val'].str.lower() != 'nan']

        print(unique_source_values)

        # get source value labels for site
        site_src_val = unique_source_values[unique_source_values['site'] == site]

        # raw data for site
        site_data_df = pd.read_csv(directory + site + '.csv', skip_blank_lines=True, dtype=str)

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

        # convert corresponding source row values to target source value
        site_source_mapping = site_src_val[site_src_val['site'] == site]

        # create a dictionary that maps the target source values to the original source site value
        site_column_mapping = dict(zip(site_source_mapping['source_val_combined'], site_source_mapping['trg_val']))

        # create alternate dictionary that maps the target source values if original is not present
        alt_site_column_mapping = dict(zip(site_source_mapping['trg_val'], site_source_mapping['trg_val']))

        # columns with no conversion metric
        exclude_col = ['studyid','visdat','age','gest_age','other_race','diagnosisage','pmhhtn_age_onset','pmhhemat_age_onset','pmhflpain_age_onset','mthr','fthr','partentdx','gstagedx',
                       'gestage','birth_weight','birth_height','apgar1min','apgar5min','ultsound','oligohydra','prenat_enl_kidn','prenat_renal_cyst','prenat_ren_abnor','nicu','interven',
                       'ventil','conven','rpamenarage','rpnumpreg','rpnumliv','parity','uti_preg','pet_preg','eclampsia_preg','preg_prot','pih','preterm_labor','iugr','ectopic','miscarr',
                       'spabort','tabort','electabort','stillbirth','gest_dm','preg_edema','preg_hemat','preg_aki','preg_complic_other','contracep_method','contracep_other','hormone_type',
                       'hormcontracep_age','hormcontracep_dur','estreplage','estrepldur','infertrxage','infertrxdur','rpmenopage','teayn','coffeeyn','sodayn','caffintake','caffdur',
                       'sucigdur','sualcodur','sualcodrinks','occupation','homeexpos','workexpos','otherexpos','otherexpos_spec','contrastnum','bldtransfnum','employstatus','employfull',
                       'employpart','workhr','limitbypkd','reasonunempl','retiredpkd','retiredat','unabletowork','dateunabletowork','reasonunempl_other','height_m','weight','bmi','waist',
                       'temp','average_hr3','average_sysbp3','average_diabp3','tanner_stage','blood_date','glucose','bun','creatinine','cystatin','ckd_epi_egfr','schwartz_gfr','ckid_gfr',
                       'u25_egfr_cr','u25_egfr_cysc','sodium','potassium','chloride','carbon_dioxide','calcium','phosphorus','tot_protein','albumin','ast','alt','alk_phos','gamma_gt',
                       'total_bili','pth','choles','trigly','hdl','ldl','vldl','uric_acid','ph','crp','hgb','hct','rbc_k','mcv','mchc','wbc_k','platelets','iron','ferritin','transferrin',
                       'protime','inr','urine_date','total_pr_urine','urine_microalb','urine_creatinine','urine_calcium','iohexol_gfr','kidmri_dat','kmrires','othrstdy','kv_left',
                       'kl_mr_left','kv_right','kl_mr_right','tkv','subject_height','httkv','mic','livmri_dat','tlv','htlv','lvrmriheplev','mricomments','kultsounddt','kultsoundecho',
                       'kultsound_crdiff','kultsoundres','kultsound_cyst_location','ptpl_2','ptpl','lvrultdt','lvrultheplev','kctscandt','kctscanres','lvrctscandt','port_flow','lvrctheplev',
                       'datecnsus','cnsfib','cnsmri','datecnsmri','patimestamp','pa1a','pa1b','pa1c','pa1d','pa2a','pa2b','pa2c','pa2d','pa3a','pa3b','pa3c','pa3d','pa4a','pa4b','pa4c','pa4d',
                       'pa5a','pa5b','pa5c','pa5d','pa6a','pa6b','pa6c','pa6d','pa7a','pa7b','pa7c','pa7d','pa8a','pa8b','pa8c','pa8d','pa9a','pa9b','pa9c','pa9d','pa10a','pa10b','pa10c',
                       'pa10d','pa11a','pa11b','pa11c','pa11d','pa12a','pa12b','pa12c','pa12d','pa13a','pa13b','pa13c','pa13d','pa14a','pa14b','pa14c','pa14d','pa15a','pa15b','pa15c','pa15d',
                       'pa16a','pa16b','pa16c','pa16d','pkd1muttype','pkd1mutdesc','pkd2muttype','pkd2mutdesc','pkhd1mutdesc','hnf1mutdesc','ganabmutdesc','dnajbmutdesc','oth_gen_nam',
                       'oth_gen_spec','ekg_yn','ekg_comment','mean_night_bp','mean_nt_map','mean_nt_hr','pwvperf','pwvdat','pwvdist','pwvprox','echo_date','echofinding','bd_yn','bdtimestamp',
                       'bd1','bd2','bd3','bd4','bd5','bd6','bd7','bd8','bd9','bd10','bd11','bd12','bd13','bd14','bd15','bd16','bd17','bd18','bd19','bd19a','bd20','bd21','bdi_score','moca_yn',
                       'mcrdate','mc0a','mc1','mc2','mc3a','mc3b','mc3c','mc4a','mc4b','mc5','mc6','mc7','mocascore','pra1','prardate','pra3','pra4','pra5','pra6','pra7','pra8','prf1','prfrdate',
                       'prf3','prf4','prf5','prf6','prf7','prf8','pri1','prirdate','pri3','pri4','pri5','pri6','pri7','pri8','prp1','prprdate','prp3','prp4','prp5','prp6','prp7','prp8','prd1',
                       'prdrdate','prd3','prd4','prd5','prd6','prd7','prd8']
        
        # dictionary comprehension to create column mappings
        column_site_mappings = {col: {val: site_column_mapping.get(val, val) for val in site_data_col_renamed_df[col].unique()} for col in site_data_col_renamed_df.columns if col not in exclude_col}

        print(column_site_mappings)

        # apply the mapping to the column with values to be converted
        site_data_col_renamed_df = site_data_col_renamed_df.replace(column_site_mappings)

        print(site_data_col_renamed_df)

        # attach site name to studyid
        site_data_col_renamed_df['studyid'] = site_data_col_renamed_df['studyid'].apply(lambda x: site + '_' + str(x))
        
        site_data_col_renamed_df =  site_data_col_renamed_df.replace('nan', '')

        site_data_col_renamed_df.dropna(subset=['redcap_event_name'], inplace=True)

        print(site_data_col_renamed_df)

        site_data_col_renamed_df.to_csv(export_directory + site + '/' + site + '.csv', index=False, float_format=None)

        # final converted site raw data
        site_csv_list.append(site_data_col_renamed_df)

    # loop through sites records in dataframe
    for i in range(len(site_csv_list)):
        print(i)
        print(len(site_csv_list))
        # check for missing values
        if site_csv_list[i].isnull().values.any():
            print(site_csv_list[i])
            # handles missing values
            site_csv_list[i] = site_csv_list[i].fillna(0)
    
    # merge all the sites csvs
    merge_site_cvs = pd.concat(site_csv_list, axis=0, ignore_index=True, sort=False)

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
