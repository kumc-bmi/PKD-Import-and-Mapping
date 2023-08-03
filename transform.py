"""
Authors:
- Mary Penne Mays
- Siddharth Satyakam
- Sravani Chandakaq
- Lav Patel
"""
import os
import sys

# set directory path
dir_path = "/var/lib/jenkins/jobs/PKI MULTI-SITE REGISTRY/workspace/"

# set system path
sys.path.append(os.path.abspath(dir_path))

from env_attrs import *

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
    col_header_df_lower = mapping_df[['src_var', 'site', 'trg_var', 'trg_logic', 'trg_val_calc']].apply(lambda val: val.str.lower() if val.dtype == 'object' else val)

    # drop columns with logic
    col_header_df = col_header_df_lower[~(col_header_df_lower['trg_logic'] == 'y')]

    # columns with logic
    col_header_logic = col_header_df_lower[~(col_header_df_lower['trg_logic'] != 'y')]

    # select unique columns from all sites (KUMC, MARYLAND, ALABAMA) where master target columns are not null
    unique_header_cols = col_header_df.loc[col_header_df['trg_var'].notnull(), ['src_var', 'site', 'trg_var']].drop_duplicates(subset=['src_var', 'site', 'trg_var'], keep='first')

    # select unique columns from logic fields (KUMC, MARYLAND, ALABAMA) where master target columns are not null
    unique_header_cols_logic = col_header_logic.loc[col_header_logic['trg_var'].notnull(), ['src_var', 'site', 'trg_var']].drop_duplicates(subset=['src_var', 'site', 'trg_var'], keep='first')
    
    # drop calculated field for later custom logic function
    unique_header_cols_df = unique_header_cols[~unique_header_cols['src_var'].str.contains('trgcalcfield', na=True)]

     # empty array to store dataframes
    site_csv_list = []

    # fetch defined variables from sys
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

        # create a logic DataFrame
        logic_cols_df = pd.DataFrame(columns=['studyid', 'redcap_event_name', 'age', 'diagnosisage', 'pmhhtn_age_onset', 'mthr', 'fthr', 'birth_weight', 'rpmenopage', 'teayn', 'coffeeyn', 'sodayn', 'caffintake', 'caffdur', 'smokever', 'sualcodur',
                                             'sualcodrinks', 'tolvaptan_treat', 'height_m', 'average_sysbp3', 'average_diabp3', 'creatinine', 'albumin', 'wbc_k', 'urine_microalb', 'subject_height'])
        
        if site == 'kumc':
           # convert onetime forms to baseline arm
           site_data_df.loc[site_data_df['redcap_event_name'] == 'onetime_forms_and_arm_1', 'redcap_event_name'] = 'baseline_arm_1'

        # make sure all NaN and None values in dataframe are replaced with empty strings
        site_data_df.fillna('', inplace=True)
        
        if site == 'kumc':
            # combine rows with related data based on studyid and redcap_event_name
            site_data_df = site_data_df.groupby(['studyid', 'redcap_event_name']).agg(lambda x: ''.join(x)).reset_index()
        
        # export processed all records including logic fields to csv
        site_data_df.to_csv(import_directory + 'merged/' + site + '_to_be_merged.csv', index=False, float_format=None)

        for index, row in site_data_df.iterrows():
            if site == 'kumc':
                # create a new dictionary to hold the values for the kumc current row
                studyid = row['studyid']
                redcap_event_name = row['redcap_event_name']

                if (row['diagnstatus'] == 'diagnosed with adpkd' and pd.notna(row['age']) and pd.notna(row['dmdat']) and pd.notna(row['diagndate'])) or (row['diagnstatus'] == '1' and pd.notna(row['age']) and pd.notna(row['dmdat']) and pd.notna(row['diagndate'])):
                    diagnosisage = str(pd.to_numeric(row['age']) - pd.to_numeric(pd.to_datetime(row['dmdat']).year - pd.to_datetime(row['diagndate']).year))
                else:
                    diagnosisage = ''

                if (row['fm1rel'] == 'mother' and row['fm1diagn'] == 'yes') or (row['fm1rel'] == '1' and row['fm1diagn'] == '1'):
                    mthr = '1'
                elif (row['fm1rel'] == 'mother' and row['fm1diagn'] == 'no') or (row['fm1rel'] == '1' and row['fm1diagn'] == '0'):
                    mthr = '0'
                elif (row['fm2rel'] == 'mother' and row['fm2diagn'] == 'yes') or (row['fm2rel'] == '1' and row['fm2diagn'] == '1'):
                    mthr = '1'
                elif (row['fm2rel'] == 'mother' and row['fm2diagn'] == 'no') or (row['fm2rel'] == '1' and row['fm2diagn'] == '0'):
                    mthr = '0'
                elif (row['fm3rel'] == 'mother' and row['fm3diagn'] == 'yes') or (row['fm3rel'] == '1' and row['fm3diagn'] == '1'):
                    mthr = '1'
                elif (row['fm3rel'] == 'mother' and row['fm3diagn'] == 'no') or (row['fm3rel'] == '1' and row['fm3diagn'] == '0'):
                    mthr = '0'
                elif (row['fm4rel'] == 'mother' and row['fm4diagn'] == 'yes') or (row['fm4rel'] == '1' and row['fm4diagn'] == '1'):
                    mthr = '1'
                elif (row['fm4rel'] == 'mother' and row['fm4diagn'] == 'no') or (row['fm4rel'] == '1' and row['fm4diagn'] == '0'):
                    mthr = '0'
                else:
                    mthr = ''

                if (row['fm1rel'] == 'father' and row['fm1diagn'] == 'yes') or (row['fm1rel'] == '2' and row['fm1diagn'] == '1'):
                    fthr = '1'
                elif (row['fm1rel'] == 'father' and row['fm1diagn'] == 'no') or (row['fm1rel'] == '2' and row['fm1diagn'] == '0'):
                    fthr = '0'
                elif (row['fm2rel'] == 'father' and row['fm2diagn'] == 'yes') or (row['fm2rel'] == '2' and row['fm2diagn'] == '1'):
                    fthr = '1'
                elif (row['fm2rel'] == 'father' and row['fm2diagn'] == 'no') or (row['fm2rel'] == '2' and row['fm2diagn'] == '0'):
                    fthr = '0'
                elif (row['fm3rel'] == 'father' and row['fm3diagn'] == 'yes') or (row['fm3rel'] == '2' and row['fm3diagn'] == '1'):
                    fthr = '1'
                elif (row['fm3rel'] == 'father' and row['fm3diagn'] == 'no') or (row['fm3rel'] == '2' and row['fm3diagn'] == '0'):
                    fthr = '0'
                elif (row['fm4rel'] == 'father' and row['fm4diagn'] == 'yes') or (row['fm4rel'] == '2' and row['fm4diagn'] == '1'):
                    fthr = '1'
                elif (row['fm4rel'] == 'father' and row['fm4diagn'] == 'no') or (row['fm4rel'] == '2' and row['fm4diagn'] == '0'):
                    fthr = '0'
                else:
                    fthr = ''

                if (pd.notna(row['suteacups']) and pd.to_numeric(row['suteacups'], errors='coerce') > 0):
                    teayn = '1'
                elif (pd.notna(row['suteacups']) and row['suteacups'] == '0'):
                    teayn = '0'
                else:
                    teayn = ''

                if (pd.notna(row['sucoffeecups']) and pd.to_numeric(row['sucoffeecups'], errors='coerce') > 0):
                    coffeeyn = '1'
                elif (pd.notna(row['sucoffeecups']) and row['sucoffeecups'] == '0'):
                    coffeeyn = '0'
                else:
                    coffeeyn = ''

                if (pd.notna(row['susodacups']) and pd.to_numeric(row['susodacups'], errors='coerce') > 0):
                    sodayn = '1'
                elif (pd.notna(row['susodacups']) and row['susodacups'] == '0'):
                    sodayn = '0'
                else:
                    sodayn = ''

                if (pd.notna(row['suteacups']) or pd.notna(row['sucoffeecups']) or pd.notna(row['susodacups'])):
                    caffintake = (pd.to_numeric(row['suteacups'], errors='coerce') + pd.to_numeric(row['sucoffeecups'], errors='coerce') + pd.to_numeric(row['susodacups'], errors='coerce')).astype(str)
                else:
                    caffintake = ''

                if (pd.notna(row['sucaffenage']) and pd.notna(row['sucaffstage']) and pd.to_numeric(row['sucaffenage'], errors='coerce') >= 0):
                    caffdur = (pd.to_numeric(row['sucaffenage'], errors='coerce') - pd.to_numeric(row['sucaffstage'], errors='coerce')).astype(str)
                elif (pd.notna(row['age']) and pd.notna(row['sucaffstage']) and pd.to_numeric(row['sucaffstage'], errors='coerce') >= 0):
                    caffdur = (pd.to_numeric(row['age'], errors='coerce') - pd.to_numeric(row['sucaffstage'], errors='coerce')).astype(str)
                else:
                    caffdur = ''

                if (pd.isna(row['sualcoenage']) and pd.notna(row['age']) and pd.notna(row['sualcostage'])):
                    sualcodur = (pd.to_numeric(row['age'], errors='coerce') - pd.to_numeric(row['sualcostage'], errors='coerce')).astype(str)
                elif (pd.notna(row['sualcoenage']) and pd.notna(row['sualcostage']) and pd.to_numeric(row['sualcoenage'], errors='coerce') > 0 and pd.to_numeric(row['sualcostage'], errors='coerce') > 0):
                    sualcodur = (pd.to_numeric(row['sualcoenage'], errors='coerce') - pd.to_numeric(row['sualcostage'], errors='coerce')).astype(str)
                else:
                    sualcodur = ''

                if pd.notna(row['height']):
                    height_m = (pd.to_numeric(row['height'], errors='coerce')/100).astype(str)
                else:
                    height_m = ''

                if pd.notna(row['average_sysbp3']):
                    average_sysbp3 = row['average_sysbp3']
                else:
                    average_sysbp3 = ''

                if pd.notna(row['average_diabp3']):
                    average_diabp3 = row['average_diabp3']
                else:
                    average_diabp3 = ''
                
                # create a new DataFrame from the logic_row dictionary
                new_logic_row = {'studyid': studyid, 'redcap_event_name': redcap_event_name, 'diagnosisage': diagnosisage, 'mthr': mthr, 'fthr': fthr, 'teayn': teayn, 'coffeeyn': coffeeyn, 'sodayn': sodayn, 'caffintake': caffintake, 'caffdur': caffdur, 'sualcodur': sualcodur, 'height_m': height_m, 'average_sysbp3': average_sysbp3, 'average_diabp3': average_diabp3}

                # concatenate the new DataFrame to the logic_cols_df DataFrame
                logic_cols_df = pd.concat([logic_cols_df, pd.DataFrame([new_logic_row])], ignore_index=True)

            if site == 'umb':
                # create a new dictionary to hold the values for the umb current row
                studyid = row['pid']
                
                if 'redcap_event_name' in row.index: 
                    redcap_event_name = row['redcap_event_name']
                else:
                    redcap_event_name = row['crvisit']

                if 'crrdate' in row.index and 'cr4' in row.index and pd.notna(row['crrdate']) and pd.notna(row['cr4']):
                    age = str((pd.to_datetime(row['crrdate'])).year - (pd.to_datetime(row['cr4'])).year)
                else:
                    age = ''

                if 'crrdate' in row.index and 'cr4' in row.index and pd.notna(row['crrdate']) and pd.notna(row['cr4']):
                    pmhhtn_age_onset = str((pd.to_datetime(row['crrdate'])).year - (pd.to_datetime(row['cr4'])).year)
                else:
                    pmhhtn_age_onset = ''

                if 'c5a' in row.index and pd.notna(row['c5a']) and row['c5a'] == 'kg':
                    birth_weight = (pd.to_numeric(row['c5a'], errors='coerce') * 1000).astype(str)
                elif 'c5a' in row.index and pd.notna(row['c5a']) and row['c5a'] == 'lb':
                    birth_weight = (pd.to_numeric(row['c5a'], errors='coerce') * 453.6).astype(str)
                elif 'c5a' in row.index and pd.notna(row['c5a']) and row['c5a'] == 'oz':
                    birth_weight = (pd.to_numeric(row['c5a'], errors='coerce') * 28.3).astype(str)
                else:
                    birth_weight = ''

                if  'cr28a' in row.index and 'cr4' in row.index and pd.notna(row['cr28a']) and pd.notna(row['cr4']):
                    rpmenopage = str((pd.to_datetime(row['cr28a'])).year - (pd.to_datetime(row['cr4'])).year)
                else:
                    rpmenopage = ''

                if 'cr66' in row.index and row['cr66'] in ('yes, tea', 'yes, both'):
                    teayn = '1'
                elif 'cr66' in row.index and row['cr66'] in ('yes, coffee', 'no, (both)'):
                    teayn = '0'
                else:
                    teayn = ''

                if 'cr66' in row.index and row['cr66'] in ('yes, coffee', 'yes, both'):
                    coffeeyn = '1'
                elif 'cr66' in row.index and row['cr66'] in ('yes, tea', 'no, (both)'):
                    coffeeyn = '0'
                else:
                    coffeeyn = ''

                if ('hb1' in row.index and row['hb1'] == 'yes') or ('hb9' in row.index and row['hb9'] == 'yes') or ('hb15' in row.index and row['hb15'] == 'yes'):
                    smokever = '1'
                elif ('hb1' in row.index and row['hb1'] == 'no') or ('hb9' in row.index and row['hb9'] == 'no') or ('hb15' in row.index and row['hb15'] == 'no'):
                    smokever = '0'
                else:
                    smokever = ''

                if 'hb30' in row.index and row['hb30'] == 'yes':
                    sualcodur = str((pd.to_datetime(row['crrdate'])).year - (pd.to_datetime(row['cr4'])).year)
                elif 'hb29a' in row.index and 'hb31a' in row.index and 'hb30' in row.index and row['hb30'] == 'no':
                    sualcodur = str((pd.to_datetime(row['hb31a'])).year - (pd.to_datetime(row['hb29a'])).year)
                else:
                    sualcodur = ''

                if 'hb35' in row.index and 'hb36' in row.index and 'hb37' in row.index and'hb30' in row.index and row['hb30'] == 'yes':
                    sualcodrinks = (pd.to_numeric(row['hb35'], errors='coerce') + pd.to_numeric(row['hb36'], errors='coerce') + pd.to_numeric(row['hb37'], errors='coerce')).astype(str)
                elif 'hb32' in row.index and 'hb33' in row.index and 'hb34' in row.index and 'hb30' in row.index and row['hb30'] == 'no':
                    sualcodrinks = (pd.to_numeric(row['hb32'], errors='coerce') + pd.to_numeric(row['hb33'], errors='coerce') + pd.to_numeric(row['hb34'], errors='coerce')).astype(str)
                else:
                    sualcodrinks = ''

                if 'cm1' in row.index and row['cm1'] == 'done' and (
                    ('cm6' in row.index and row['cm6'] in ('tolvaptan', 'jynarque')) or
                    ('cm7' in row.index and row['cm7'] in ('tolvaptan', 'jynarque')) or
                    ('cm8' in row.index and row['cm8'] in ('tolvaptan', 'jynarque')) or
                    ('cm9' in row.index and row['cm9'] in ('tolvaptan', 'jynarque')) or
                    ('cm10' in row.index and row['cm10'] in ('tolvaptan', 'jynarque')) or
                    ('cm11' in row.index and row['cm11'] in ('tolvaptan', 'jynarque')) or
                    ('cm12' in row.index and row['cm12'] in ('tolvaptan', 'jynarque')) or
                    ('cm13' in row.index and row['cm13'] in ('tolvaptan', 'jynarque')) or
                    ('cm14' in row.index and row['cm14'] in ('tolvaptan', 'jynarque')) or
                    ('cm15' in row.index and row['cm15'] in ('tolvaptan', 'jynarque'))):
                    tolvaptan_treat = '1'
                elif 'cm1' in row.index and row['cm1'] == 'done':
                    tolvaptan_treat = '0'
                else:
                    tolvaptan_treat = ''

                if 'pf8a' in row.index and pd.notna(row['pf8a']) and row['pf8a'] != '':
                    height_m = (pd.to_numeric(row['pf8a'], errors='coerce') / 100).astype(str)
                elif 'pf8b' in row.index and pd.notna(row['pf8b']) and row['pf8b'] != '':
                    height_m = (pd.to_numeric(row['pf8b'], errors='coerce') * 0.0254).astype(str)
                else:
                    height_m = ''

                # average_sysbp3'] =
                # average_diabp3'] =
                # urine_microalb'] =
                # subject_height'] =
                # livercysts_mr_num'] =

                # create a new DataFrame from the logic_row dictionary
                new_logic_row = {'studyid': studyid, 'redcap_event_name': redcap_event_name, 'age': age, 'pmhhtn_age_onset': pmhhtn_age_onset, 'birth_weight': birth_weight, 'rpmenopage': rpmenopage, 'teayn': teayn, 'coffeeyn': coffeeyn, 'smokever': smokever, 'sualcodur': sualcodur, 'sualcodrinks': sualcodrinks, 'tolvaptan_treat': tolvaptan_treat, 'height_m': height_m}

                # concatenate the new DataFrame to the logic_cols_df DataFrame
                logic_cols_df = pd.concat([logic_cols_df, pd.DataFrame([new_logic_row])], ignore_index=True)

            if site == 'uab':
                # create a new dictionary to hold the values for the uab current row
                studyid = row['subject_id']
                redcap_event_name = row['redcap_event_name']
            
                if 'date_contact' in row.index and pd.notna(row['date_contact']) and ('birthdate') in row.index and pd.notna(row['birthdate']):
                    age = str((pd.to_datetime(row['date_contact'])).year - (pd.to_datetime(row['birthdate'])).year)
                else:
                    age = ''

                if 'hypertdx' in row.index and pd.notna(row['hypertdx']) and ('birthdate') in row.index and pd.notna(row['birthdate']):
                    pmhhtn_age_onset = str((pd.to_datetime(row['hypertdx'])).year - (pd.to_datetime(row['birthdate'])).year)
                else:
                    pmhhtn_age_onset = ''

                if 'omedspec' in row.index and row['omedspec'] in ('tolvaptan', 'jynarque'):
                    tolvaptan_treat = '1'
                else:
                    tolvaptan_treat = ''

                if 'creatinelvl' in row.index and row['creatinelvl'] == 'mg/dl' and ('lstcreatine') in row.index and pd.notna(row['lstcreatine']):
                    creatinine = row['lstcreatine']
                elif 'creatinelvl' in row.index and row['creatinelvl'] == 'mmol/l' and ('lstcreatine') in row.index and pd.notna(row['lstcreatine']):
                    creatinine = (pd.to_numeric(row['lstcreatine'], errors='coerce') / 88.4).astype(str)
                else:
                    creatinine = ''

                if 'album' in row.index and pd.notna(row['album']):
                    albumin = (pd.to_numeric(row['album'], errors='coerce') / 10).astype(str)
                else:
                    albumin = ''

                if 'wbc' in row.index and pd.notna(row['wbc']):
                    wbc_k = (pd.to_numeric(row['wbc'], errors='coerce') / 1000).astype(str)
                else:
                    wbc_k = ''

                # create a new DataFrame from the logic_row dictionary
                new_logic_row = {'studyid': studyid, 'redcap_event_name': redcap_event_name, 'age': age, 'pmhhtn_age_onset': pmhhtn_age_onset, 'tolvaptan_treat': tolvaptan_treat, 'creatinine': creatinine, 'albumin': albumin, 'wbc_k': wbc_k}

                 # concatenate the new DataFrame to the logic_cols_df DataFrame
                logic_cols_df = pd.concat([logic_cols_df, pd.DataFrame([new_logic_row])], ignore_index=True)

        # remove string nan on dataframe
        logic_cols_df = logic_cols_df.replace('nan', '')
       
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
        
        if site == 'kumc':
            # append race columns to kumc dataframe
            site_final_df = pd.merge(site_final_df, logic_cols_df[['studyid', 'redcap_event_name', 'diagnosisage','mthr','fthr','teayn','coffeeyn','sodayn','caffintake','caffdur','sualcodur','height_m','average_sysbp3','average_diabp3']], 
                                                            on=['studyid', 'redcap_event_name'], how='left')
        if site == 'umb':
            # append race columns to umb dataframe
            site_final_df = pd.merge(site_final_df, logic_cols_df[['studyid','redcap_event_name','age','pmhhtn_age_onset','birth_weight','rpmenopage','teayn','coffeeyn','smokever','sualcodur','sualcodrinks','tolvaptan_treat','height_m']], 
                                                            on=['studyid', 'redcap_event_name'], how='left')
        if site == 'uab':
            # append race columns to uab dataframe
            site_final_df = pd.merge(site_final_df, logic_cols_df[['studyid','redcap_event_name','age','pmhhtn_age_onset','tolvaptan_treat','creatinine','albumin','wbc_k']], on=['studyid', 'redcap_event_name'], how='left')
        
        # attach site name to studyid
        site_final_df['studyid'] = site_final_df['studyid'].apply(lambda x: site + '_' + str(x))

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