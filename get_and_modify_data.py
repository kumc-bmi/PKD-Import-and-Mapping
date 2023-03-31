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
        #print(entry.name)
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
def redcap_call(redcap_api_url, dataconfig, os, logging, post):
    try:
        # Referrenced from code in 
        # https://github.com/kumc-bmi/redcapex/blob/29b49d21836a7337beebc1a8b8c7de3dbc541691/download_redcap_data.py#L10
        log_error_str = """
            redcap rest call was unsuccessful
            or target server is down/ check configuration
            %s
            """ % (dataconfig)
        response = post(redcap_api_url, data=dataconfig)
        print(post(redcap_api_url, data=dataconfig))
        if response.status_code == 200:
            return response.content
        else:
            logging.error(response.content)

    except Exception as e:
        logging.error('log_error_str : %s' % (e))

def mapping_kumc(kumc_int_df, logging, os, post):
    import pandas
    redcap_api_url =  os.getenv('kumc_redcap_api_url')
    cols = kumc_int_df.columns
    
    kumc_final_df = kumc_int_df[['Study ID', 'Event Name','Date of visit', 'Date of Birth', 'Sex', "If 'Other', please specify", 'What is the ethnicity of the subject?', 'What is the race of the subject? (choice=Black or African American)', 'What is the race of the subject? (choice=American Indian or Alaska Native)', 'What is the race of the subject? (choice=Asian)', 'What is the race of the subject? (choice=Native Hawaiian or Other Pacific Islander)', 'What is the race of the subject? (choice=White)', 'What is the race of the subject? (choice=Other)']].copy()
    
    # Mapping and transformation of the study Id/record_id(i.e. the primary key):
    kumc_final_df['studyid'] = 'kumc_' + kumc_final_df['Study ID'].astype(str)
    studyid_first_column = kumc_final_df.pop('studyid')
    kumc_final_df.insert(0, 'studyid', studyid_first_column)
    kumc_final_df = kumc_final_df.drop(columns=['Study ID'])
    
    # Mapping and transformation of the visit date:
    kumc_final_df = kumc_final_df.rename(columns={"Date of visit": "visdat"})
    kumc_final_df['visdat'] = pandas.to_datetime(kumc_final_df['visdat'])

    # Mapping of the Sex Field:
    kumc_final_df = kumc_final_df.rename(columns={"Sex": "sex"})
    
    # mapping of the age field:
    kumc_final_df = kumc_final_df.rename(columns={"Age in years at this visit": "age"})
    kumc_final_df['age'] = (kumc_final_df['visdat'] - kumc_int_df['Date of Birth'].astype('datetime64[ns]')).dt.days/365
    
    # Mapping of the race fields: (conversion of the race fields to 1 where the checkbox fields are
    # not null)
    ids = kumc_final_df['studyid'].tolist()
    
    kumc_final_df['What is the race of the subject? (choice=Black or African American)'] = kumc_final_df['What is the race of the subject? (choice=Black or African American)'].fillna('0')
    kumc_final_df['What is the race of the subject? (choice=White)'] = kumc_final_df['What is the race of the subject? (choice=White)'].fillna('0')
    kumc_final_df['What is the race of the subject? (choice=Asian)'] = kumc_final_df['What is the race of the subject? (choice=Asian)'].fillna('0')
    kumc_final_df['What is the race of the subject? (choice=American Indian or Alaska Native)'] = kumc_final_df['What is the race of the subject? (choice=American Indian or Alaska Native)'].fillna('0')
    
    race_col_list = ['What is the race of the subject? (choice=Black or African American)', 'What is the race of the subject? (choice=White)','What is the race of the subject? (choice=Asian)', 'What is the race of the subject? (choice=American Indian or Alaska Native)']    
    for col in race_col_list:
        alter_race_col_kumc = kumc_final_df.pop(col)
        for i in range(0, len(alter_race_col_kumc)):
            if alter_race_col_kumc[i] != '0':
                alter_race_col_kumc[i] = '1'
        
        kumc_final_df.insert(4, col, alter_race_col_kumc)
    
    kumc_final_df['What is the race of the subject? (choice=Black or African American)'] = kumc_final_df['What is the race of the subject? (choice=Black or African American)'].astype('int64')
    kumc_final_df['What is the race of the subject? (choice=White)'] = kumc_final_df['What is the race of the subject? (choice=White)'].astype('int64')
    kumc_final_df['What is the race of the subject? (choice=Asian)'] = kumc_final_df['What is the race of the subject? (choice=Asian)'].astype('int64')
    kumc_final_df['What is the race of the subject? (choice=American Indian or Alaska Native)'] = kumc_final_df['What is the race of the subject? (choice=American Indian or Alaska Native)'].astype('int64')
    
    # Mapping of other_race:
    kumc_final_df = kumc_final_df.rename(columns={"If 'Other', please specify": 'other_race'})
    
    # Mapping of the ethinicity:
    kumc_final_df = kumc_final_df.rename(columns={'What is the ethnicity of the subject?': 'ethnic'})
    # mapping and transf. of adpkd_status to adpkd_yn:
    
    kumc_final_df = kumc_final_df.drop(columns=['Date of Birth'])    
    kumc_final_df.to_csv('./Test_csvs/final_kumc.csv',encoding='utf-8', index='false')
    
    return kumc_final_df
    
#def mapping_UAB(df_chld,mapping_csv,logging,os):

def mapping_UMB(maryland_int_df,logging,os,post):
    import pandas
    maryland_final_df = maryland_int_df[['Participant ID:', 'Event Name', 'Visit Date','6. Gender','7. Race', '7a. Specify other race','8. Ethnicity', '9a. National health insurance or government coverage', '10. What is the highest level of education the subject has completed? (Select best answer)', '9b. Employer provided private health insurance']].copy()
    
    # Mapping of the studyids:
    maryland_final_df = maryland_final_df.rename(columns={"Participant ID:": "studyid"})
    maryland_final_df['studyid'] = 'umb_' + maryland_final_df['studyid'].astype(str)
    
    # Mapping of visit date:
    maryland_final_df = maryland_final_df.rename(columns={"Visit Date": "visdat"})
    maryland_final_df['visdat'] = pandas.to_datetime(maryland_final_df['visdat'])
    
    # mapping of the age:
    maryland_final_df['age'] = (maryland_final_df['visdat'] - maryland_int_df['4. Date of Birth'].astype('datetime64[ns]')).dt.days/365
    
    # Mapping of the gender field:
    maryland_final_df = maryland_final_df.rename(columns={"6. Gender": "sex"})
    
    # Mapping of the race field:
    ids = maryland_final_df['studyid'].values
    
    maryland_final_df['7. Race'] = maryland_final_df['7. Race'].fillna(' ')

    for id in ids:
        if maryland_final_df.loc[maryland_final_df['studyid'] == id,'7. Race'].values.all() == 'Black or African American':
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=Black or African American)'] = str(1)
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=White)'] = 0
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=Asian)'] = 0
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=American Indian or Alaska Native)'] = 0
        elif maryland_final_df.loc[maryland_final_df['studyid'] == id,'7. Race'].values.all() == 'White':
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=White)'] = str(1)
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=Black or African American)'] = 0
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=Asian)'] = 0
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=American Indian or Alaska Native)'] = 0
        elif maryland_final_df.loc[maryland_final_df['studyid'] == id,'7. Race'].values.all() == 'Asian':
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=Asian)'] = str(1)
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=White)'] = 0
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=Black or African American)'] = 0
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=American Indian or Alaska Native)'] = 0     
        elif maryland_final_df.loc[maryland_final_df['studyid'] == id,'7. Race'].values.all() == 'American Indian or Alaskan Native':
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=American Indian or Alaska Native)'] = 1
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=Asian)'] = 0
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=White)'] = 0
            maryland_final_df.loc[maryland_final_df['studyid'] == id,'What is the race of the subject? (choice=Black or African American)'] = 0            

    # Remove the 7. Race field used in above ode:
    maryland_final_df = maryland_final_df.drop(columns = ['7. Race'])
    
    # Mapping of other race field:
    maryland_final_df = maryland_final_df.rename(columns={"7a. Specify other race":"other_race"})
    
    # Mapping of the ethinic field:
    maryland_final_df = maryland_final_df.rename(columns={"8. Ethnicity":"ethnic"})
    maryland_final_df.loc[maryland_final_df['ethnic']=="Unknown",'ethnic'] = "NA"
    
    # Mapping of the gov or national health:
    maryland_final_df = maryland_final_df.rename(columns={"9a. National health insurance or government coverage":"govinsur"})
    
    # Mapping of the employer provided health
    maryland_final_df = maryland_final_df.rename(columns={"9b. Employer provided private health insurance":"employerins"})
    
    # Mapping of self insured:
    maryland_final_df = maryland_final_df.rename(columns={"9c. Self-insured (private health insurance)":"selfinsur"})
    
    # Mapping of the no insurance:
    maryland_final_df = maryland_final_df.rename(columns={"9d. No health insurance":"noinsur"})
    
    # mapping of the highest education:
    maryland_final_df = maryland_final_df.rename(columns={"10. What is the highest level of education the subject has completed? (Select best answer)":"educlevel"})
    maryland_final_df.loc[maryland_final_df['educlevel']=="Unknown",'educlevel'] = "NA"
    
    print(maryland_final_df)
    maryland_final_df.to_csv('./Test_csvs/final_umb.csv',encoding='utf-8')
    
    return maryland_final_df
    
def load_final_data(transformed_df_kumc, transformed_df_umb):
    import pandas
    
    # Concatination of all sites data:
    final_df_load = pandas.concat([transformed_df_kumc, transformed_df_umb],     # Append two pandas DataFrames
                      ignore_index = True,
                      sort = False)
    print(final_df_load.columns) 
    final_df_load.to_csv('./Test_csvs/final_demo_load.csv', encoding='utf-8', index=False)
    
    # Creation of the multi choice field data dict:
    upload_data_dict = pandas.read_csv('./Mapping_csvs/PKDMultisiteRegistry_DataDictionary.csv', encoding='utf-8', header=0)
    data_dict_cols = upload_data_dict.columns
    
    cols_of_data_dict = []
    for cols in data_dict_cols:
        if str.lower(cols).find('label') and str.lower(cols).find('field'):
            cols_of_data_dict.append(cols)
        elif str.lower(cols).find('choices') and str.lower(cols).find('calculations'):
            cols_of_data_dict.append(cols)
            
    final_field_choice_allowed = upload_data_dict[cols_of_data_dict].copy()
    final_field_choice_allowed = final_field_choice_allowed.dropna()
    
    # conversion of multichoice fields to numerical values for REDCap import:
    cols_in_fin_df = final_df_load.columns
    cols_in_multicoice = final_field_choice_allowed['Variable / Field Name'].tolist()
    ids = final_df_load['studyid'].tolist()
    tot_choices_options = dict()
    choice_fields = []
    
    for col in cols_in_fin_df:
        if col in cols_in_multicoice:
            tot_choices_options[" _"+col] = '0'
            choice_fields.append(col)
            tot_choices = (final_field_choice_allowed.loc[final_field_choice_allowed['Variable / Field Name']==col, 'Choices, Calculations, OR Slider Labels'].values)[0]
            tot_choices_or_splits = str(tot_choices).split('|')
            for items in tot_choices_or_splits:
                temp_choice_holder = items.split(',')
                tot_choices_options[str(temp_choice_holder[1]).strip()+'_'+col] = str(temp_choice_holder[0]).strip() 
    
    print(tot_choices_options)           
    
    final_df_load = final_df_load.fillna(' ')
    
    to_be_altered_col = []
    for cols in choice_fields:
        to_be_altered_col = final_df_load[cols]
        for i in range(0,len(to_be_altered_col)):
            to_be_altered_col[i] = tot_choices_options[to_be_altered_col[i]+"_"+cols]
        
        final_df_load.pop(cols)
        final_df_load.insert(4, cols, to_be_altered_col)
    
    final_df_load.to_csv('./Test_csvs/final_demo_import_ready.csv', encoding='utf-8', index=False)

def redcap_api_cred_creation(token, prefix_for_pull, os):
    dataconfig = dict()
    dataconfig["token"] = token
    for key, value in os.environ.items():
        if key.startswith(prefix_for_pull):
            key_entry = key.split(prefix_for_pull)[1]
            dataconfig[key_entry] = value

    return dataconfig
            
def main(umd_base_data_dir, logging, post, scandir, os, StringIO,date,datetime):
    error_list = []
    
    # read children national config file
    data_token_chld = os.getenv('token_chld')
    chld_redcap_api_url = os.getenv('chld_redcap_api_url')
    chld_data_import_data_dict = dict(redcap_api_cred_creation(data_token_chld,'API_data_',os))

    # read kumc config file
    data_token_kumc = os.getenv('token_kumc')
    kumc_redcap_api_url = os.getenv('kumc_redcap_api_url')
    kumc_data_import_data_dict = dict(redcap_api_cred_creation(data_token_kumc,'API_data_',os))
    
    print(kumc_data_import_data_dict)
    
    # redcap calls-KUMC
    df_kumc = redcap_call(kumc_redcap_api_url, kumc_data_import_data_dict, os, logging, post)
    temp_df_kumc = str(df_kumc,'utf-8')
    data_kumc = StringIO(temp_df_kumc)
    kumc_int_df = pd.read_csv(data_kumc, header=0, encoding='utf-8')
    print(kumc_int_df.columns)

    # redcap calls-CHLD
    df_chld = redcap_call(chld_redcap_api_url, chld_data_import_data_dict, os, logging, post)
    temp_df_chld = str(df_chld,'utf-8')
    data_chld = StringIO(temp_df_chld)
    chld_int_df = pd.read_csv(data_chld, header=0, encoding='utf-8')
    #print(data_df_chld)
    
    # redcap calls-MARYLAND
    maryland_int_df = get_maryland_data_from_sftp(umd_base_data_dir, scandir, logging,date,datetime)
    print(maryland_int_df)
    
    # Mapping data:
    transformed_df_kumc = mapping_kumc(kumc_int_df, logging, os,post)
    transformed_df_umb = mapping_UMB(maryland_int_df, logging, os, post)
    
    # Data loading into PKD Multi-site Registry(https://redcap.kumc.edu/redcap_v13.1.14/index.php?pid=30282)
    load_final_data(transformed_df_kumc, transformed_df_umb)
    
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
