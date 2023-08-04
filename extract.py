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

# set up connection to REDCap API Export
def redcap_export_api():
    vaiables = main_attributes(os_path, openf, argv)
    kumc_api_url = str(vaiables['kumc_redcap_api_url'])
    cri_api_url =  str(vaiables['chld_redcap_api_url'])
    token_kumc = str(vaiables['token_kumc'])
    token_chld = str(vaiables['token_chld'])
    export_directory = './export/temp/raw_data/'
    kumc_project_id = str(vaiables['kumc_project_id'])
    chld_project_id = str(vaiables['chld_project_id'])

    kumc_sftp_host = str(vaiables['kumc_sftp_host'])
    kumc_sftp_username = str(vaiables['kumc_sftp_username'])
    kumc_sftp_pwd = str(vaiables['kumc_sftp_pwd'])
    sftp_remote_path = str(vaiables['sftp_remote_path'])
    sftp_port = 22

    # folders for exported files
    folders = ['kumc', 'umb', 'uab']

    for folder in folders:
        # site csv file
        filename = export_directory + folder + '.csv'

        # assign token and project_id
        if folder == 'kumc':
            token = token_kumc
            project_id = kumc_project_id
            api_url = kumc_api_url
        elif folder == 'uab':
            token = token_chld
            project_id = chld_project_id
            api_url = cri_api_url
        
        # log details
        logging.getLogger(__name__).debug('API URL: %s', api_url)

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
            print(folder + ' data exported successfully') 
        else:
            # print error result for unsucessful export
            print('Error exporting ' + folder + ' data: ', response.text)
        
        if folder == 'umb':

            def recent_csv_file(dir):
                csv_files = glob.glob(os.path.join(dir, "*.csv"))
                csv_files = [file for file in csv_files if os.path.basename(file) != folder + '.csv']
                if csv_files:
                    return max(csv_files, key=os.path.getatime)
                return None

            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            # create connection
            with pysftp.Connection(kumc_sftp_host, username=kumc_sftp_username, password=kumc_sftp_pwd, port=sftp_port, cnopts=cnopts) as sftp:

                # remote directory
                sftp.cwd(sftp_remote_path)
                
                # Maryland source file name
                umb_file = folder + '.csv'

                file_path =  os.path.join(sftp_remote_path, umb_file)
                if not os.path.exists(file_path):
                    latest_file = recent_csv_file(sftp_remote_path)
                    if not latest_file:
                        print("umb.csv file does not exist")
                        return
                    
                    file_path = latest_file

                    date_time = datetime.now().strftime("%Y%m%d_%H%M%S")

                    file_backup_path = os.path.join(sftp_remote_path, folder + "_" + date_time + ".csv")
                    os.rename(file_path, file_backup_path)

                    umb_file_path = os.path.join(sftp_remote_path, folder + ".csv")
                    os.rename(file_backup_path, umb_file_path)
                    umb_file = folder + '.csv'
                
                # download file
                sftp.get(umb_file, export_directory + umb_file)
                print("umb csv sftp file download complete")

                # close session
                sftp.close()
