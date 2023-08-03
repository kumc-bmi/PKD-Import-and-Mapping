"""
Authors:
- Mary Penne Mays
- Siddharth Satyakam
- Sravani Chandakaq
- Lav Patel
"""
from app import main_attributes, os_path, openf, argv, log_details, requests, logging

log_details = logging.getLogger(__name__)

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
        log_details.debug('API URL: %s', api_url)

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
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            # create connection
            with pysftp.Connection(kumc_sftp_host, username=kumc_sftp_username, password=kumc_sftp_pwd, port=sftp_port, cnopts=cnopts) as sftp:

                # remote directory
                sftp.cwd(sftp_remote_path)
                
                # Maryland source file name
                umb_file = folder + '.csv'

                # download file
                sftp.get(umb_file, export_directory + umb_file)
                print("umb csv sftp file download complete")

                # close session
                sftp.close()

def main_attributes(os_path, openf, argv):
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
