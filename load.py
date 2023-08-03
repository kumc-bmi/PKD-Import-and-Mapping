"""
Authors:
- Mary Penne Mays
- Siddharth Satyakam
- Sravani Chandakaq
- Lav Patel
"""
from app import *

# set up connection to REDCap API Import
def redcap_import_api():
    vaiables = main_attributes(os_path, openf, argv)
    api_url = str(vaiables['kumc_redcap_api_url'])
    api_import_token = str(vaiables['import_token'])
    logging.getLogger(__name__).debug('API URL: %s', api_url)
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
