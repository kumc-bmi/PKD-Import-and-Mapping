## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

import configparser
import logging

log_details = logging.getLogger(__name__)

def main(os_path, openf, argv):
    def get_config():
        [config_fn, pid] = argv[1:3]

        config = configparser.SafeConfigParser()
        config_fp = openf(config_fn)
        config.readfp(config_fp, filename=config_fn)

        # read kumc and children national credentials from config file
        def pull_api_data(api_arr, token_arr):
            results = []
            for api_value, token_value in zip(api_arr, token_arr):
                api_url = config.get('api', api_value)
                verify_ssl = config.getboolean('api', 'verify_ssl')
                log_details.debug('API URL: %s', api_url)

                data_token = config.get(pid, token_value)
                # data_proj = Project(api_url, data_token, verify_ssl=verify_ssl)
                results.append(data_token)
            # return the data API for each RedCap sites
            return results

        api = ['kumc_redcap_api_url', 'chld_redcap_api_url']
        token = ['token_kumc', 'token_chld']

        data_projs = pull_api_data(api, token)
                
        def open_dest(file_name, file_format):
            file_dest = config.get(pid, 'file_dest')
            return openf(os_path.join(file_dest,
                                      file_name + '.' + file_format), 'wb')

        return pid, data_projs, open_dest
    return get_config  
        

if __name__ == "__main__":

    def _main_ocap():
        '''
        # https://www.madmode.com/2019/python-eng.html
        '''
        from sys import argv
        from os import path as os_path
        from __builtin__ import open as openf
        # from redcap import Project

        config_details = main(os_path, openf, argv)
        print(type(config_details))

    _main_ocap()
