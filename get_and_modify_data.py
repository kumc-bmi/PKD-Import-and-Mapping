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

        values = {}
        values['kumc_api'] = config.get('api', 'kumc_redcap_api_url')
        values['verify_ssl'] = config.getboolean('api', 'verify_ssl')
        values['file_dest'] = config.get(pid, 'file_dest')

        def open_dest(file_name, file_format):
            file_dest = config.get(pid, 'file_dest')
            return openf(os_path.join(file_dest, 
                                    file_name + '.' + file_format), 'wb')
        return values, open_dest
    return get_config
        

if __name__ == "__main__":

    def _main_ocap():
        from sys import argv
        from os import path as os_path
        from __builtin__ import open as openf

        config_values = main(os_path, openf, argv)
        print(config_values[0])
        print(config_values[1])
        print(config_values[2])
       

    _main_ocap()
