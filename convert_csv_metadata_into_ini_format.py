## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.5.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
from sys import argv
case_sensitive_param= {
    'exportdataaccessgroups': 'exportDataAccessGroups'
}

def convert_csv_metadata_into_ini_format(input_csv_path, output_ini_path):

    config_df = pd.read_csv(input_csv_path, dtype=str)
    config_dict_lst = config_df.to_dict('records')

    output = []
    for config in config_dict_lst:
        title = config['title']
        output.append(f'[{title}]')
        for key in config.keys():
            value = config[key]
            if key in case_sensitive_param.keys():
                key = case_sensitive_param[key]
            output.append(f'{key}:{value}')
        output.append('\n')
    output_string = "\n".join(output)

    with open(output_ini_path, 'w', encoding='utf-8') as f:
        f.write(output_string)


# -
if __name__ == "__main__":
    print(f'argv: {argv}')

    [input_csv_path, output_ini_path] = argv[1:]

    convert_csv_metadata_into_ini_format(input_csv_path, output_ini_path)
