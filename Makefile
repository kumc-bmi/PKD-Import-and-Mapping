run: clean venv
	. venv/bin/activate && \
	which python3 && \
	sftp_maryland_data
	# download which projects needs to export and its token && \
	python3 download_redcap_data.py .env/REDCap_Export_Metadata_config.ini 22394 'https://redcap.kumc.edu/api/' local && \
	# converted downloaded csv with token into ini && \
	python3 convert_csv_metadata_into_ini_format.py '.env/redcap_projects_exports.csv'  '.env/redcap_projects_exports.ini' && \
	# download all listed redcap projects && \
	python3 download_redcap_data.py .env/redcap_projects_exports.ini ALL 'https://redcap.kumc.edu/api/' ${where_to_save}

venv:
	# "creating python3 virtual env"
	python3 -m pip install --upgrade pip
	python3 -m pip install virtualenv
	python3 -m virtualenv venv
	. ./venv/bin/activate && \
	pip3 install --upgrade pip  && \
	pip3 install -r requirements.txt  && \
	pip3 install -r requirements_dev.txt  && \
	pip3 freeze >  requirements_pip_freeze.txt  && \
	which pip3 && which python3 && python3 --version

venv_clean:
	# "deleting python3 virtual env"
	rm -rf ./venv

sftp_maryland_data:
    # to do: retrieve the key using pps-client and place in workspace of jenkins job
	mkdir -p ./STUDY00146013
	sftp -i ./svc-mi-redcap-account/dr_yu_mapping_project_priv_key -r svc-mi-redcap@sftp.kumc.edu:STUDY00146013/* ./STUDY00146013
	
clean:	
	rm -rf ./
	rm -rf ./STUDY00146013
	venv_clean
	
install_python3_cifs:
	sudo yum install -y python3-pip cifs-utils
	python3 -m pip3 install --user --upgrade pip
	python3 -m pip3 install --user virtualenv

    
