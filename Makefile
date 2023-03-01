## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

run: clean venv_clean venv maryland-dat
	. venv/bin/activate && \
	which python3 && \
	# download which projects needs to export and its token && \
	python3 get_and_modify_data.py ./STUDY00146013 && \
	backup_maryland_dat

venv:
	python3 -m pip install --upgrade pip
	python3 -m pip install virtualenv
	python3 -m virtualenv venv
	. ./venv/bin/activate && \
	pip3 install --upgrade pip  && \
	pip3 install -r requirements.txt  && \
	pip3 install -r requirements_dev.txt  && \
	pip3 freeze >  requirements_pip_freeze.txt  && \
	which pip3 && which python3 && python3 --version

maryland-dat:
	mkdir -p ./STUDY00146013 && \
	rm -rf ./children_national_alabama_data && \
	sftp -i ../svc-mi-redcap-account/dr_yu_mapping_project_priv_key -r svc-mi-redcap@sftp.kumc.edu:./STUDY00146013/* ./STUDY00146013

backup_maryland_dat:
	sftp -i ../svc-mi-redcap-account/dr_yu_mapping_project_priv_key svc-mi-redcap@sftp.kumc.edu:./My\ Folder <<< 'put ./STUDY00146013/*'

clean:
	rm -rf ./STUDY00146013  && \
	rm -rf ./children_national_alabama_data

venv_clean:
	# "deleting python3 virtual env"
	rm -rf ./venv
	
install_python3_cifs:
	sudo yum install -y python3-pip cifs-utils && \
	python3 -m pip3 install --user --upgrade pip && \
	python3 -m pip3 install --user virtualenv    
