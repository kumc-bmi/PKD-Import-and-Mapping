## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

.PHONY: all clean

clean:
	rm -f .make.* || true

all: .make.venv .make.get_and_modify_data

.make.venv:
	# "creating python virtual env"
	which python3
	python3 -V
	pip3 freeze
	pip3 install -r requirements.txt  && \
	pip3 freeze >  requirements_pip_freeze.txt  && \
	which python3
	python3 -V
	pip3 freeze
	touch .make.venv

.make.get_and_modify_data: .make.venv
	python3 get_and_modify_data.py $(config_file) 30282
	touch .make.get_and_modify_data
