## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

.SHELLFLAGS := -x

.PHONY: all clean

clean:
	rm -f .make.* || true

all: .make.venv .make.get_and_modify_data

.make.venv:
	# "creating python virtual env"
	. $(PYENV)/bin/activate
	which python
	python -V
	python -m pip install --upgrade pip
	pip freeze
	pip install -r requirements.txt  && \
	pip freeze >  requirements_pip_freeze.txt  && \
	which python
	python -V
	pip freeze
	touch .make.venv

.make.get_and_modify_data: .make.venv
	python get_and_modify_data.py $(config_file) 30282
	touch .make.get_and_modify_data
