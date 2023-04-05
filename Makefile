## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

.PHONY: all clean

clean:
	rm -f .make.* || true
	rm -rf ./
	# "deleting python3 virtual env"
	rm -rf ./venv

all: .make.venv

.make.venv:
	# "creating python3 virtual env"
	python3 -m pip install --upgrade pip
	python3 -m pip install virtualenv
	python3 -m virtualenv venv
	. ./venv/bin/activate && \
	pip3 install --upgrade pip  && \
	pip3 install -r requirements.txt  && \
	pip3 freeze >  requirements_pip_freeze.txt  && \
	which pip3 && which python3 && python3 --version
	touch .make.venv
