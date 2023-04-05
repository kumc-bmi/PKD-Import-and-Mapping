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
	which python
	python -V
	pip freeze
	python -m virtualenv venv
	. ./venv/bin/activate && \
	pip install -r requirements.txt  && \
	pip freeze >  requirements_pip_freeze.txt  && \
	which python
	python -V
	pip freeze
	touch .make.venv
