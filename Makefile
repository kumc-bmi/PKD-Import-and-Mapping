## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

# .SHELLFLAGS := -x

.PHONY: all clean

clean:
	rm -f .make.* || true

all: .make.venv .make.env_attrs .make.app .make.extract .make.transform .make.load

.make.venv:
	which python3
	python3 -V
	pip install --upgrade pip
	pip3 install -r requirements.txt  && \
	pip3 freeze >  requirements_pip_freeze.txt  && \
	which python3
	python3 -V
	pip3 freeze
	touch .make.venv

.make.env_attrs: .make.venv
	python env_attrs.py $(config_file) 30282
	touch .make.env_attrs

.make.app: .make.venv .make.env_attrs
	python app.py $(config_file) 30282
	touch .make.app

.make.extract: .make.venv .make.app
	python extract.py $(config_file) 30282
	touch .make.extract

.make.transform: .make.venv .make.extract
	python transform.py $(config_file) 30282
	touch .make.transform

.make.load: .make.venv .make.transform
	python load.py $(config_file) 30282
	touch .make.load
