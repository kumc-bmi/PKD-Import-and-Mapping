## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

# .SHELLFLAGS := -x

.PHONY: all clean

# Target to cleanup or remove make runs
clean:
	rm -f .make.* || true

# Target to run all specified targets for projects
all: .make.venv .make.env_attrs .make.app .make.extract .make.transform .make.load

# Target to run setup
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

# Target to run env_attrs.py (loads environment and related projject variables)
.make.env_attrs: .make.venv
	python env_attrs.py $(config_file) 30282
	touch .make.env_attrs

# Target to run app.py
.make.app: .make.venv .make.env_attrs
	python app.py $(config_file) 30282
	touch .make.app

# Target to run extract.py
.make.extract: .make.venv .make.app
	python extract.py $(config_file) 30282
	touch .make.extract

# Target to run transform.py
.make.transform: .make.venv .make.extract
	python transform.py $(config_file) 30282
	touch .make.transform

# Target to run load.py
.make.load: .make.venv .make.transform
	python load.py $(config_file) 30282
	touch .make.load
