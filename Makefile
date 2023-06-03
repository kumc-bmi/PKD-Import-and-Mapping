## Reference from the code in the repo redcapex(https://github.com/kumc-bmi/redcapex)

# .SHELLFLAGS := -x

.PHONY: all clean

clean:
	rm -f .make.* || true

all: .make.venv .make.get_and_modify_data

.make.venv:
	which python3
	python3 -V
	pip install --upgrade pip
	pip3 install pysftp
	pip3 install -r requirements.txt  && \
	which python3
	python3 -V
	touch .make.venv

.make.get_and_modify_data: .make.venv
	python get_and_modify_data.py $(config_file) 30282
	touch .make.get_and_modify_data
