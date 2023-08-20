<a name="readme-top"></a>
## 📽️ PKD MULTI-SITE REGISTRY

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white) ![Makefile](https://img.shields.io/badge/GNU%20Bash-4EAA25?style=for-the-badge&logo=GNU%20Bash&logoColor=white)

<img src="https://projectredcap.org/wp-content/themes/rcap/images/answerhub.png" />

## 🚩 Table of Contents

- [Authors](#authors)
- [Site](#site)
- [Why PKD MULTI-SITE REGISTRY?](#why-pkd-multi-site-registry)
- [Setup](#setup)

## <img src="images/authors-badge-small.png" width="180" height="37"/>

Thank you to all our authors! ✨
<table>
  <tr>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/72703458?v=4" width="100px;" /><br /><sub><b>Mary Penne Mays</b></sub></td>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/43289998?v=4" width="100px;" /><br /><sub><b>Siddharth Satyakam</b></sub></td>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/8313457?v=4" width="100px;" /><br /><sub><b>Sravani Chandaka</b></sub></td>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/8277466?v=4" width="100px;" /><br /><sub><b>Lav Patel</b></sub></td>
    <td align="center"><sub><b>Alan Yu(MD, PhD)</b></sub></td>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/4640305?v=4" width="100px;" /><br /><sub><b>Kelechi Anuforo</b></sub></td>
  </tr>
</table>



## ⛬ Site

| Name | Description |
| --- | --- |
| Organization | University of Kansas Medical Center |
| Project | https://redcap.kumc.edu/redcap_v13.1.27/index.php?pid=30282 |
| Contact | https://www.kumc.edu/research-informatics.html |
| Copyright | Copyright (c) 2023 Univeristy of Kansas Medical Center |

## 🧢 Why PKD MULTI-SITE REGISTRY?

This project imports data from Maryland University and children's national; 
makes changes as per requirements and then maps the data to KUMC redcap PKD project

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## ⚙ Setup
<!-- GETTING STARTED -->
## Getting Started

Instructions on setting up the PKD project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

There are several ways to install Python, that is if it needs to be installed at all!

### Check what’s installed first

* version
  ```sh
  python --version
  ```

Based on your OS (Operating System), you need to download python software. The example below is on a Linux/unix system through command line.

### Installation

* python
  ```sh
  apt install python3 python-is-python3
  ```

1. Clone the PKD Registry repo
   ```sh
   git clone https://github.com/kumc-bmi/PKD-Import-and-Mapping.git
   ```
2. Create virtual environment
   ```sh
   python3 -m venv PKD-Import-and-Mapping/
   ```
3. Start up virtual enviroment
  ```sh
   cd PKD-Import-and-Mapping/
   ```
   ```sh
   source bin/activate
   ```
4. Install all required python packages
  ```sh
   pip install -r requirements.txt
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Download Instructions

Sites API, TOKEN, PROJECT_ID credentials are located in `env_attrs.py` under the api and token sections. API access is only permitted for University of Kansas Medical Center and Children National. Maryland data is uploaded by the Maryland team through sftp which is then accessed by provided sftp credentials.

<!-- ENVIROMENT VARIABLES -->
### Main Variables
  ```py
  values['kumc_redcap_api_url'] = 'KUMC REDCAP API URL';
  values['chld_redcap_api_url'] = 'CHILDREN NATIONAL REDCAP API URL';
  ```

- [x] `token_kumc`: KUMC redcap project API token
- [x] `token_chld`: Children National redcap project API token
- [x] `proj_token`: Unified redcap project API token
- [x] `kumc_project_id`: KUMC redcap project identification number
- [x] `chld_project_id`: Children National redcap project identification number
- [x] `project_id`: Unified redcap project identification number
- [x] `kumc_sftp_host`: Secure file transfer protocol host URL for Maryland data
    - [x] `kumc_sftp_username`: SFTP username
    - [x] `kumc_sftp_pwd`: SFTP password
    - [x] `sftp_remote_path`: SFTP file path for stored data

`extract.py` file codebase extracts/downloads all project data in csv format using either API or SFTP pull from all three sites.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Mapping Instructions

### Import Instructions
