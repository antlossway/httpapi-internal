# Internal SMS HTTP API

This is a backend for white-label SMS platform using fastAPI.
Internal call from frontend.



Install system package
----------------------
apt install cpanminus
apt install libpq-dev

perl module DBI
perl -MCPAN -e 'install Bundle::DBI'
cpanm install DBD::Pg

Install database
-----------------
- postgresql
- redis-server

create systemd service file
--------------------------
/etc/systemd/system/httpapi.service


Setup FastAPI
====================
1. set up virtual environment
python3 -m venv venv
source venv/bin/activate

2. update pip
easy_install -U pip

3. install dependencies
pip install -r requirements.txt 

4. create .config, sample:
[redis]
host=localhost
port=6379

[postgresql]
host=localhost
port=5432
db=database name
user=database user
password=xxxxx

[provider_api_credential]
provider_name=api_key---api_secret


