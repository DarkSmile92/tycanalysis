#!/bin/sh
pip install pymssql sqlalchemy python-dotenv
touch ./.env
echo "DBUSER=
DBPASS=
DBURL=
DBPORT=
DBNAME=" > ./.env