#!/bin/sh
pip install pymssql sqlalchemy python-dotenv pandas tablulate
touch ./.env
echo "DBUSER=
DBPASS=
DBURL=
DBPORT=
DBNAME=" > ./.env