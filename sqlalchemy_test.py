
import os
from fastapi import FastAPI, Path
from fastapi.openapi.models import ParameterBase
from fastapi.param_functions import Depends
from pydantic import BaseModel
# import mysql.connector
from dotenv import load_dotenv
from pydantic.types import Json
from fastapi.middleware.cors import CORSMiddleware # for fixing cors issues/allowing differnet origins
import sqlalchemy

load_dotenv()

db_hostname=os.environ['HOST']
db_port=int(os.environ['PORT'])
db_user=os.environ['USER']
db_pass=os.environ['PASSWORD']
db_name = "accounts"

engine = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL(
        drivername="mysql+pymysql",
        username=db_user,  # e.g. "my-database-user"
        password=db_pass,  # e.g. "my-database-password"
        host=db_hostname,  # e.g. "127.0.0.1"
        port=db_port,  # e.g. 3306
        database=db_name,  # e.g. "my-database-name"
    )
)

with engine.connect() as connection:
    result = connection.execute(str("select * from participants"))
    for row in result:
        print("email:", row['email'])