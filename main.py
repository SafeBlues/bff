import os
from fastapi import FastAPI, Path
from fastapi.openapi.models import ParameterBase
from fastapi.param_functions import Depends
from pydantic import BaseModel
import mysql.connector
from dotenv import load_dotenv
from pydantic.types import Json
from fastapi.middleware.cors import CORSMiddleware # for fixing cors issues/allowing differnet origins

app = FastAPI()
origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
]
app.add_middleware(
    CORSMiddleware,
    # allow_origins=origins,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
load_dotenv('.env') 

def make_query(database, query):
    # Connect to server
    cnx = mysql.connector.connect(
        host=os.environ.get('HOST'),
        port=os.environ.get('PORT'),
        user=os.environ.get('USER'),
        password=os.environ.get('PASSWORD'))

    # Get a cursor
    cur = cnx.cursor()
    
    # make a query
    cur.execute(f"USE {database}")
    cur.execute(query)
    res = cur.fetchall()

    # clean up
    cnx.close()
    return(res)


@app.get('/admins')
def get_all_admins():
    res = make_query(query='SELECT * FROM admin_accounts', database='accounts')
    return(res)

@app.get('/admins/{id}')
def get_admin_by_id(id: int) -> Json:
    res = make_query(query=f'SELECT * FROM admin_accounts WHERE id={id}', database='accounts')
    return(res)

class Participant(BaseModel):
    first_name: str
    last_name: str
    email: str
    password: str

@app.get('/participants')
def get_all_participants():
    res = make_query(query='SELECT * FROM participants', database='accounts')
    return(res)

@app.get('/participants/{id}')
def get_participant_by_id(id: int) -> Json:
    res = make_query(query=f'SELECT * FROM participants WHERE id={id}', database='accounts')
    return(res)

@app.post('/Participants')
def create_Participant(participant: Participant):
    # query = "INSERT INTO participants (first_name, last_name, email, password) " \
    #         f'VALUES ("{participant.first_name}", "{participant.last_name}", "{participant.email}", "{participant.password}");'
    # print(f"{query=}")
    
     # Connect to server
    cnx = mysql.connector.connect(
        host=os.environ.get('HOST'),
        port=os.environ.get('PORT'),
        user=os.environ.get('USER'),
        password=os.environ.get('PASSWORD'))

    # Get a cursor
    cur = cnx.cursor()
    
    # make a query
    insert_stmt = (
                    "INSERT INTO participants (first_name, last_name, email, user_password) "
                    "VALUES (%s, %s, %s, %s)"
                    )
    data = ('josh', 'mcdonald', 'joshua@curiousthing.io', 'chickens')
    database = 'accounts'
    print(insert_stmt)
    cur.execute("USE accounts")
    query = "INSERT INTO participants (first_name, last_name, email, password) " \
        f'VALUES ("{participant.first_name}", "{participant.last_name}", "{participant.email}", "{participant.password}");'
    print(query)

    try:
        cur.execute(insert_stmt, data)
        cnx.close()
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))

    # clean up
    cnx.close()
    return("account created")



# @app.post("/{name}")
# def read_root(name: str = Path(..., example="some example")) -> str:
#     """
#     Some descriptive doc-string
#     """
#     return f"Hello {name}"


# @app.post("/hello/{name}")
# def read_hello(name: str = ParameterBase(description= "test desc", example="test example")) -> str:
#     """
#     Some descriptive doc-string
#     """
#     return f"Hello {name}"