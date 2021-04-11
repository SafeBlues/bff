import os
from functools import wraps
from fastapi import FastAPI, Path, HTTPException, status
from fastapi.openapi.models import ParameterBase
from fastapi.param_functions import Depends
from pydantic import BaseModel
from dotenv import load_dotenv
from pydantic.networks import EmailStr
from pydantic.types import Json
# for fixing cors issues/allowing differnet origins
from fastapi.middleware.cors import CORSMiddleware
import sqlalchemy
from starlette.datastructures import QueryParams

from starlette.responses import Response, JSONResponse
from starlette.requests import Request

from fastapi.encoders import jsonable_encoder
import uvicorn

from email_validator import validate_email, EmailNotValidError
from uuid import uuid4
from datetime import datetime
import bcrypt
import numpy as np
import logging
import requests
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

# load_dotenv()

db_hostname = os.environ['HOST']
db_port = int(os.environ['DB_PORT'])
db_user = os.environ['USER']
db_pass = os.environ['PASSWORD']
db_name = os.environ['DB_NAME']
PORT = int(os.environ['PORT'])
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

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost:*",
    "http://localhost:3000",
    "http://130.216.216.231:3000",
    "http://participant.safeblues.org:3000",
    "http://participant.safeblues.org",
    "https://participant.safeblues.org",
]

app = FastAPI(title="Safe Blues Backend for frontend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

class SignInPayload(BaseModel):
    email: EmailStr
    password: str

def clear_login_tokens(user_id, connection):
    # delete old tokens
    delete_old_tokens_query = """DELETE FROM login_tokens 
                                WHERE user_id = (%(user_id)s)"""
    connection.execute(delete_old_tokens_query, {'user_id': user_id})

def create_new_login_token(user_id, connection):
    # create new login token
    time = str(datetime.now())
    uuid = uuid4()
    create_new_uuid = "INSERT INTO login_tokens (user_id, uuid, time_created) VALUES (%(user_id)s, %(uuid)s, %(time)s)"
    connection.execute(create_new_uuid, {
                        'user_id': user_id, 'uuid': uuid, 'time': time})
    return(uuid)
    
@app.post('/v1/signin')
def signin(payload: SignInPayload):
    query = 'SELECT id, password FROM participants WHERE email=%(email)s'
    with engine.connect() as connection:
        result = connection.execute(query, {'email': payload.email})
        vals = result.fetchone()
        logging.info(f"fetched values: {vals}")
        if vals == None:
            return("Email does not exist")
        user_id, user_password = vals
        if bcrypt.checkpw(payload.password.encode(), user_password.encode('utf-8')):
            clear_login_tokens(user_id, connection)            
            uuid = create_new_login_token(user_id, connection)

            response = JSONResponse(content=
                {'passwords_match': True, 'uuid': str(uuid)})
            response.set_cookie("Authorization", uuid,
                                httponly=True, samesite='lax', secure=False)
            return response
        else:
            return({'passwords_match': False})


# def validate_login(req: Request):
#     def with_validation(*args, **kwargs):
#         print(req.cookies['Authorization'])
#         return(func(req))
#     return(with_validation)

def validate_token(req: Request):
    """
    A decorator that checks the UUID token of a user, and throws an error if the
    UUID is not valid.
    """
    uuid = req.cookies['Authorization']
    with engine.connect() as connection:
        query = """
                    SELECT * 
                    FROM participants
                    JOIN (login_tokens) ON (participants.id = login_tokens.user_id)
                    WHERE uuid=%(uuid)s
                """
        res = connection.execute(query, {'uuid': uuid})
    user = res.fetchone()
    if user:
        print(f"PARTICIPANT token validated: {user.email}")
        return(req)
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )


def validate_admin_token(req: Request):
    """
    A decorator that checks the UUID token of a user, and throws an error if the
    user is not an admin, or if the token is not valid.
    """
    try:
        uuid = req.cookies['Authorization']
    except KeyError as e:
        msg = "No authorization sent in cookie!"
        logging.info(msg)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No auth sent in request, or uuid is not valid",
        )
    with engine.connect() as connection:
        query = """
                    SELECT * 
                    FROM participants
                    JOIN (login_tokens) ON (participants.id = login_tokens.user_id)
                    WHERE uuid=%(uuid)s
                """
        res = connection.execute(query, {'uuid': uuid})
    user = res.fetchone()
    if user is None:
        logging.info(f"no user matches uuid {uuid}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No user matches the uuid",
        )
    if user and user.account_type == 'admin':
        # happy path:
        logging.info(f"ADMIN token validated: {user.email}")
        return
    else:
        logging.info(f'user: {user.email} trying to login as admin!')
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )
@app.get('/v1/validate-admin')
def validate_admin_login(req: Request = Depends(validate_admin_token)):
    return(True)


# @validate_login(Request)
@app.get('/v1/logged_in_test')
def logged_in_test(req: Request = Depends(validate_token)):
    # TODO change this into a decorator to wrap other endpoints in.
    uuid = req.cookies['Authorization']
    with engine.connect() as connection:
        query = """
                    SELECT * 
                    FROM participants
                    JOIN (login_tokens) ON (participants.id = login_tokens.user_id)
                    WHERE uuid=%(uuid)s
                """
        res = connection.execute(query, {'uuid': uuid})
    user = res.fetchone()
    return(f'logged in as {user.first_name} {user.last_name} email:{user.email}')


@app.get('/v1/uuid4')
def get_uuid():
    """
    Just generates a UUID4 for testing
    """
    return(uuid4())


@app.get('/v1/view_cookie')
def view_cookies(req: Request):
    """
    just a helper for testing
    """
    return(req.cookies)


class Participant(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    password: str



@app.get('/v1/participants')
def get_all_participants(req: Request = Depends(validate_admin_token)):
    query = 'SELECT * FROM participants'
    with engine.connect() as connection:
        result = connection.execute(query)
        return(result.fetchall())


@app.get('/v1/participants/{id}')
def get_participant_by_id(id: int) -> Json:
    query = f'SELECT * FROM participants WHERE id=%(id)s'
    with engine.connect() as connection:
        result = connection.execute(query, {'id': id})
        return(result.fetchall())


@app.post('/v1/participants')
def create_Participant(participant: Participant):

    encrypted_password = bcrypt.hashpw(
        participant.password.encode(), bcrypt.gensalt())

    query = "INSERT INTO participants (first_name, last_name, email, password) " \
            'VALUES (%(first_name)s, %(last_name)s, %(email)s, %(password)s);'
    with engine.connect() as connection:
        result = connection.execute(query, {"first_name": participant.first_name,
                                            "last_name": participant.last_name, "email": participant.email, "password": encrypted_password})
        return 'success'


def check_if_participant_id_exists(participant_id):
    with engine.connect() as connection:
        query = """SELECT COUNT(1)
                FROM participants
                WHERE participant_id = %(participant_id)s;"""
        result = connection.execute(
            query, {"participant_id": participant_id})
        participant_exists = bool(result.fetchone()["COUNT(1)"])
        return participant_exists


class Participant2(BaseModel):
    email: EmailStr
    participant_id: str


@app.post('/v2/participants')
def create_Participant2(participant: Participant2):
    if len(participant.participant_id) != 10:
        detail = [  # recreating fastAPI typing error for custom error
            {
                "loc": [
                    "body",
                    "participant_id"
                ],
                "msg": "participant_id is the wrong length",
                "type": "value_error.participant_id"
            }
        ]
        raise HTTPException(status_code=422, detail=detail)

    if not check_if_participant_id_exists(participant.participant_id):
        with engine.connect() as connection:
            query = "INSERT INTO participants (email, participant_id) " \
                    'VALUES (%(email)s, %(participant_id)s);'
            result = connection.execute(
                query, {"email": participant.email, "participant_id": participant.participant_id})
            # TODO check if the participant id already exists
            # TODO check for success
            # TODO set a uuid for the user at the same time
            return {"status": 200}
            # TODO validate that the participant id actually exists
            # TODO return a setcookie with a uuid for sign in
    else:
        detail = [  # recreating fastAPI typing error for custom error
            {
                "loc": [
                    "body",
                    "participant_id"
                ],
                "msg": "participant_id is already linked to an email",
                "type": "value_error.participant_id"
            }
        ]
        raise HTTPException(status_code=422, detail=detail)


class ExperimentData(BaseModel):
    participant_id: str
    statuses: list


@app.post('/push_experiment_data')
def push_experiment_data(data: ExperimentData):
    """
    this endpoint will take the data pushed from the aws app and the mobile apps
    and store it in the database/pms. 
    """
    time = str(datetime.now())
    with engine.connect() as connection:
        for status in data.statuses:
            query = "INSERT INTO experiment_data (participant_id, status_id, date, truncated_entry_time, duration, count_active) " \
                    'VALUES (%(participant_id)s, %(status_id)s, %(date)s, %(truncated_entry_time)s, %(duration)s, %(count_active)s);'
            result = connection.execute(
                query, {"participant_id": data.participant_id, "status_id": status["status_id"], "date": time, "truncated_entry_time": status["truncated_entry_time"], "duration": status["duration"], "count_active": status["count_active"]})
        return {"status": 200}


@app.get("/api/stats/{participant_id}")
def get_stats_for_participant(participant_id: str) -> dict:
    """
    returns the total number of hours that a participant has spent on campus
    """
    # TODO add a catch for when the participant_id does not exist
    # - consider making this a funcion all on its own?
    if not check_if_participant_id_exists(participant_id):
        payload = {"status": 400, "description": "participant_id does not exist"}
        return(payload)
    with engine.connect() as connection:
        query = """SELECT SUM(duration) as total_time_on_campus from experiment_data
                    where participant_id = %(participant_id)s
                    """
        result = connection.execute(query, {"participant_id": participant_id})
        duration_ms = result.fetchone()["total_time_on_campus"]
        hours_on_campus = round(duration_ms/3600000, 1)
        payload = {"participant_id": participant_id,
                   "total_hours_on_campus": hours_on_campus,
                   "status": 200}
        return payload

# TODO add caching to this function, wit daily ttl
@app.get("/api/stats")
def get_aggregate_statistics():
    """
    Should be consumed by the https://participant.safeblues.org/stats page only.

    this endpoint should return a list of every participants total number of
    hours on campus, but should not list any identifying information.
    should simple return {"total_hours_list": [12, 14, 1, 5 ... ]}.

    This data should be used for generating the plots for showing the
    distribution of students campus hours.
    TODO add caching to this function, so that it only gets generated once a day
    or so, so that we done have a heavy aggregate operation run everytime
    someone loads up their stats.
    """
    with engine.connect() as connection:
        query = """SELECT SUM(duration)
                    FROM experiment_data
                    GROUP BY participant_id;"""
        result = connection.execute(query)
        hours_on_campus_list = [float(round(duration_ms[0]/3600000, 1)) for duration_ms in result.fetchall()]
        logging.info(hours_on_campus_list)
        # payload = {"hours_on_campus_list": hours_on_campus_list}
        # hours_on_campus = [6, 31.8, 9.2, 4.6]
        hist, bin_edges = np.histogram(hours_on_campus_list, bins=15)
        # payload = {"hist": hist, "bin_edges": bin_edges}
        hist = [round(i, 2) for i in hist.tolist()]
        bin_edges = [round(i, 2) for i in bin_edges.tolist()]
        payload = {"hist": hist, "bin_edges": bin_edges}
        return payload
        # return {"hist": hours_on_campus_list}


def scientific_round(num: int, keep: int)-> int:
    """
    takes a number, and returns it trimmed to the level of precision specified
    by `keep`. 0 keeps only the leading, 1 keeps the leading + 1 digit.
    eg: scientific_round(123,1) -> 120 
    """
    denom = 10**(len(str(num))-1)
    result = round(num/denom, keep)*denom
    return int(result)

@app.get("/api/num_participants")
def get_rough_num_participants() -> dict:
    """
    gives us a some-what privacy preserving way of displaying the number of 
    participants in the safe blues experiment

    returns a dict representing 'roughly' the number of participants.
    """
    with engine.connect() as connection:
        query = """SELECT COUNT(DISTINCT participant_id)
                    FROM participants
                    """
        result = connection.execute(query)
        num_participants = result.fetchone()[0]
        logging.info(f"current number of participants: {num_participants}")
        if num_participants < 10:
            return {"num_participants": "<10"}
        if num_participants < 50:
            return {"num_participants": "<50"}
        if num_participants < 100:
            return {"num_participants": "<100"}
        num_participants = scientific_round(num_participants, 1)
        return {"num_participants": f"about {num_participants}"}

@app.get("/strands")
def get_strands(req: Request = Depends(validate_admin_token)):
    response = requests.get("https://api.safeblues.org/admin/list")
    return(response.json())


if __name__ == '__main__':
    uvicorn.run('main:app', host="0.0.0.0", port=PORT, reload=True, debug=True)
