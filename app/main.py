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
    "http://localhost",
    "http://localhost:*",
    "https://localhost:*",
    "http://localhost:8080",
    "http://localhost:8000",
    "http://localhost:3000",
    "http://0.0.0.0:3000",
    "http://127.0.0.1:*",
    "http://127.0.0.1:3000",
    "https://127.0.0.1:*",
    # "frontend:3000",
    # "http://frontend:3000",
    # "https://frontend:3000",
]

app = FastAPI(title="Safe Blues Backend for frontend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    # allow_methods=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get('/v1/admins')
def get_all_admins_api():
    query = 'SELECT * FROM admin_accounts'
    with engine.connect() as connection:
        result = connection.execute(query)
        res = [row for row in result]
        return(res)


@app.get('/v1/admins/{id}')
def get_admin_by_id(id: int) -> Json:
    query = f'SELECT * FROM admin_accounts WHERE id=%(id)s'
    with engine.connect() as connection:
        result = connection.execute(query, {'id': id})
        # res = [row for row in result]
        return(result.fetchall())


class SignInPayload(BaseModel):
    email: EmailStr
    password: str


@app.post('/v1/signin')
def signin(payload: SignInPayload):
    query = 'SELECT id, password FROM participants WHERE email=%(email)s'
    with engine.connect() as connection:
        result = connection.execute(query, {'email': payload.email})
        user_id, user_password = result.fetchone()
        if bcrypt.checkpw(payload.password.encode(), user_password.encode('utf-8')):
            # happy path
            time = str(datetime.now())
            uuid = uuid4()
            # TODO add a way to clear all old tokens in here, or avoid making a
            # new one if one already exists - but update the created at time?
            create_new_uuid = "INSERT INTO login_tokens (user_id, uuid, time_created) VALUES (%(user_id)s, %(uuid)s, %(time)s)"
            connection.execute(create_new_uuid, {
                               'user_id': user_id, 'uuid': uuid, 'time': time})

            response = Response(content=str(
                {'passwords_match': True, 'uuid': str(uuid)}))
            response.set_cookie("Authorization", uuid,
                                httponly=True, samesite='lax', secure=False)
            return response
            # return({'passwords_match': True, 'uuid': uuid})
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
    print(req.cookies)
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
    if user and user.account_type == 'admin':
        print(f"ADMIN token validated: {user.email}")
        return(req)
    else:
        print(f'user: {user.email} trying to login as admin!')
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )


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
    participant_id: int


@app.post('/v2/participants')
def create_Participant2(participant: Participant2):
    if len(str(participant.participant_id)) != 10:
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


if __name__ == '__main__':
    uvicorn.run('main:app', host="0.0.0.0", port=PORT, reload=True, debug=True)
