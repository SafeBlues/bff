import logging
import os
import random
from datetime import datetime
from typing import Optional

import numpy as np
import sqlalchemy
import uvicorn
from fastapi import FastAPI, HTTPException, Path, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pydantic.networks import EmailStr
from scipy.stats import gamma, gaussian_kde

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)

# which phase to cumulate hours into
# 0 for off
# 1,2,3 for 1st,2nd,3rd phases
CURRENT_READ_PHASE = 2
CURRENT_READ_DISPLAY_HOURS = "display_hours_" + str(CURRENT_READ_PHASE)
CURRENT_READ_EXTRA_HOURS = "extra_hours_" + str(CURRENT_READ_PHASE)
CURRENT_WRITE_PHASE = 2
CURRENT_WRITE_DISPLAY_HOURS = "display_hours_" + str(CURRENT_WRITE_PHASE)
CURRENT_WRITE_EXTRA_HOURS = "extra_hours_" + str(CURRENT_WRITE_PHASE)

db_hostname = os.environ["HOST"]
db_port = int(os.environ["DB_PORT"])
db_user = os.environ["USER"]
db_pass = os.environ["PASSWORD"]
db_name = os.environ["DB_NAME"]
PORT = int(os.environ["PORT"])
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

origins = ["https://participant.safeblues.org", "http://localhost:3000"]

app = FastAPI(title="Safe Blues Backend for frontend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def check_if_participant_id_exists(participant_id):
    with engine.connect() as connection:
        query = """SELECT COUNT(1)
                FROM participants
                WHERE participant_id = %(participant_id)s;"""
        result = connection.execute(query, {"participant_id": participant_id})
        participant_exists = bool(result.fetchone()["COUNT(1)"])
        return participant_exists


def check_if_referral_code_exists(referral_code):
    with engine.connect() as connection:
        query = """SELECT COUNT(1)
                FROM participants
                WHERE referral_code = %(referral_code)s;"""
        result = connection.execute(query, {"referral_code": referral_code})
        code_exists = bool(result.fetchone()["COUNT(1)"])
        return code_exists


def generate_new_referral_code():
    while True:
        referral_code = str(random.randint(0, 10**6-1)).zfill(6)
        if not check_if_referral_code_exists(referral_code):
            break
    return referral_code


class Participant2(BaseModel):
    email: EmailStr
    participant_id: str
    referrer: str


@app.post("/v3/participants")
def create_Participant2(participant: Participant2):
    if len(participant.participant_id) != 10:
        detail = [  # recreating fastAPI typing error for custom error
            {
                "loc": ["body", "participant_id"],
                "msg": "participant_id is the wrong length",
                "type": "value_error.participant_id",
            }
        ]
        raise HTTPException(status_code=422, detail=detail)

    if not check_if_participant_id_exists(participant.participant_id):
        referral_code = generate_new_referral_code()

        with engine.connect() as connection:
            query = "INSERT INTO participants (email, participant_id, referral_code, referrer) " "VALUES (%(email)s, %(participant_id)s, %(referral_code)s, %(referrer)s);"
            result = connection.execute(
                query, {"email": participant.email, "participant_id": participant.participant_id, "referral_code": referral_code, "referrer": participant.referrer}
            )
            # TODO check if the participant id already exists
            # TODO check for success
            # TODO set a uuid for the user at the same time
            return {"status": 200}
            # TODO validate that the participant id actually exists
            # TODO return a setcookie with a uuid for sign in
    else:
        detail = [  # recreating fastAPI typing error for custom error
            {
                "loc": ["body", "participant_id"],
                "msg": "participant_id is already linked to an email",
                "type": "value_error.participant_id",
            }
        ]
        raise HTTPException(status_code=422, detail=detail)


class ExperimentData(BaseModel):
    participant_id: str
    version_code: Optional[int] = None
    statuses: list


@app.post("/push_experiment_data")
def push_experiment_data(data: ExperimentData):
    """
    this endpoint will take the data pushed from the aws app and the mobile apps
    and store it in the database/pms.
    """
    time = str(datetime.now())
    with engine.connect() as connection:
        for status in data.statuses:
            duration = status["duration"]
            count_active = status["count_active"]
            # update experiment_data set display_hours_1 = least(40,greatest(0,greatest(count_active,duration)))/4.0;
            display_hours = min(40, max(0, max(duration, count_active))) / 4
            query = (
                "INSERT IGNORE INTO experiment_data (participant_id, version_code, status_id, date, truncated_entry_time, duration, count_active, "
                + CURRENT_WRITE_DISPLAY_HOURS
                + ") "
                "VALUES (%(participant_id)s, %(version_code)s, %(status_id)s, %(date)s, %(truncated_entry_time)s, %(duration)s, %(count_active)s, %("
                + CURRENT_WRITE_DISPLAY_HOURS
                + ")s);"
            )
            result = connection.execute(
                query,
                {
                    "participant_id": data.participant_id,
                    "status_id": status["status_id"],
                    "version_code": data.version_code,
                    "date": time,
                    "truncated_entry_time": status["truncate_entry_time"],
                    "duration": duration,
                    "count_active": count_active,
                    CURRENT_WRITE_DISPLAY_HOURS: display_hours,
                },
            )
        return {"status": 200}


@app.get("/v3/stats/{participant_id}")
def get_stats_for_participant(participant_id: str) -> dict:
    """
    returns the total number of hours that a participant has spent on campus
    """
    # TODO add a catch for when the participant_id does not exist
    # - consider making this a funcion all on its own?
    if not check_if_participant_id_exists(participant_id):
        payload = {"status": 400, "description": "participant_id does not exist"}
        return payload
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT referral_code, referrer, "
            "GREATEST(" + CURRENT_READ_EXTRA_HOURS + " + total_hours, 0) AS hours FROM participants, "
            "(SELECT SUM(" + CURRENT_READ_DISPLAY_HOURS + ") AS total_hours FROM experiment_data "
            "WHERE participant_id = %(participant_id)s) t "
            "WHERE participants.participant_id = %(participant_id)s",
            {"participant_id": participant_id},
        ).fetchone()

        count = connection.execute(
            "SELECT COUNT(*) AS count FROM participants JOIN "
            "(SELECT participant_id, SUM(" + CURRENT_READ_DISPLAY_HOURS + ") AS total_hours FROM experiment_data "
            "GROUP BY experiment_data.participant_id) t "
            "ON participants.participant_id = t.participant_id "
            "WHERE (" + CURRENT_READ_EXTRA_HOURS + " + total_hours) >= 20 "
            "AND referrer = %(referral_code)s",
            {"referral_code": result["referral_code"]}
        ).fetchone()["count"]

        campus_hours = min(round(float(result["hours"] or 0), 0), 200.0)
        eligible_hours = (
            campus_hours + min(campus_hours, 20)
            + 5.0 * int(result["referrer"] != "") * int(campus_hours >= 20)
            + 5.0 * min(count, 10)
        )

        logging.debug(f"participant {participant_id} has {campus_hours} campus hours and {eligible_hours} eligible hours")
        return {
            "participant_id": participant_id,
            "total_hours_on_campus": campus_hours,
            "eligible_hours": eligible_hours,
            "status": 200,
        }


# TODO add caching to this function, wit daily ttl
@app.get("/v3/stats")
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
        result = connection.execute(
            "SELECT referral_code, referrer, "
            "GREATEST(LEAST(" + CURRENT_READ_EXTRA_HOURS + " + total_hours, 200), 0) AS hours FROM participants JOIN "
            "(SELECT participant_id, SUM(" + CURRENT_READ_DISPLAY_HOURS + ") AS total_hours FROM experiment_data "
            "GROUP BY experiment_data.participant_id) t "
            "ON participants.participant_id = t.participant_id "
            "WHERE (" + CURRENT_READ_EXTRA_HOURS + " + total_hours) > 0"
        )
        rows = result.fetchall()

        campus_hours = [round(float(row[2]), 0) for row in rows]

        referrers = list(map(lambda row: row[1], filter(lambda row: row[2] >= 20, rows)))
        eligible_hours = [
            x + min(x, 20.0)
            + 5.0 * min(referrers.count(rows[i][0]), 10) * int(x >= 20)
            + 5.0 * int(rows[i][1] != "")
            for i, x in enumerate(campus_hours)
        ]

        hist, bin_edges = np.histogram(eligible_hours, bins=15)

        hist = [round(i, 2) for i in hist.tolist()]
        bin_edges = [round(i, 2) for i in bin_edges.tolist()]
    
        kde = gaussian_kde(eligible_hours)
        x_smooth = np.linspace(0, max(eligible_hours), 100)
        y_smooth = kde(x_smooth)

        payload = {
            "hist": hist, "bin_edges": bin_edges,
            "x_smooth": list(x_smooth), "y_smooth": list(y_smooth),
        }
        return payload


@app.get("/v3/num_participants")
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

        result = connection.execute(
            "SELECT COUNT(DISTINCT participants.participant_id) FROM participants JOIN "
            f"(SELECT participant_id, SUM({CURRENT_READ_DISPLAY_HOURS}) AS total_hours "
            "FROM experiment_data GROUP BY experiment_data.participant_id ) t "
            "ON participants.participant_id = t.participant_id "
            f"WHERE ({CURRENT_READ_EXTRA_HOURS} + total_hours) > 0"
        )
        num_active = result.fetchone()[0]

        logging.debug(f"current number of participants: {num_participants}")
        logging.debug(f"current number of active participants: {num_active}")
        return {"num_participants": f"{num_participants}", "num_active": f"{num_active}"}


@app.get("/v3/referral/{participant_id}")
def get_referral_code(participant_id: str):
    """
    Gets a participant's referral code.
    """
    if not check_if_participant_id_exists(participant_id):
        payload = {"status": 400, "description": "participant_id does not exist"}
        return payload
    
    with engine.connect() as connection:
        query = """SELECT referral_code
                   FROM participants
                   WHERE participant_id = %(participant_id)s;"""
        result = connection.execute(query, {"participant_id": participant_id})
        referral_code = result.fetchone()[0]
        return {
            "participant_id": participant_id,
            "referral_code": referral_code,
            "status": 200
        }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=True, debug=True)
