import datetime
import sys
import uuid

from typing import Callable, Dict, List
from uuid import UUID

import pytz
from sqlalchemy import create_engine
from sqlalchemy.sql import text

###########################################################
## SETTINGS
###########################################################
db1_string = "postgres://dwbh:dwbh@localhost:5432/dwbh_old"
db2_string = "postgres://dwbh:dwbh@localhost:5432/dwbh"

tz = pytz.timezone('Europe/Madrid')

###########################################################
## UTILS FUNCTIONS
###########################################################

def stdout(message: str) -> None:
    print(f"[dwbh-1to2] {message}")

def stderr(message: str) -> None:
    print(f"[dwbh-1to2] {message}", file=sys.stderr)

ID: Callable[[], UUID] = lambda: uuid.uuid4()

###########################################################
## QUERIES
###########################################################

# users
query_select_users = """
    SELECT id, username, full_name, password
      FROM dwbh_user
"""
query_insert_users = """
    INSERT INTO users
                (id, name, email, password)
         VALUES (:id, :name, :email, :password)
"""

# groups
query_select_groups = """
    SELECT id, name, visible_member_list, anonymous_vote, hour, owner_id,
           day1, day2, day3, day4, day5, day6, day7
      FROM dwbh_group
"""
query_insert_groups = """
    INSERT INTO groups
                (id, name, visible_member_list, anonymous_vote, voting_time, voting_days)
         VALUES (:id, :name, :visible_member_list, :anonymous_vote, :voting_time, :voting_days)
"""

# users_groups
query_select_users_groups = """
    SELECT group_id, user_id
      FROM user_group
"""
query_insert_users_groups = """
    INSERT INTO users_groups
                (group_id, user_id, is_admin)
         VALUES (:group_id, :user_id, :is_admin)
"""

# voting
query_select_voting = """
    SELECT id, group_id, date, average
      FROM votation
"""
query_insert_voting = """
    INSERT INTO voting
                (id, group_id, created_at, created_by, average)
         VALUES (:id, :group_id, :created_at, :created_by, :average)
"""
# vote
query_select_vote = """
    SELECT votation_id, user_id, date, value, comment
      FROM vote
"""
query_insert_vote = """
    INSERT INTO vote
                (id, voting_id, created_by, created_at, score, comment)
         VALUES (:id, :voting_id, :created_by, :created_at, :score, :comment)
"""

# MAIN
if __name__ == "__main__":
    # Connections
    try:
        db1 = create_engine(db1_string)
    except Exception as e:
        stderr(f"Error creating '{db1_string}': {e}")
        exit(1)

    try:
        engine2 = create_engine(db2_string)
    except Exception as e:
        stderr(f"Error creating '{db2_string}': {e}")
        exit(1)

    db2 = engine2.connect()

    with db2.begin() as trans:
        ###########################################################
        # USERS
        ###########################################################
        users: Dict[str, Dict[str, str]] = {}

        result_set = db1.execute(text(query_select_users))
        for result in result_set:
            id, username, full_name, password = result
            user = {
              "old_id": id,
              "id": ID(),
              "name": full_name,
              "email": username,
              "password": password,
            }
            db2.execute(text(query_insert_users), **user)
            users[id] = user

        ###########################################################
        # GROUPS
        ###########################################################
        groups: Dict[str, Dict[str, str]] = {}

        result_set = db1.execute(text(query_select_groups))
        for result in result_set:
            id, name, visible_member_list, anonymous_vote, hour, owner_id, day1, day2, day3, day4, day5, day6, day7 = result
            group = {
                "old_id": id,
                "id": ID(),
                "name": name,
                "owner_id": owner_id,
                "visible_member_list": visible_member_list,
                "anonymous_vote": anonymous_vote,
                "voting_time": datetime.time(hour, 0, 0),
                "voting_days": []
            }
            if day1: group["voting_days"].append("MONDAY")
            if day2: group["voting_days"].append("TUESDAY")
            if day3: group["voting_days"].append("WEDNESDAY")
            if day4: group["voting_days"].append("THURSDAY")
            if day5: group["voting_days"].append("FRIDAY")
            if day6: group["voting_days"].append("SATURDAY")
            if day7: group["voting_days"].append("SUNDAY")
            db2.execute(text(query_insert_groups), **group)
            groups[id] = group

        ###########################################################
        # USER GROUPS RELATIONSHIPD
        ###########################################################
        users_groups: List[Dict[str, str]] = []
        result_set = db1.execute(text(query_select_users_groups))
        for result in result_set:
            group_id, user_id = result
            user_group = {
                "group_id": groups[group_id]["id"],
                "user_id": users[user_id]["id"],
                "is_admin": groups[group_id]['owner_id'] == user_id
            }

            db2.execute(text(query_insert_users_groups), **user_group)
            users_groups.append(user_group)


        ###########################################################
        # VOTTING
        ###########################################################
        votings: Dict[str, Dict[str, str]] = {}
        result_set = db1.execute(text(query_select_voting))
        for result in result_set:
            id, group_id, date, average = result
            voting = {
                "old_id": id,
                "id": ID(),
                "group_id": groups[group_id]['id'],
                "created_at": tz.localize(date),
                "created_by": users[groups[group_id]["owner_id"]]["id"],
                "average": average
            }
            db2.execute(text(query_insert_voting), **voting)
            votings[id] = voting

        ###########################################################
        # VOTE
        ###########################################################
        votes: List[Dict[str, str]] = []
        result_set = db1.execute(text(query_select_vote))
        for result in result_set:
            votation_id, user_id, date, value, comment = result
            vote = {
                "id": ID(),
                "voting_id": votings[votation_id]["id"],
                "created_by": users[user_id]['id'] if user_id  else None,
                "created_at": tz.localize(date),
                "score": value,
                "comment": comment or ''
            }
            db2.execute(text(query_insert_vote), **vote)
            votes.append(vote)
