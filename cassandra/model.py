#!/usr/bin/env python3
import logging

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

#general_campaign
CREATE_GENERAL_RENTAL_CAMPAIGN_TABLE = """
    CREATE TABLE IF NOT EXISTS general_campaign (
        destination TEXT,
        month INT,
        transit TEXT,
        day INT,
        PRIMARY KEY ((transit, destination), month, day)
    )
"""

SELECT_BEST_MONTH_GENERAL_CAMPAIGN = """
    SELECT destination, month
    FROM general_campaign
    WHERE transit = ? AND destination = ?
"""

SELECT_BEST_MONTH = """
    SELECT destination, month, count(*)
    FROM general_campaign
    WHERE transit = ? AND destination = ?
    GROUP BY month
"""

#general_campaign
CREATE_RENTAL_CAMPAIGN_TABLE = """
    CREATE TABLE IF NOT EXISTS rental_campaign (
        destination TEXT,
        month INT,
        transit TEXT,
        day INT,
        reason TEXT,
        PRIMARY KEY ((transit, destination), reason, month, day)
    )
"""

SELECT_BEST_MONTH_RENTAL_CAMPAIGN = """
    SELECT destination, month, reason
    FROM rental_campaign
    WHERE transit = ? AND destination = ?
"""

#show all flights
CREATE_ALL_FLIGHTS = """
    CREATE TABLE IF NOT EXISTS all_flights(
        origin TEXT,
        destination TEXT,
        year INT,
        month INT,
        day INT,
        airline TEXT,
        PRIMARY KEY((origin, destination), year, month, day, airline )
    )


"""

SELECT_ALL_FLIGHTS = """
    SELECT origin, destination, year, month, day, airline
    FROM all_flights
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_GENERAL_RENTAL_CAMPAIGN_TABLE)
    session.execute(CREATE_RENTAL_CAMPAIGN_TABLE)
    session.execute(CREATE_ALL_FLIGHTS)

#get_user_accounts
def get_all_flights(session):
    log.info(f"Retrieving all flights")
    stmt = session.prepare(SELECT_ALL_FLIGHTS)
    rows = session.execute(stmt) #rows = session.execute(stmt, [username])
    for row in rows:
        print(f"=== From: {row.origin} ===")
        print(f"=== To: {row.destination} ===")
        print(f"=== Date: {row.day}/{row.month}/{row.year} ===")
        print(f"=== Airline: {row.airline} === \n")

#get_positions_accounts
def get_general_campaign(session, airport):
    log.info(f"Showing the best options for the campaign")
    stmt = session.prepare(SELECT_BEST_MONTH_GENERAL_CAMPAIGN)
    rows = session.execute(stmt, ["Car rental", airport])
    for row in rows:
        print(f"=== Airport: {row.destination} ===")
        print(f"=== Month: {row.month} === \n")

#get_best
def get_best_month(session, airport):
    log.info(f"Showing the best options for the campaign")
    stmt = session.prepare(SELECT_BEST_MONTH)
    rows = session.execute(stmt, ["Car rental", airport])
    for row in rows:
        print(f"=== Airport: {row.destination} ===")
        print(f"=== Month: {row.month} === ")
        print(f"=== Count: {row.count} === \n")

#get_trades_all
def get_detailed_campaign(session, airport):
    log.info(f"Showing the best options for the campaign (more detailed)")
    stmt = session.prepare(SELECT_BEST_MONTH_RENTAL_CAMPAIGN)
    rows = session.execute(stmt, ["Car rental", airport])
    for row in rows:
        print(f"=== Airport: {row.destination} ===")
        print(f"=== Month: {row.month} ===")
        print(f"=== Reason: {row.reason} === \n")

