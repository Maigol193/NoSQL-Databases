#!/usr/bin/env python3
import logging
import os
import random
import datetime

from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Show All Flights",
        2: "Show data related to the Airport",
        3: "Show data related to the Airport (more details)",
        4: "Summary of months by Airport",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            model.get_all_flights(session)
        if option == 2:
            account = input('Enter the airport to see all data: ')
            model.get_general_campaign(session, account)
        if option == 3:
            account = input('Enter the airport to see all data (more detailed): ')
            model.get_detailed_campaign(session, account)
        if option == 4:
            account = input('Enter the airport for the Summary: ')
            model.get_best_month(session, account)            
        if option == 5:
            exit(0)

if __name__ == '__main__':
    main()