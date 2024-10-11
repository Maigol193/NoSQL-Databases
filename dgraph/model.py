#!/usr/bin/env python3
import datetime
import json
import os
import pydgraph
import csv

import model

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def set_schema(client):
    schema = """
    type Passenger {
        age
        gender
        takes
    }

    age: int .
	gender: string .
    takes: [uid] @reverse .

	type Flight {
		airline
        month
	}

    airline: string @index(exact) .
	month: int @count @index(int) .

    """
    return client.alter(pydgraph.Operation(schema=schema))

def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()

def create_data(client):
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Create schema
    model.set_schema(client)

    todo = []

    with open('flight_passengers.csv', newline='') as csvfile:
        datos = csv.reader(csvfile)
        count = True

        for fila in datos:
            if(count):
                count = False
                continue
            obj1 = {}
            obj2 = {}
            airline, origin , destination, day, month, year, age, gender, reason, stay, transit, connection, wait = fila
            obj1['age'] = age
            obj1['gender'] = gender
            obj1['uid'] = "_:"+age+gender
            obj1['dgraph.type'] = 'Passenger'
            obj2['airline'] = airline
            obj2['month'] = month
            obj2['uid'] = "_:"+airline+month
            obj2['dgraph.type'] = 'Flight'
            obj1['takes'] = [obj2]
            todo.append(obj1)

        
    # Create a new transaction.
    txn = client.txn()
    try:
        p = todo

        response = txn.mutate(set_obj=p)

        # Commit transaction.
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")

        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()

#query count_months($a: int)
def count_months_general(client, name): #count_months(client, num)
    query = """query count_months($a: int) { 
        countByMonth(func: eq(month, $a)) {
  	        airline
            ~takes{
            total: count(uid)	
 		    }
        }	
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Mostrando la frecuencia de vuelos tomados por mes:\n{json.dumps(ppl, indent=2)}")

#search_user
def count_months_by_airline(client, name):
    query = """query count_months_by_airline($a: string) {
        countByMonth(func: eq(airline, $a)) {
  	        month
            ~takes{
            total: count(uid)	
 		    }
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Mostrando la frecuencia de vuelos tomados por mes para la aerolinea {name}:\n{json.dumps(ppl, indent=2)}")

def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))
