import airflow
import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from faker import Faker
import json
import csv
import random
import urllib.request as request

fake = Faker()

def getRaces():
    response = request.urlopen("http://www.dnd5eapi.co/api/races").read()
    return json.loads(response.decode('utf8'))

def getLanguages(raceIndex):
    response = request.urlopen("http://www.dnd5eapi.co/api/races/"+raceIndex).read()
    return json.loads(response.decode('utf8'))["languages"]

def getClasses():
    response = request.urlopen("http://www.dnd5eapi.co/api/classes").read()
    return json.loads(response.decode('utf8'))

def getSpells(classIndex, level):
    response = request.urlopen("http://www.dnd5eapi.co/api/classes/"+classIndex+'/spells?level=1,2').read()
    resultArray = json.loads(response.decode('utf8'))
    string = ''
    if resultArray["count"] != 0 :
        for _ in range(level + 3):
            string += resultArray["results"][random.randrange(resultArray["count"])]["index"] + ','
    return string[:-1]

def getProficiencies(classIndex):
    response = request.urlopen("http://www.dnd5eapi.co/api/classes/"+classIndex).read()
    resultArray = json.loads(response.decode('utf8'))["proficiencies"]
    string = ''
    for proficiencie in resultArray:
        string += proficiencie["index"] + ','
    return string[:-1]


default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=30),
}

dnd_dag = DAG(
    dag_id='dnd_dag',
    default_args=default_args_dict,
    catchup=False,
)

def generate_character_csv() :
    with open('characters.csv','w', newline='') as csvfile:
        fieldnames = ['name','attributes', 'race', 'languages', 'class', 'profficiency_choices', 'level', 'spells']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        races = getRaces()
        classes = getClasses()
        for _ in range(5):
            randomRace = races["results"][random.randrange(races["count"])]["index"]
            languages = getLanguages(randomRace)
            randomClass = classes["results"][random.randrange(classes["count"])]["index"]
            randomLevel = random.randrange(1,3)
            writer.writerow({'name': fake.name(), 
            'attributes': '['+str(random.randrange(6,18))+','+str(random.randrange(2,18))+','+str(random.randrange(2,18))+','+str(random.randrange(2,18))+','+str(random.randrange(2,18))+','+str(random.randrange(2,18))+']',
            'race': randomRace,
            'languages': languages[random.randrange(len(languages))]["index"],
            'class': randomClass,
            'profficiency_choices': getProficiencies(randomClass),
            'level': randomLevel,
            'spells': getSpells(randomClass, randomLevel)
            })
        csvfile.close()

task_one = PythonOperator(
    task_id='generate_csv',
    dag= dnd_dag,
    python_callable= generate_character_csv,
    trigger_rule='all_success',
    depends_on_past=False,
)

def create_insert_script():
    with open('characters.csv','r') as csvfile:
        with open('insert_characters.sql', 'w') as sqlfile:
            reader = csv.DictReader(csvfile)
            sqlfile.write(
            "CREATE TABLE IF NOT EXISTS characters (\n"
            "name VARCHAR(255),\n"
            "attributes VARCHAR(255),\n"
            "race VARCHAR(255),\n"
            "languages VARCHAR(255),\n"
            "class VARCHAR(255),\n"
            "profficiency_choices VARCHAR(1000),\n"
            "level VARCHAR(255),\n"
            "spells VARCHAR(255));\n"
            )
            for row in reader:
                sqlfile.write(
                "INSERT INTO characters VALUES ("
                f"'{row['name']}', '{row['attributes']}', '{row['race']}', '{row['languages']}', '{row['class']}', '{row['profficiency_choices']}', '{row['level']}', '{row['spells']}'"
                ");\n"
                )
        sqlfile.close()
    csvfile.close()

task_two = PythonOperator(
    task_id='create_insertion_query',
    dag=dnd_dag,
    python_callable=create_insert_script,
    trigger_rule='all_success',
)

task_three = PostgresOperator(
    task_id='insert_character_query',
    dag=dnd_dag,
    postgres_conn_id='postgres_default',
    sql='insert_characters.sql',
    trigger_rule='all_success',
    autocommit=True,
)

end = DummyOperator(
    task_id='end',
    dag=dnd_dag,
    trigger_rule='none_failed'
)


task_one >> task_two >> task_three >> end