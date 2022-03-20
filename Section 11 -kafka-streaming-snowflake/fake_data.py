import pandas as pd
import random
from faker import Faker
from random import randrange
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer

nr_of_customers = 100

fake = Faker('de_DE')

customers = dict()

topic_name='sales-data'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

for customers_id in range(nr_of_customers):

    # Create transaction date 
    d1 = datetime.strptime(f'1/1/2021', '%m/%d/%Y')
    d2 = datetime.strptime(f'8/10/2021', '%m/%d/%Y')
    transaction_date = fake.date_between(d1, d2)

    #create customer's name
    name = fake.name()

    # Create gender
    gender = random.choice(["M", "F"])

    # Create email 
    email = fake.ascii_email()

    #Create city
    city = fake.city()

    #create product ID in 8-digit barcode
    product_id = fake.ean(length=8)
    
    #create amount spent
    amount_spent = fake.pyfloat(right_digits=2, positive=True, min_value=1, max_value=100)

    customers={
            'transaction_date':str(transaction_date) ,
            'name':name,'gender':gender,'city':city,
            'email':email,'product_id':product_id,
            'amount_spent':amount_spent
            }
    print(customers)
    
    producer.send(topic_name, value=dumps(customers))
    sleep(5)