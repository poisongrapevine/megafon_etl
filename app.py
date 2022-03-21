import pandas as pd
import numpy as np
import random as rn
import sqlite3

from random import randrange
from datetime import timedelta, datetime

from faker import Faker

def generate_data(db_filename):
    
    fake = Faker('ru_RU')
    
    # generate tables

    users = pd.DataFrame(columns=['balance', 'enter_date', 'age', 'city', 'last_activity', 'active_rate_plan'])

    # users: random generation using random and faker
    # 1000 users

    balance = np.random.randint(0, 1000, size=(1000, ))
    users['balance'] = pd.Series(balance)
    enter_date = [fake.date_between(start_date='-10y') for i in range(1000)]
    users['enter_date'] = pd.Series(enter_date)
    for i in range(0,100):
        while True:
            try:
                users['end_date'] = users['enter_date'].apply(lambda x: x.replace(x.year + rn.randint(1, 8)))
            except ValueError:
                continue
            break    
    users['age'] = np.random.randint(16, 90, size=(1000, ))
    users['city'] = [fake.city_name() for i in range(1000)]
    # last_activity is left blank, to be updated from events
    users['active_rate_plan'] = np.random.randint(0, 7, size=(1000, ))
    events = pd.DataFrame(columns=['time', 'user_id', 'service_type', 'units_spent'])

    # rate plans: made after the real plans

    rates = [['Минимум', '', '', 300, 0, 6],
             ['Звонки', '', '', 800, 0, 6],
             ['Интернет', '', '', 300, 0, 30],
             ['Мега тариф', '', '', 800, 300, 35],
             ['Максимум', '', '', 1200, 300, 50],
             ['VIP', '', '', 1700, 300, 50],
             ['Премиум', '', '', 5000, 300, 60]]

    rate_plans = pd.DataFrame(rates, columns=['name', 'start_date', 'end_date', 'minutes', 'sms', 'traffic'])
    rate_plans['traffic'] = rate_plans['traffic'] * 1024 # turn into megabytes
    rate_plans.drop(columns=['start_date', 'end_date'], inplace=True) 
    # i have removed the dates because i can't understand what they mean

    # generate events
    # i have named service types like columns in rate_plans for aggregation convenience

    events['user_id'] = np.random.randint(0, 1000, size=(50_000, ))
    events['service_type'] = [rn.choice(['minutes', 'sms', 'traffic']) for i in range(50_000)]
    events['units_spent'] = np.random.randint(1, 10, size=(50_000, ))

    # make sure all the timestamps are after the enter_time

    for user in range(1000):
        times = events.loc[events['user_id'] == user, 'user_id'].apply(lambda x: fake.date_time_between(start_date=users.loc[x]['enter_date']))
        events.loc[times.index, 'time'] = times.values.astype(datetime)

    # now fill in the users['last_activity']

    for user in range(1000):
        users.loc[user, 'last_activity'] = events.loc[events['user_id'] == user, 'time'].max()

    # make database

    users.to_sql('users', con=con)
    events.to_sql('events', con=con)
    rate_plans.to_sql('rate_plans', con=con)

# make the aggregation for each date and user

def aggregation():
    
    query = '''
            SELECT 
            user_id, date(time/1000000000, 'unixepoch') AS date, 
            SUM(CASE WHEN service_type='minutes' THEN units_spent ELSE 0 END) AS minutes_used,
            SUM(CASE WHEN service_type='sms' THEN units_spent ELSE 0 END) AS sms_used,
            SUM(CASE WHEN service_type='traffic' THEN units_spent ELSE 0 END) AS traffic_used
            FROM events
            GROUP BY date
            '''

    return pd.read_sql(query, con=con)

if __name__ == '__main__':
    
    db_filename = input('Database filename: ')
    con = sqlite3.connect(db_filename)
    cur = con.cursor()
    generate_data(db_filename)
    print('Aggregation ready: \n', aggregation())