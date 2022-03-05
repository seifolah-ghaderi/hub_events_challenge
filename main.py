# Thisfilepath=NonePython script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import datetime
import time

import psycopg2
import pandas as pds
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
db_conn='postgresql+psycopg2://postgres:pass@127.0.0.1'

def load_data():

    # and load into a pandas DataFrame

    # Create an engine instance

    alchemyEngine = create_engine(db_conn, pool_recycle=3600)

    # Connect to PostgreSQL server

    dbConnection = alchemyEngine.connect()

    # Read data from PostgreSQL database table and load into a DataFrame instance

    dataFrame = pds.read_sql("select * from public.\"MY_TABLE\" ", dbConnection)

    pds.set_option('display.expand_frame_repr', False)

    dbConnection.close()
    return dataFrame


def wrtie_df_todb(df_table):
    # and load into a pandas DataFrame
    deleted_rows = delete_today_records()
    print('cleaned today rows : ', deleted_rows)
    # Create an engine instance

    alchemyEngine = create_engine(db_conn, pool_recycle=3600)

    # Connect to PostgreSQL server

    dbConnection = alchemyEngine.connect()

    df_table.to_sql('supplier_score_metrics', alchemyEngine,if_exists='append',index=False)

    dbConnection.close()
    print('---write to database done √---')
def delete_today_records():

    conn = None
    rows_deleted = 0
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(database="postgres", user='postgres', password='pass', host='127.0.0.1', port= '5432')
        conn.autocommit = True
        # create a new cursor
        cur = conn.cursor()
        # remove today records, to prevent dublicate records
        str= "delete from public.supplier_score_metrics s where s.calculated_at like '%s'" % time.strftime("%Y-%m-%d")
        cur.execute(str)
        # get the number of updated rows
        rows_deleted = cur.rowcount
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return rows_deleted
def load_datafram(filepath):
    df = pd.read_csv(filepath, index_col=0)
    return df


def load_all_orders(df):
    df_mask = df['name'] == 'order/execute/customer/status/processing'
    req = df[df_mask].copy()

    data = []
    ff = req.iloc[:, 2]
    for i in ff:
        m = (pd.json_normalize(eval(i)))
        data.append(m[['event', 'hub_id', 'order_id', 'timestamp', 'context_traits_uid']])

    hub_orders_all = pd.DataFrame(np.concatenate(data))
    hub_orders_all.columns = ['event', 'supplier_id', 'order_id', 'timestamp', 'customer_id']

    return hub_orders_all


def get_accepted_orders(df):
    df_mask = df['name'] == 'order/execute/customer/status/payment'
    orders_accepted = df[df_mask].copy()

    data = []
    ff = orders_accepted.iloc[:, 2]
    for i in ff:
        m = (pd.json_normalize(eval(i)))
        data.append(m[['event', 'hub_id', 'order_id', 'timestamp', 'context_traits_uid']])

    hub_accepted_orders = pd.DataFrame(np.concatenate(data))
    hub_accepted_orders.columns = ['event', 'supplier_id', 'order_id', 'timestamp', 'customer_id']

    return hub_accepted_orders
def cal_accepted_orders_count(hub_accepted_orders):
    # change in columns
    order_acc = hub_accepted_orders.groupby(['supplier_id']).count()
    order_acc = order_acc.iloc[:, :1]
    order_acc.sort_values(by=['event'], ascending=False)

    names = order_acc.columns.tolist()
    names[0] = 'accepted'
    order_acc.columns = names
    order_acc.reset_index(inplace=True)
    order_acc.sort_values(by=['accepted'], ascending=False)

    return  order_acc
def cal_all_order_count(df_all):
    df_all = df_all.groupby(['supplier_id']).count()

    df_all = df_all.iloc[:, :1]

    names = df_all.columns.tolist()
    names[0] = 'total'
    df_all.columns = names

    df_all.reset_index(inplace=True)
    df_all.sort_values(by=['total'], ascending=False)

    return df_all
def cal_accept_ratio(df):
    df_all_o = load_all_orders(df)
    df_accepteds_o = get_accepted_orders(df)

    df_all=cal_all_order_count(df_all_o)
    df_accepteds=cal_accepted_orders_count(df_accepteds_o)

    ratio_df = pd.merge(df_all, df_accepteds)

    ratio_df['value'] = round((ratio_df['accepted'] / ratio_df['total']) * 100).astype(int)
    ratio_df=ratio_df[['supplier_id','value']]
    ratio_df=ratio_df.sort_values(by=['value'], ascending=False)
    ratio_df['metric'] = 'acceptance_ratio'
    ratio_df['calculated_at'] = time.strftime("%Y-%m-%d")
    print('-------acceptance ratio ------')
    print(ratio_df)
    return ratio_df
def cal_sup_resp_time(df):
    df_all = load_all_orders(df)
    df_accepteds = get_accepted_orders(df)

    req = df_all.copy()
    order = df_accepteds.copy()

    req = req[['order_id', 'supplier_id', 'customer_id', 'timestamp']]
    req.rename(columns={"timestamp": "order_time"},inplace = True)
    order = order[['order_id', 'supplier_id', 'customer_id', 'timestamp']]
    order.rename(columns={"timestamp": "accept_time"},inplace = True)
    t2 = pd.merge(order, req, on=["order_id", "supplier_id", "customer_id"])
    t2['order_time'] = pd.to_datetime(t2['order_time'])
    t2['accept_time'] = pd.to_datetime(t2['accept_time'])
    t2['value'] = round((t2['accept_time'] - t2['order_time']) / pd.Timedelta(hours=1), 2).astype(int)

    t2 = t2[['supplier_id', 'value']]

    g2 = t2.groupby(['supplier_id']).mean().astype(int)

    g2 = g2.sort_values(by=['value'], ascending=True)
    g2.reset_index(inplace=True)
    grouped_df = g2.sort_values(by=['value'], ascending=False)
    grouped_df['metric'] = 'average_response_time_h'
    grouped_df['calculated_at'] = time.strftime("%Y-%m-%d")
    print('-------supplier response time avg -----')
    print(grouped_df)
    return grouped_df
def get_all_reviews(df):
    df_mask = df['name'] == 'node/review/created'
    reviews = df[df_mask].copy()

    data = []
    ff = reviews.iloc[:, 2]
    for i in ff:
        m = (pd.json_normalize(eval(i)))
        data.append(m[['event', 'hub_id', 'order_id', 'timestamp', 'context_traits_uid', 'review_value_speed',
                       'review_value_print_quality']])

    hub_reviews = pd.DataFrame(np.concatenate(data))
    hub_reviews.columns = ['event', 'supplier_id', 'order_id', 'timestamp', 'customer_id', 'review_speed',
                           'review_quality']

    df_rv = hub_reviews[['supplier_id', 'order_id','customer_id','review_speed', 'review_quality']].copy()

    df_rv['review_speed'] = df_rv['review_speed'].astype('Int64')
    df_rv['review_quality'] = df_rv['review_quality'].astype('Int64')

    return df_rv


def get_upd_reviews(df):
    df_mask = df['name'] == 'node/review/updated'
    reviews = df[df_mask].copy()

    data = []
    ff = reviews.iloc[:, 2]
    for i in ff:
        m = (pd.json_normalize(eval(i)))
        data.append(m[['event', 'hub_id', 'order_id', 'timestamp', 'context_traits_uid', 'review_value_speed',
                       'review_value_print_quality']])

    rev = pd.DataFrame(np.concatenate(data))
    rev.columns = ['event', 'supplier_id', 'order_id', 'timestamp', 'customer_id', 'review_speed',
                   'review_quality']

    df_rv = rev[['supplier_id', 'order_id', 'customer_id', 'review_speed', 'review_quality']].copy()

    hub_rev_upd = df_rv[~df_rv['review_speed'].isnull() | ~df_rv['review_quality'].isnull()].copy()

    hub_rev_upd['review_speed'] = hub_rev_upd['review_speed'].astype('Int64')
    hub_rev_upd['review_quality'] = hub_rev_upd['review_quality'].astype('Int64')

    return hub_rev_upd


def get_del_reviews(df):
    df_mask = df['name'] == 'node/review/deleted'
    reviews = df[df_mask].copy()

    data = []
    ff = reviews.iloc[:, 2]
    for i in ff:
        m = (pd.json_normalize(eval(i)))
        data.append(m[['event', 'hub_id', 'order_id', 'timestamp', 'context_traits_uid', 'review_value_speed',
                       'review_value_print_quality']])

    hub_reviews = pd.DataFrame(np.concatenate(data))
    hub_reviews.columns = ['event', 'supplier_id', 'order_id', 'timestamp', 'customer_id', 'review_speed',
                           'review_quality']

    df_rv = hub_reviews[['supplier_id', 'order_id','customer_id', 'review_speed', 'review_quality']].copy()

    df_rv['review_speed'] = df_rv['review_speed'].astype('Int64')
    df_rv['review_quality'] = df_rv['review_quality'].astype('Int64')

    return df_rv


def cal_review(df):
    df_reviews = get_all_reviews(df)
    df_upd_reviews = get_upd_reviews(df)
    df_del_reviews = get_del_reviews(df)

    # apply updated reviews
    df_reviews = df_reviews.set_index('order_id')
    df_upd_reviews = df_upd_reviews.set_index('order_id')[['review_speed', 'review_quality']]

    df_reviews.update(df_upd_reviews)
    df_reviews = df_reviews.reset_index()

    # delete removed reviews
    cond = df_reviews['order_id'].isin(df_del_reviews['order_id']) & df_reviews['customer_id'].isin(df_del_reviews['customer_id'])
    df_reviews.drop(df_reviews[cond].index, inplace=True)

    # calculate avg reviews
    df_reviews = df_reviews[['supplier_id', 'review_speed', 'review_quality']].copy()
    # df_rv
    df_reviews['review_speed'] = df_reviews['review_speed'].astype('Int64')
    df_reviews['review_quality'] = df_reviews['review_quality'].astype('Int64')
    grouped_df = df_reviews.groupby(['supplier_id']).mean().astype(int)

    grouped_df=grouped_df.reset_index()
    grouped_df['value'] = round((grouped_df['review_speed'] + grouped_df['review_quality']) / 2).astype(int)
    grouped_df=grouped_df[['supplier_id', 'value']].copy()

    grouped_df = grouped_df.sort_values(by=['value'], ascending=False)
    grouped_df['metric'] = 'average_rating'
    grouped_df['calculated_at'] = time.strftime("%Y-%m-%d")
    print('----final avg rating ----')
    print(grouped_df)
    return  grouped_df

csv_file = 'csv/hub.csv'

if __name__ == '__main__':

    try:
        f = open(csv_file)
        # Do something with the file
    except IOError:
        print("File not loaded from db")
        df1 = load_data()
        df1.to_csv(csv_file, index=False)
    finally:
        f.close()

    df = load_datafram(csv_file)
    # load_all_orders(df)
    ratio_df = cal_accept_ratio(df)
    avg_df= cal_review(df)
    df_sup_time = cal_sup_resp_time(df)
    df_res =pd.concat([ratio_df, avg_df,df_sup_time],ignore_index=True)

    print('----- final metric table ----')
    df_res = df_res.sort_values(by=['supplier_id'], ascending=False)
    df_res.reset_index(drop=True, inplace=True)
    print(df_res)
    wrtie_df_todb(df_res)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
