# Thisfilepath=NonePython script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import psycopg2
import pandas as pds
import pandas as pd
import numpy as np

from sqlalchemy import create_engine


def load_data():
    # Example python program to read data from a PostgreSQL table

    # and load into a pandas DataFrame

    # Create an engine instance

    alchemyEngine = create_engine('postgresql+psycopg2://postgres:pass@127.0.0.1', pool_recycle=3600)

    # Connect to PostgreSQL server

    dbConnection = alchemyEngine.connect()

    # Read data from PostgreSQL database table and load into a DataFrame instance

    dataFrame = pds.read_sql("select * from public.\"MY_TABLE\" ", dbConnection)

    pds.set_option('display.expand_frame_repr', False)

    # Print the DataFrame

    # print(dataFrame)

    # Close the database connection

    dbConnection.close()
    return dataFrame


def load_datafram(filepath):
    df = pd.read_csv(filepath, index_col=0)
    print(df.head())
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
    print('---------- all orders ----------')
    print(hub_orders_all.head())

    orders_total = hub_orders_all.groupby(['supplier_id']).count()

    orders_total = orders_total.iloc[:, :1]

    # hub_req['supplier_id'].count()
    names = orders_total.columns.tolist()
    names[0] = 'total'
    orders_total.columns = names


    orders_total.reset_index(inplace=True)
    orders_total.sort_values(by=['total'], ascending=False)
    print('------- supplier orders -------')
    print(orders_total.head())

    return  orders_total

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

    #change in columns
    hub_accepted_orders['supplier_id'].nunique()
    order_acc = hub_accepted_orders.groupby(['supplier_id']).count()
    order_acc = order_acc.iloc[:, :1]
    order_acc.sort_values(by=['event'], ascending=False)
    # hub_orders['supplier_id'].count()

    names = order_acc.columns.tolist()
    names[0] = 'accepted'
    order_acc.columns = names
    order_acc.reset_index(inplace=True)
    order_acc.sort_values(by=['accepted'], ascending=False)

    print('------ accepeted orders -----')
    print(order_acc.head())
    return  order_acc

def cal_accept_ratio(df):
    df_all=load_all_orders(df)
    df_accepteds=get_accepted_orders(df)

    ratio_df = pd.merge(df_all, df_accepteds)

    ratio_df['ratio'] = round((ratio_df['accepted'] / ratio_df['total']) * 100)
    ratio_df.sort_values(by=['ratio'], ascending=False)
    print('-------acceptance ratio ------')
    print(ratio_df)
# Press the green button in the gutter to run the script.
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
    #load_all_orders(df)
    cal_accept_ratio(df)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
