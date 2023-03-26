# Required libraries for the program
import pandas as pd
import json
import os
import mysql.connector


def create_database(database_name):
    """ Creates a Database named database_name = PhonePe """
    # Connect to the MySQL database
    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="Philo@leo92",
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"CREATE DATABASE if not exists {database_name}")


def connect_database():
    """ Creates a connection to database PhonePe """
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Philo@leo92",
        database="PhonePe"
    )
    return mydb


def agg_trans():
    path = "PhonePe_data/data/aggregated/transaction/country/india/state/"
    # Agg_state_list--> to get the list of states in India
    Agg_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'Transaction_Type': [],
           'Transaction_Count': [],
           'Transaction_Amount': []}
    for i in Agg_state_list:
        p_i = path + i + "/"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                for z in D['data']['transactionData']:
                    Name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm['Transaction_Type'].append(Name)
                    clm['Transaction_Count'].append(count)
                    clm['Transaction_Amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created an Aggregate Transaction dataframe
    Agg_Trans = pd.DataFrame(clm)
    # print(Agg_Trans.isnull().sum())
    return Agg_Trans


def agg_user_state_reg():
    path = "PhonePe_data/data/aggregated/user/country/india/state/"
    # Agg_state_list--> to get the list of states in India
    Agg_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'Registered_Users': [],
           'App_Opens': []}

    for i in Agg_state_list:
        p_i = path + i + "/"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                registeredUsers = D['data']['aggregated']['registeredUsers']
                appOpens = D['data']['aggregated']['appOpens']
                clm['Registered_Users'].append(registeredUsers)
                clm['App_Opens'].append(appOpens)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created a Aggregate User State wise Registered dataframe
    Agg_User_State_Reg = pd.DataFrame(clm)
    # print(Agg_User_State_Reg.isnull().sum())
    return Agg_User_State_Reg


def agg_user_state_brand():
    path = "PhonePe_data/data/aggregated/user/country/india/state/"
    # Agg_state_list--> to get the list of states in India
    Agg_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'Brand': [],
           'Registration_Count': [],
           'Registration_Percentage': []}
    for i in Agg_state_list:
        p_i = path + i + "/"
        Agg_yr = os.listdir(p_i)
        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)
            for k in Agg_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                if D['data']['usersByDevice'] is None:
                    pass
                    # clm['Brand'].append("DataNotUpdated")
                    # clm['Registration_Count'].append("DataNotUpdated")
                    # clm['Registration_Percentage'].append("DataNotUpdated")
                    # clm['State'].append(i)
                    # clm['Year'].append(j)
                    # clm['Quarter'].append(int(k.strip('.json')))
                else:
                    for z in D['data']['usersByDevice']:
                        brand = z['brand']
                        count = z['count']
                        percentage = z['percentage']
                        clm['Brand'].append(brand)
                        clm['Registration_Count'].append(count)
                        clm['Registration_Percentage'].append(percentage)
                        clm['State'].append(i)
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created a Aggregate User Each State Brand wise dataframe
    Agg_User_StateBrandWise_Reg = pd.DataFrame(clm)
    # print(Agg_User_StateBrandWise_Reg.isnull().sum())
    return Agg_User_StateBrandWise_Reg


def map_trans():
    path = "PhonePe_data/data/map/transaction/hover/country/india/state/"
    # Map_state_list--> to get the list of states in India
    Map_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'District': [],
           'Transaction_Count': [],
           'Transaction_Amount': []}
    for i in Map_state_list:
        p_i = path + i + "/"
        Map_yr = os.listdir(p_i)
        for j in Map_yr:
            p_j = p_i + j + "/"
            Map_yr_list = os.listdir(p_j)
            for k in Map_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                for z in D['data']['hoverDataList']:
                    district = z['name']
                    count = z['metric'][0]['count']
                    amount = z['metric'][0]['amount']
                    clm['District'].append(district)
                    clm['Transaction_Count'].append(count)
                    clm['Transaction_Amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created a Map Transaction dataframe
    Map_Trans = pd.DataFrame(clm)
    # print(Map_Trans.isnull().sum())
    return Map_Trans


def map_user():
    path = "PhonePe_data/data/map/user/hover/country/india/state/"
    # Map_state_list--> to get the list of states in India
    Map_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'District': [],
           'Registered_Users': [],
           'App_Opens': []}
    for i in Map_state_list:
        p_i = path + i + "/"
        Map_yr = os.listdir(p_i)
        for j in Map_yr:
            p_j = p_i + j + "/"
            Map_yr_list = os.listdir(p_j)
            for k in Map_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                for key, value in D['data']['hoverData'].items():
                    district = key
                    registeredUsers = value['registeredUsers']
                    appOpens = value['appOpens']
                    clm['App_Opens'].append(appOpens)
                    clm['District'].append(district)
                    clm['Registered_Users'].append(registeredUsers)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created a Map User dataframe
    Map_User = pd.DataFrame(clm)
    # print(Map_User.isnull().sum())
    return Map_User


def top_trans_district():
    path = "PhonePe_data/data/top/transaction/country/india/state/"
    # Top_state_list--> to get the list of states in India
    Top_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'District': [],
           'Transaction_Count': [],
           'Transaction_Amount': []}
    for i in Top_state_list:
        p_i = path + i + "/"
        Top_yr = os.listdir(p_i)
        for j in Top_yr:
            p_j = p_i + j + "/"
            Top_yr_list = os.listdir(p_j)
            for k in Top_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                for z in D['data']['districts']:
                    entityName = z['entityName']
                    count = z['metric']['count']
                    amount = z['metric']['amount']
                    clm['District'].append(entityName)
                    clm['Transaction_Count'].append(count)
                    clm['Transaction_Amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created a Top Transaction District dataframe
    Top_Trans_District = pd.DataFrame(clm)
    # print(Top_Trans_District.isnull().sum())
    return Top_Trans_District


def top_trans_pincode():
    path = "PhonePe_data/data/top/transaction/country/india/state/"
    # Top_state_list--> to get the list of states in India
    Top_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'Pincode': [],
           'Transaction_Count': [],
           'Transaction_Amount': []}
    for i in Top_state_list:
        p_i = path + i + "/"
        Top_yr = os.listdir(p_i)
        for j in Top_yr:
            p_j = p_i + j + "/"
            Top_yr_list = os.listdir(p_j)
            for k in Top_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                for z in D['data']['pincodes']:
                    entityName = z['entityName']
                    count = z['metric']['count']
                    amount = z['metric']['amount']
                    clm['Pincode'].append(entityName)
                    clm['Transaction_Count'].append(count)
                    clm['Transaction_Amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created a Top Transaction Pincode dataframe
    Top_Trans_Pincode = pd.DataFrame(clm)
    Top_Trans_Pincode.dropna(inplace=True)
    # print(Top_Trans_Pincode.isnull().sum())
    return Top_Trans_Pincode


def top_user_district():
    path = "PhonePe_data/data/top/user/country/india/state/"
    # Top_state_list--> to get the list of states in India
    Top_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'District': [],
           'Registered_Users': []}
    for i in Top_state_list:
        p_i = path + i + "/"
        Top_yr = os.listdir(p_i)
        for j in Top_yr:
            p_j = p_i + j + "/"
            Top_yr_list = os.listdir(p_j)
            for k in Top_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                for z in D['data']['districts']:
                    district = z['name']
                    registeredUsers = z['registeredUsers']
                    clm['District'].append(district)
                    clm['Registered_Users'].append(registeredUsers)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created a Top User District wise dataframe
    Top_User_District = pd.DataFrame(clm)
    # print(Top_User_District.isnull().sum())
    return Top_User_District


def top_user_pincode():
    path = "PhonePe_data/data/top/user/country/india/state/"
    # Top_state_list--> to get the list of states in India
    Top_state_list = os.listdir(path)

    # This is to extract the data's to create a dataframe
    clm = {'State': [],
           'Year': [],
           'Quarter': [],
           'Pincode': [],
           'Registered_Users': []}
    for i in Top_state_list:
        p_i = path + i + "/"
        Top_yr = os.listdir(p_i)
        for j in Top_yr:
            p_j = p_i + j + "/"
            Top_yr_list = os.listdir(p_j)
            for k in Top_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                D = json.load(Data)
                for z in D['data']['pincodes']:
                    pincode = z['name']
                    registeredUsers = z['registeredUsers']
                    clm['Pincode'].append(pincode)
                    clm['Registered_Users'].append(registeredUsers)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))

    # Successfully created a Top User Registered Pincode dataframe
    Top_User_Pincode = pd.DataFrame(clm)
    # print(Top_User_Pincode.isnull().sum())
    return Top_User_Pincode


def dataframe_to_mysql(df, table_name):
    """ Inserts the pandas DataFrame into connected MySql Database
        Input Parameters: DataFrame, Table Name"""
    df.to_sql(name=table_name, con=mydb, if_exists='replace', index=False)


def insert_into_mysql(table_name_, dataframe, conn):
    """Loads a DataFrame 'dataframe' with table name 'table_name' into mysql database
       using connection details in conn"""
    query = conn.cursor()
    cols = ','.join(list(dataframe.columns))
    for i, row in dataframe.iterrows():
        query.execute(f"INSERT INTO {table_name_} ({cols}) VALUES {tuple(row)}")
        conn.commit()


def sql_table_creation(conn):
    """ Creates the respective tables in the established database connection conn  """
    table_query = conn.cursor()
    query_agg_trans = f"CREATE TABLE if not exists agg_trans (State varchar(255), Year varchar(255), Quarter varchar(255)," \
                      f" Transaction_Type varchar(255), Transaction_Count int, Transaction_Amount bigint)"
    query_agg_user_state_reg = f"CREATE TABLE if not exists agg_user_state_reg (State varchar(255), Year varchar(255), Quarter varchar(255)," \
                               f" Registered_Users int, App_Opens bigint)"
    agg_user_state_brand = f"CREATE TABLE if not exists agg_user_state_brand (State varchar(255), Year varchar(255), Quarter varchar(255)," \
                           f" Brand varchar(255), Registration_Count int, Registration_Percentage int)"
    map_trans = f"CREATE TABLE if not exists map_trans (State varchar(255), Year varchar(255), Quarter varchar(255)," \
                f" District varchar(255), Transaction_Count int, Transaction_Amount bigint)"
    map_user = f"CREATE TABLE if not exists map_user (State varchar(255), Year varchar(255), Quarter varchar(255)," \
               f" District varchar(255), Registered_Users bigint, App_Opens bigint)"
    top_trans_district = f"CREATE TABLE if not exists top_trans_district (State varchar(255), Year varchar(255), Quarter varchar(255)," \
                         f" District varchar(255), Transaction_Count int, Transaction_Amount bigint)"
    top_trans_pincode = f"CREATE TABLE if not exists top_trans_pincode (State varchar(255), Year varchar(255), Quarter varchar(255)," \
                              f" Pincode int, Transaction_Count int, Transaction_Amount bigint)"
    top_user_district = f"CREATE TABLE if not exists top_user_district (State varchar(255), Year varchar(255), Quarter varchar(255)," \
                        f" District varchar(255), Registered_Users bigint)"
    top_user_pincode = f"CREATE TABLE if not exists top_user_pincode (State varchar(255), Year varchar(255), Quarter varchar(255)," \
                       f" Pincode int, Registered_Users bigint)"
    table_query.execute(query_agg_trans)
    table_query.execute(query_agg_user_state_reg)
    table_query.execute(agg_user_state_brand)
    table_query.execute(map_trans)
    table_query.execute(map_user)
    table_query.execute(top_trans_district)
    table_query.execute(top_trans_pincode)
    table_query.execute(top_user_district)
    table_query.execute(top_user_pincode)
    table_query.close()


# ----------------------------------- Main ---------------------------------------------------------
create_database(database_name='PhonePe')
mydb = connect_database()
sql_table_creation(mydb)

insert_into_mysql(table_name_="agg_trans", dataframe=agg_trans(), conn=mydb)
insert_into_mysql(table_name_="agg_user_state_reg", dataframe=agg_user_state_reg(), conn=mydb)
insert_into_mysql(table_name_="agg_user_state_brand", dataframe=agg_user_state_brand(), conn=mydb)
insert_into_mysql(table_name_="map_trans", dataframe=map_trans(), conn=mydb)
insert_into_mysql(table_name_="map_user", dataframe=map_user(), conn=mydb)
insert_into_mysql(table_name_="top_trans_district", dataframe=top_trans_district(), conn=mydb)
insert_into_mysql(table_name_="top_trans_pincode", dataframe=top_trans_pincode(), conn=mydb)
insert_into_mysql(table_name_="top_user_district", dataframe=top_user_district(), conn=mydb)
insert_into_mysql(table_name_="top_user_pincode", dataframe=top_user_pincode(), conn=mydb)
mydb.close()

