import sys           # Read system parameters.
import pandas as pd  # Manipulate and analyze data.
import sqlite3       # Manage SQL databases.


######################Extract Data######################

complaints_data = pd.read_csv('consumer_loan_complaints.csv')
#print(complaints_data.head(n = 3))


conn = sqlite3.connect('user_data.db')
conn

# Write a query that selects everything from the users table.
query = 'SELECT * FROM users'
# Read the query into a DataFrame.
users = pd.read_sql(query, conn)
# Preview the data.
#print(users.head());
# Check the shape of the data.
#print(users.shape)

#--------------------------------------------------------------------------


query = 'SELECT * FROM device'
device = pd.read_sql(query, conn)
#print(device.head())
#print(device.shape)



#--------------------------------------------------------------------------


# Read the user transactions in the last 30 days. 
query = 'SELECT * FROM transactions'
transactions = pd.read_sql(query, conn)
#print(transactions.head())
#print(transactions.shape)



#--------------------------------------------------------------------------


# Aggregate data on the number of transactions and the total amount.
query = '''SELECT user_id, 
                  COUNT(*) AS number_transactions, 
                  SUM(amount_usd) AS total_amount_usd 
           FROM transactions 
           GROUP BY user_id'''

transactions_agg = pd.read_sql(query, conn)
#print(transactions_agg.head())
#print(transactions_agg.shape)



#--------------------------------------------------------------------------


# Do a left join, as all users in the users table are of interest.
query = '''SELECT left_table.*,           
                  right_table.device       
           FROM users AS left_table         
           LEFT JOIN device AS right_table   
             ON left_table.user_id = right_table.user_id'''

users_w_device = pd.read_sql(query, conn)
#print(users_w_device.head(n = 3))
#print(users_w_device.shape)


conn.close()

#--------------------------------------------------------------------------

######################Transform Data######################


# Do a right join so users won't be lost.
users_w_devices_and_transactions = \
transactions_agg.merge(users_w_device,
                       on = 'user_id', how = 'right')

#print(users_w_devices_and_transactions.head())
#print(users_w_devices_and_transactions.shape)



#--------------------------------------------------------------------------



#Identify data where age is greater than 150
#print(users_w_devices_and_transactions[users_w_devices_and_transactions.age > 150])



#--------------------------------------------------------------------------


#Drop incorrect data
users_cleaned = \
users_w_devices_and_transactions[users_w_devices_and_transactions.age < 150]

#print(users_cleaned.shape)

#print(users_cleaned.info())
#print(users_cleaned.default.value_counts())
#print(users_cleaned.device.value_counts())

#--------------------------------------------------------------------------

#Identify more potentially erroneous data
#print(pd.crosstab(users_cleaned['age'], users_cleaned['device']))


crosstab_result = pd.crosstab(users_cleaned['age'], users_cleaned['device']);

device_counts = crosstab_result.sum(axis=1)

min_age = device_counts.idxmin()
max_age = device_counts.idxmax()

min_count = device_counts.min()
max_count = device_counts.max()

#print(f"Age with the minimum total device usage: {min_age} ({min_count} devices)")
#print(f"Age with the maximum total device usage: {max_age} ({max_count} devices)")

# Device type data based on minimum and maximum age
devices_min_age = crosstab_result.loc[min_age]
devices_max_age = crosstab_result.loc[max_age]

# Types of devices with usage count for minimum and maximum age
#print(f"Device types for age {min_age} (minimum usage):\n{devices_min_age}")
#print(f"\nDevice types for age {max_age} (maximum usage):\n{devices_max_age}")




""""
df_min = devices_min_age.reset_index().rename(columns={min_age: "Count"}).sort_values(by="Count", ascending=False)
df_max = devices_max_age.reset_index().rename(columns={max_age: "Count"}).sort_values(by="Count", ascending=False)

print("\nDevices based on age with minimum usage:")
print(df_min)

print("\nDevices based on age with maximum usage:")
print(df_max)
"""

#--------------------------------------------------------------------------



#Convert the relevant variables to a Boolean type
users_cleaned_1 = users_cleaned.copy()  # Work with a new object.

users_cleaned_1.default = \
users_cleaned_1.default.map(dict(yes = 1, no = 0)).astype(bool)

#print(users_cleaned_1.default.value_counts())
#print(users_cleaned_1.head())
#print(users_cleaned_1["default"].head())


#--------------------------------------------------------------------------

# Do the same for the other Boolean variables.
bool_vars = ['housing', 'loan', 'term_deposit']
for var in bool_vars:
    users_cleaned_1[var] = \
    users_cleaned_1[var].map(dict(yes = 1, no = 0)).astype(bool)
    #print(f'Converted {var} to Boolean.')

#print(users_cleaned_1.info())


#Convert date_joined to a datetime format
users_cleaned_2 = users_cleaned_1.copy()  # Work with a new object.

users_cleaned_2['date_joined'] = \
pd.to_datetime(users_cleaned_2['date_joined'],
               format = '%Y-%m-%d')

#print(users_cleaned_1['date_joined'].head())
#print(users_cleaned_2['date_joined'].head())
#print(users_cleaned_1.info())
#print(users_cleaned_2.info())

#--------------------------------------------------------------------------


#Identify all duplicated data
duplicated_data = \
users_cleaned_2[users_cleaned_2.duplicated(keep = False)]

duplicated_data = \
duplicated_data.sort_values(by = list(duplicated_data.columns))



""""
print('Number of rows with duplicated data:',
      duplicated_data.shape)

print('Number of rows with duplicated data:',
      duplicated_data.shape[0])

print(duplicated_data)
"""

#Remove the duplicated data
users_cleaned_final = \
users_cleaned_2[~users_cleaned_2.duplicated()]

#print(users_cleaned_2[users_cleaned_2['user_id'] == "cba59442-af3c-41d7-a39c-0f9bffba0660"])
#print(users_cleaned_final[users_cleaned_final['user_id'] == "cba59442-af3c-41d7-a39c-0f9bffba0660"])

#print(users_cleaned_2.shape)
#print(users_cleaned_final.shape)


#--------------------------------------------------------------------------


######################Load Data into a Destination######################

#Load data into an SQL database
conn = sqlite3.connect('users_data_cleaned.db')

users_cleaned_final.to_sql('users_cleaned_final',
                           conn,
                           if_exists = 'replace',
                           index = False)

query = 'SELECT * FROM users_cleaned_final'

datafinal = pd.read_sql(query, conn).head()
#print(datafinal.head())
conn.close()


#--------------------------------------------------------------------------

#Write the DataFrame as a pickle file
users_cleaned_final.to_pickle('users_data_cleaned.pickle')

#Confirm that the data was written to the pickle file
#pd.read_pickle('users_data_cleaned.pickle').head()
#pd.read_pickle('users_data_cleaned.pickle').info()


#--------------------------------------------------------------------------


#Write the data to a CSV file
users_cleaned_final.to_csv('users_data_cleaned.csv',
                           index = False)

#Confirm that the data was written to a CSV file.
pd.read_csv('users_data_cleaned.csv').head()
pd.read_csv('users_data_cleaned.csv').info()


#--------------------------------------------------------------------------