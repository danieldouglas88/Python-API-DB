import pyodbc
import requests
import datetime

#print time
datetime.time(15, 8, 24)
print("Script began at: ", datetime.datetime.now().time())

#connect to server and db 
cnxn = pyodbc.connect("Driver={SQL Server};"
                        "Server=;"
                        "Database=;"
                        "Trusted_Connection=yes;")

cursor = cnxn.cursor()
    
def apiConnect(pubApi, privApi, accountId):

    #pull in and authenticate API and convert into proper JSON format
    api = requests.get('https://api.e2ma.net/' + accountId + '/groups', auth=(pubApi, privApi))
    api= api.json()
    
    #iterate through JSON object
    for x in api:   
    
        #insert parsed JSON values into DB and pass params for stored procedure
        query = "exec MyemmaGroups @active_count = ?, @deleted_at = ?, @error_count = ?, @optout_count = ?, @group_type = ?, @member_group_id = ?, @purged_at = ?, @account_id = ?, @group_name = ?, @date_time = ?"
        values = (	x['active_count'], x['deleted_at'], x['error_count'], x['optout_count'], x['group_type'], x['member_group_id'], x['purged_at'], x['account_id'], x['group_name'], '')
        cursor.execute(query, values)            
        cnxn.commit()

#new function which passes params and calls other function for each store/brand with the necessary exception handle in place
def funcCall():
    try:
        apiConnect('', '', '')
    except Exception as e: 
        print("STORE FAILED. Error Message: ", e)
              
#function call
funcCall()

#print time
print("Script finished running at: ", datetime.datetime.now().time())

