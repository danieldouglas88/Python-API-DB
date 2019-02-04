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
    api = requests.get('https://api.e2ma.net/' + accountId + '/response', auth=(pubApi, privApi))
    api= api.json()
    
    #iterate through JSON object
    for x in api:   
        #insert parsed JSON values into DB and pass params for stored procedure
        query = "exec MyemmaResponse @count_purchased = ?, @delivered = ?, @clicked = ?, @account_id = ?, @clicked_unique = ?, @shared = ?, @mailings = ?, @year = ?, @month = ?, @opened = ?, @opted_out = ?, @sent = ?, @signed_up = ?, @webview_shared = ?, @share_click = ?, @bounced = ?, @webview_share_clicked = ?, @sum_purchased = ?, @forwarded = ?"
        values = (x['count_purchased'], x['delivered'], x['clicked'], x['account_id'], x['clicked_unique'], x['shared'], x['mailings'], x['year'], x['month'], x['opened'], x['opted_out'], x['sent'], x['signed_up'], x['webview_shared'], x['share_clicked'], x['bounced'], x['webview_share_clicked'], x['sum_purchased'], x['forwarded'])
        cursor.execute(query, values)            
        cnxn.commit()

#new function which passes params and calls other function for each store/brand with the necessary exception handle in place
def funcCall():
    try:
        apiConnect('', '', '')
    except:
        print("Store failed at: ", datetime.datetime.now().time())
              
    try:
        apiConnect('', '', '')
    except:
        print("Store failed at: ", datetime.datetime.now().time())
        

#function call
funcCall()

#print time
print("Script finished running at: ", datetime.datetime.now().time())

