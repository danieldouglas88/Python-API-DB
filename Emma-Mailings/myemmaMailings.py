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
    
def apiConnect(pubApi, privApi, accountId, start, end):

    #pull in and authenticate API and convert into proper JSON format
    api = requests.get('https://api.e2ma.net/' + accountId + '/mailings?, start=' + start + '&end=' + end, auth=(pubApi, privApi))
    api= api.json()
    
    #iterate through JSON object
    for x in api:   
        try:
            SendStarted = x['send_started'].replace('@D:', ' ')
            SendStarted = SendStarted.replace('T', ' ')
        except:
            SendStarted = None
            
        try:
            StartOrFinished = x['started_or_finished'].replace('@D:', ' ')
            StartOrFinished = StartOrFinished.replace('T', ' ')
        except:
            StartOrFinished = None
            
        try:
            CreatedTs = x['created_ts'].replace('@D:', ' ')
            CreatedTs = CreatedTs.replace('T', ' ')
        except:
            CreatedTs = None
            
        try:
            SendFinished = x['send_finished'].replace('@D:', ' ')
            SendFinished = SendFinished.replace('T', ' ')
        except:
            SendFinished = None
        
        try:
            SendAt = x['send_at'].replace('@D:', ' ')
            SendAt = SendAt.replace('T', ' ')
        except:
            SendAt = None
            
        try:
            Sender = x['sender'].replace('"', '')
        except:
            Sender = None
            
        try:
            Name = x['name'].replace('_', ' ')
        except:
            Name = None
    
        #insert parsed JSON values into DB and pass params for stored procedure
        query = "exec MyemmaMailings @mailing_type = ?, @send_started = ?, @cancel_by_user_id = ?, @mailing_id = ?, @recipient_count = ?, @cancel_ts = ?, @mailing_status = ?, @account_id = ?, @monthCol = ?, @failure_ts = ?, @reply_to = ?, @yearCol = ?, @deleted_at = ?, @started_or_finished = ?, @subject = ?, @disabled = ?, @created_ts = ?, @sender = ?, @plaintext_only = ?, @name = ?, @hour = ?, @parent_mailing_id = ?, @failure_message = ?, @day = ?, @send_finished = ?, @datacenter = ?, @send_at = ?, @signup_form_id = ?, @purged_at = ?, @archived_ts = ?"
        values = (x['mailing_type'], SendStarted, x['cancel_by_user_id'], x['mailing_id'], x['recipient_count'], x['cancel_ts'], x['mailing_status'], x['account_id'], x['month'], x['failure_ts'], x['reply_to'], x['year'], x['deleted_at'], StartOrFinished, x['subject'], x['disabled'], CreatedTs, Sender, x['plaintext_only'], Name, x['hour'], x['parent_mailing_id'], x['failure_message'], x['day'], SendFinished, x['datacenter'], SendAt, x['signup_form_id'], x['purged_at'], x['archived_ts'])
        cursor.execute(query, values)            
        cnxn.commit()

#new function which passes params and calls other function for each store/brand with the necessary exception handle in place
def funcCall(start, end):
    try:
        apiConnect('', '', '' , start, end)
    except Exception as e: 
        print("STORE FAILED. Error Message: ", e)
              
    try:
        apiConnect('', '', '' , start, end)
    except Exception as e:
        print("STORE FAILED. Error Message: ", e)


#function call
funcCall('0', '5000')
funcCall('5000', '10000')
funcCall('10000', '15000')
funcCall('15000', '20000')
funcCall('20000', '25000')

#print time
print("Script finished running at: ", datetime.datetime.now().time())

