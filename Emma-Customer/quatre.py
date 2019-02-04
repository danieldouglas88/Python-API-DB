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
    api = requests.get('https://api.e2ma.net/' + accountId + '/members?start=' + start +'&end=' + end, auth=(pubApi, privApi))
    api= api.json()
    
    #iterate through JSON object
    for x in api:

    #allocate parsed json value to variable, and if exception then give NULL value to variable
        try:
            storeCode = x['fields']['storecode']
        except:
            storeCode = None
            
        try:
            firstName = x['fields']['first_name']
        except:
            firstName = None

        try: 
            lastName = x['fields']['last_name']
        except:
            lastName = None

        try:
            eClubMember = x['fields']['e-club-member']
        except:
            eClubMember = None

        try:
            emailNumber =  x['fields']['email-number']
        except:
            emailNumber = None

        try: 
            eClub = x['fields']['e-club']
        except:
            eClub = None

        try:
            lastInputSource = x['fields']['last-input-source']
        except: 
            lastInputSource = None

        try:
            fishbowlJoinDate = x['fields']['fishbowl-join-date']
            fishbowlJoinDate = fishbowlJoinDate.replace("@D:","")
        except:
            fishbowlJoinDate = None

        try:
            pxOrOtJoindate = x['fields']['px-or-ot-joindate']
            pxOrOtJoindate = pxOrOtJoindate.replace("@D:","")
        except:
            pxOrOtJoindate = None

        try:
            otSignup = x['fields']['ot-signup']
        except:
            otSignup = None
            
        try:
            pxCardNumber = x['fields']['pxcardnumber']
        except:
            pxCardNumber = None

        try:
            lastModAt = x['last_modified_at'][3]+x['last_modified_at'][4]+x['last_modified_at'][5]+x['last_modified_at'][6]+x['last_modified_at'][7]+x['last_modified_at'][8]+x['last_modified_at'][9]+x['last_modified_at'][10]+x['last_modified_at'][11]+x['last_modified_at'][12]
        except:
            lastModAt = None

        try:
            memberSince = x['member_since'][3]+x['member_since'][4]+x['member_since'][5]+x['member_since'][6]+x['member_since'][7]+x['member_since'][8]+x['member_since'][9]+x['member_since'][10]+x['member_since'][11]+x['member_since'][12]
        except:
            memberSince = None

        #insert parsed JSON values into DB and pass params for stored procedure
        query = "exec MembersSP @Status = ?, @Confirmed_Opt_In = ?, @Account_ID = ?, @StoreCode = ?, @FirstName = ?, @LastName = ?, @Eclub_Member = ?, @Email_Number = ?, @Eclub = ?, @Last_Input_Source = ?, @Fishbowl_Join_Date = ?, @Px_Or_Ot_Joindate = ?, @Ot_Signup = ?, @Member_ID = ?, @Last_Modified_At = ?, @Member_Status_ID = ?, @Plaintext_Preferred = ?, @Email_Error = ?, @Member_Since = ?, @Bounce_Count = ?, @Deleted_At =?, @Email = ?, @PxCardNumber = ?"
        values = (x['status'], x['confirmed_opt_in'], x['account_id'], storeCode, firstName, lastName,
        eClubMember, emailNumber, eClub, lastInputSource, fishbowlJoinDate, pxOrOtJoindate,otSignup, x['member_id'],
        lastModAt, x['member_status_id'], x['plaintext_preferred'], x['email_error'],
        memberSince, x['bounce_count'], x['deleted_at'], x['email'], pxCardNumber)
        cursor.execute(query, values)            
        cnxn.commit()


#new function which passes params and calls other function for each store/brand with the necessary exception handle in place
def funcCall(start, end):
    try:
        apiConnect('', '', '', start, end)
    except:
        try:
            apiConnect('', '', '', start, end)
        except:
            print(" failed at: ", datetime.datetime.now().time())
        

#function call
funcCall('0','5000')
funcCall('5000','10000')
funcCall('10000','15000')
funcCall('15000','20000')
funcCall('20000','25000')
funcCall('25000','30000')
funcCall('30000','35000')
funcCall('35000','40000')
funcCall('40000','45000')
funcCall('45000','50000')
funcCall('50000','55000')
funcCall('55000','59000')


#print time
print("Script finished running at: ", datetime.datetime.now().time())

