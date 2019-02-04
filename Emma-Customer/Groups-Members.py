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
    
def apiConnect(pubApi, privApi, accountId, start, end, membergroupid):

    #pull in and authenticate API and convert into proper JSON format
    api = requests.get('https://api.e2ma.net/' + accountId + '/groups/' + membergroupid + '/members?start=' + start + '&end=' + end, auth=(pubApi, privApi))
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
        query = "exec MembersSP @Status = ?, @Confirmed_Opt_In = ?, @Account_ID = ?, @StoreCode = ?, @FirstName = ?, @LastName = ?, @Eclub_Member = ?, @Email_Number = ?, @Eclub = ?, @Last_Input_Source = ?, @Fishbowl_Join_Date = ?, @Px_Or_Ot_Joindate = ?, @Ot_Signup = ?, @Member_ID = ?, @Last_Modified_At = ?, @Member_Status_ID = ?, @Plaintext_Preferred = ?, @Email_Error = ?, @Member_Since = ?, @Bounce_Count = ?, @Deleted_At =?, @Email = ?, @PxCardNumber = ?, @Member_Group_ID = ?"
        values = (x['status'], x['confirmed_opt_in'], x['account_id'], storeCode, firstName, lastName,
        eClubMember, emailNumber, eClub, lastInputSource, fishbowlJoinDate, pxOrOtJoindate,otSignup, x['member_id'],
        lastModAt, x['member_status_id'], x['plaintext_preferred'], x['email_error'],
        memberSince, x['bounce_count'], x['deleted_at'], x['email'], pxCardNumber, membergroupid)
        cursor.execute(query, values)            
        cnxn.commit()


#new function which passes params and calls other function for each store/brand with the necessary exception handle in place
def funcCall(start, end):
    
    arr = ['5112862', '5127198', '5134366', '5160990', '5162014', '5171230', '5349406', '5440542', '5500958', '5524510', '5525534', '5526558', '5527582', '5528606', '5529630', '5531678']
    for j in arr:

        try:
            apiConnect('', '', '', start, end, j)
        except Exception as e:
            print("Error Message: ", e)


#function call
funcCall('0','500')
funcCall('500','1000')
funcCall('1000','1500')
funcCall('1500','2000')
funcCall('2000','2500')
funcCall('2500','3000')
funcCall('3000','3500')
funcCall('3500','4000')
funcCall('4000','4500')
funcCall('4500','5000')
funcCall('5000','5500')
funcCall('5500','6000')
funcCall('6000','6500')
funcCall('6500','7000')
funcCall('7000','7500')
funcCall('7500','8000')
funcCall('8000','8500')
funcCall('8500','9000')
funcCall('9000','9500')
funcCall('9500','10000')


#print time
print("Script finished running at: ", datetime.datetime.now().time())

