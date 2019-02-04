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
    
def apiConnect(pubApi, privApi, accountId, start, end, mailingid):

    #pull in and authenticate API and convert into proper JSON format
    api = requests.get('https://api.e2ma.net/' + accountId + '/response/' + mailingid + '/opens?start=' + start + '&end=' + end, auth=(pubApi, privApi))
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
            lastInputSource = x['fields']['last-input-source']
        except: 
            lastInputSource = None

        try:
            fishbowlJoinDate = x['fields']['fishbowl-join-date'].replace("@D:","")
        except:
            fishbowlJoinDate = None

        try:
            pxOrOtJoindate = x['fields']['px-or-ot-joindate'].replace("@D:","")
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
            dateLastDined = x['fields']['datelastdined'].replace("@D:","")
        except:
            dateLastDined = None
            
        try:
            birthDay = x['fields']['birthdate'].replace("@D:","")
        except:
            birthDay = None
            
        try:
            memberSince = x['member_since'].replace("@D:","")
            memberSince = memberSince.replace("T"," ")
        except:
            memberSince = None
            
        try:
            timeStamp = x['timestamp'].replace("@D:","")
            timeStamp = timeStamp.replace("T"," ")
        except:
            timeStamp = None
            
        
        #insert parsed JSON values into DB and pass params for stored procedure
        query = "exec MyemmaResponseOpens @storecode = ?, @firstname = ?, @lastname = ?, @px_or_ot_joindate = ?, @datelastdined = ?, @pxcardnumber = ?, @birthdate = ?, @fishbowl_join_date = ?, @ot_signup = ?, @last_input_source = ?, @timestamp = ?, @member_id = ?, @member_since = ?, @email_domain = ?, @email_user = ?, @email = ?, @member_status_id = ?, @mailing_id = ?"
        values = (storeCode, firstName, lastName, pxOrOtJoindate, dateLastDined, pxCardNumber, birthDay, fishbowlJoinDate, otSignup, lastInputSource, timeStamp, x['member_id'], memberSince, x['email_domain'], x['email_user'], x['email'], x['member_status_id'], mailingid)
        cursor.execute(query, values)            
        cnxn.commit()


#new function which passes params and calls other function for each store/brand with the necessary exception handle in place
def funcCall(start, end):
    
    arr = ['164412449', '164413473', '164414497', '164426785', '164427809', '164680737', '164741153', '164742177', '165046305', '165144609', '165976097', '165978145', '165979169', '165981217', '166022177', '166023201', '166082593', '166083617', '166086689', '166087713', '166165537', '166391841', '166462497', '166520865', '166574113', '166985761', '167003169', '167004193', '167032865', '167555105', '167556129', '169234465', '169314337', '170130465', '170149921', '170151969', '170256417', '170258465', '170547233', '171257889', '171258913', '171259937', '173417505', '173648929', '173926433', '173930529', '174516257', '174563361', '174603297', '176287777', '176288801', '176933921', '177314849', '177478689', '177722401', '177779745', '177980449', '178977825', '178979873', '178980897', '178984993', '179750945', '179760161', '180069409', '180070433', '180071457', '180074529', '180076577', '180077601', '180078625', '180080673', '181296161', '182029345', '182157345', '182397985', '182859809', '182877217', '182934561', '183073825', '183127073', '183167009', '183168033', '183458849', '183482401', '183508001', '183623713', '184793121', '184894497', '185044001', '185051169', '185216033', '185353249', '185401377', '185419809', '186116129', '186435617', '186437665', '188001313', '188003361', '188004385', '188238881', '188344353', '188494881', '188708897', '188903457', '190059553', '190060577', '190061601', '190204961', '190205985', '190207009', '190208033', '190467105', '190468129', '191824929', '191826977', '191828001', '191829025', '191917089', '191918113', '192011297', '192012321', '192298017', '192359457', '192360481', '192635937', '192636961', '193983521', '193984545', '194265121', '194267169', '194295841', '194573345', '194574369', '196565025', '196566049', '199718945', '199720993', '200688673', '200689697', '201725985', '201796641']
    
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