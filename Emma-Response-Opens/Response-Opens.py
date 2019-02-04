import pyodbc
import requests
import datetime

#print time
datetime.time(15, 8, 24)
print("Script began at: ", datetime.datetime.now().time())

#connect to server and db 
cnxn = pyodbc.connect("Driver={SQL Server};"
                        "Server=devsql01;"
                        "Database=MyEmma;"
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
    
    arr = ['369742878', '369743902', '370086942', '370087966', '370371614', '370372638', '370389022', '370768926', '370904094', '371483678', '371484702', '372239390', '372240414', '372241438', '372242462', '372248606', '373028894', '373029918', '373030942', '373031966', '373032990', '373034014', '373035038', '373066782', '373368862', '373369886', '373448734', '373451806', '373926942', '373971998', '374088734', '374089758', '374140958', '374377502', '374440990', '374442014', '374443038', '374444062', '374447134', '374448158', '374449182', '374450206', '374582302', '374732830', '374735902', '374751262', '374758430', '374759454', '374760478', '374761502', '374762526', '374763550', '374803486', '374810654', '374814750', '374851614', '374863902', '374949918', '374955038', '375134238', '375170078', '375192606', '375257118', '375297054', '375309342', '375310366', '375312414', '375345182', '375346206', '375373854', '375398430', '375413790', '375414814', '375536670', '375537694', '375538718', '375583774', '375623710', '375654430', '375655454', '375687198', '375688222', '375689246', '375690270', '375692318', '375706654', '375707678', '375708702', '375820318', '375937054', '375944222', '375945246', '375946270', '375947294', '376055838', '376056862', '376069150', '376070174', '376071198', '376162334', '376163358', '376205342', '376206366', '376476702', '376477726', '376531998', '376533022', '376534046', '376535070', '377205790', '377923614', '377949214', '377950238', '377951262', '377952286', '378186782', '378187806', '378294302', '378295326', '378388510', '378389534', '379057182', '379067422']
    
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