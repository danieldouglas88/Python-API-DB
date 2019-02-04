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
    
    arr = ['49116187', '49117211', '49159195', '49160219', '49168411', '49169435', '49174555', '49178651', '49179675', '49381403', '49383451', '49460251', '49461275', '49519643', '49520667', '49602587', '49603611', '49624091', '49625115', '50112539', '50113563', '50114587', '50115611', '51050523', '51051547', '51083291', '51084315', '51129371', '51130395', '51163163', '51164187', '51165211', '51166235', '51579931', '51580955', '52111387', '52112411', '52937755', '54016027', '54017051', '54075419', '54076443', '54340635', '54341659', '54347803', '54348827', '54411291', '55464987', '55466011', '55467035', '55483419', '55484443', '55486491', '55643163', '55644187', '56682523', '56683547', '56704027', '56705051', '56749083', '56750107', '56845339', '56847387', '56863771', '56864795', '57204763', '57205787', '57223195', '57224219', '57267227', '57268251', '57277467', '57278491', '57670683', '57673755', '57721883', '57722907', '57744411', '57745435', '57857051', '57858075', '58303515', '58304539', '58305563', '58306587', '58307611', '58308635', '58440731', '58443803', '58447899', '58448923', '58499099', '58500123', '58972187', '58973211', '58974235', '58975259', '58976283', '58977307', '58978331', '58979355', '59061275', '59062299', '59063323', '59064347', '59469851', '59472923', '59474971', '59475995', '59748379', '59749403', '59750427', '59752475', '59753499', '59754523', '60343323', '60344347', '60345371', '60346395', '60502043', '60503067', '60504091', '60505115', '60627995', '60629019', '60630043', '60631067', '61352987', '61354011', '62355483', '62356507', '62365723', '62366747', '62507035', '62508059', '62509083', '62510107', '62625819', '62626843', '62627867', '62628891', '63593499', '63594523', '63619099', '63620123']
    
    for j in arr:

        try:
            apiConnect('', '', '', start, end, j)
        except Exception as e:
            print("Error Message: ", e)    
            
    arr = ['315801626', '316506138', '318220314', '318221338', '318222362', '319240218', '319241242', '319365146', '319872026', '319938586', '320031770', '320033818', '320354330', '320392218', '320393242', '320394266', '320524314', '320686106', '320689178', '320861210', '320891930', '321254426', '321319962', '321636378', '321637402', '321638426', '321682458', '321684506', '321715226', '321804314', '321930266', '321935386', '321940506', '322011162', '322012186', '322013210', '322014234', '322017306', '322018330', '322109466', '322110490', '322111514', '322112538', '322113562', '322114586', '322165786', '322205722', '322206746', '322207770', '322215962', '322216986', '322241562', '322243610', '322259994', '322261018', '322463770', '322464794', '322465818', '322839578', '322840602', '322843674', '322974746', '322975770', '322997274', '323706906', '323707930']
    
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