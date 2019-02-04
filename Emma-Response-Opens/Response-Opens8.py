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
    
    arr = ['85385240', '85389336', '85393432', '85402648', '85503000', '85504024', '85524504', '85633048', '85634072', '85635096', '85761048', '85779480', '85781528', '85782552', '85783576', '85784600', '85786648', '85787672', '85789720', '85790744', '85791768', '85792792', '85809176', '85973016', '85974040', '86009880', '86010904', '86165528', '86171672', '86182936', '86183960', '86249496', '86250520', '86254616', '86338584', '86339608', '86463512', '86719512', '86720536', '86764568', '86925336', '86926360', '86927384', '86964248', '86965272', '87023640', '87041048', '87057432', '87068696', '87104536', '87108632', '87555096', '87752728', '87753752', '87754776', '87755800', '87756824', '87989272', '88055832', '88191000', '88192024', '88303640', '88306712', '88330264', '88403992', '88419352', '88436760', '88450072', '88510488', '88523800', '88686616', '88687640', '88688664', '88724504', '88762392', '88774680', '88794136', '88798232', '88880152', '88885272', '88887320', '88941592', '88950808', '89015320', '89016344', '89017368', '89018392', '89216024', '89217048', '89218072', '89287704', '89299992', '89314328', '89321496', '89334808', '89387032', '89388056', '89391128', '89400344', '89401368', '89403416', '89432088', '89433112', '89643032', '89644056', '89693208', '89694232', '89695256', '89696280', '89785368', '89786392', '89799704', '89800728', '89809944', '89810968', '89811992', '89896984', '89901080', '89902104', '90193944', '90195992', '90197016', '90198040', '90199064', '90200088', '90201112', '90202136', '90203160', '90204184', '90205208', '90401816', '90402840', '90403864', '90404888', '90565656', '90566680', '90996760', '90997784', '91029528', '91129880', '91144216', '91145240', '91146264', '91148312', '91266072', '91267096', '91874328', '91875352', '91876376', '91877400', '92651544', '93305880', '93306904', '93337624', '94341144', '94374936', '94945304', '95596568', '95597592']
    
    for j in arr:

        try:
            apiConnect('', '', '', start, end, j)
        except Exception as e:
            print("Error Message: ", e)    
    

    arr = ['434240479', '434533343', '434534367', '435173343', '435709919', '437547999', '438142943', '438149087', '439936991', '443342815', '455390175', '455964639', '463471583', '463528927', '464569311', '465239007', '466573279', '468550623', '468551647', '470652895', '471900127', '472919007', '472923103', '472924127', '474525663', '475558879', '475560927', '475962335', '476500959', '476697567', '476782559', '476788703', '477936607', '480446431', '480717791', '480790495', '483243999', '483419103', '483901407', '484778975', '484839391', '484840415', '485781471', '485794783', '485795807', '485799903', '487067615', '487069663', '487684063', '488592351', '490324959', '490799071', '490871775', '490872799', '493137887', '493974495', '434206687', '436528095', '436531167', '436555743', '437115871', '440170463', '440171487', '443408351', '444984287', '445256671', '449768415', '463384543', '463387615', '465237983', '466575327', '470648799', '470651871', '472925151', '474483679', '475399135', '475541471', '475547615', '475559903', '476503007', '476748767', '476752863', '476779487', '477184991', '479115231', '479116255', '479118303', '479119327', '480304095', '480410591', '480441311', '480445407', '480784351', '480809951', '480819167', '481523679', '481535967', '481542111', '481581023', '483616735', '483895263', '484134879', '484389855', '484783071', '484784095', '485796831', '485800927', '486332383', '488595423', '488697823', '491323359', '492185567', '434850783', '435167199', '435170271', '435247071', '437768159', '438146015', '438330335', '438332383', '439302111', '439933919', '439942111', '440172511', '444980191', '445247455', '457105375', '463529951', '465124319', '466679775', '470650847', '470665183', '471905247', '472926175', '475553759', '476495839', '476771295', '479949791', '481080287', '483245023', '483246047', '483375071', '483418079', '483902431', '484786143', '485798879', '486333407', '487071711', '488593375', '491020255', '491022303', '493353951', '493976543', '434207711', '434430943', '435168223', '436526047', '437038047', '437875679', '438148063', '438150111', '439943135', '439946207', '443459551', '443468767', '443580383', '444983263', '444985311', '445104095', '445248479', '446861279', '455389151', '457104351', '463385567', '463386591', '463522783', '466574303', '467332063', '470654943', '474482655', '474714079', '476497887', '477083615', '477290463', '477937631', '479117279', '479596511', '480439263', '480440287', '481543135', '481544159', '483374047', '483903455', '484782047', '484836319', '484845535', '485033951', '485034975', '485035999', '485230559', '487070687', '487683039', '488696799', '491021279', '493975519']
    
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