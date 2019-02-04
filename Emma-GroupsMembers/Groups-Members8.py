import pyodbc
import requests
import datetime
import time


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
            
        try:
            henrysprefer = x['fields']['henrys-tavern-preferred-location']
        except:
            henrysprefer = None
            
        try:
            palominoprefer = x['fields']['palomino-preferred-location']
        except:
            palominoprefer = None
            
        try:
            kincaidsprefer = x['fields']['kincaids-preferred-location']
        except:
            kincaidsprefer = None
            
        try:
            pscprefer = x['fields']['portland-seafood-preferred-location']
        except:
            pscprefer = None
            
        try:
            stanfordsprefer = x['fields']['stanfords-preferred-location']
        except:
            stanfordsprefer = None
            
        try:  
            portland_or = x['fields']['portland-or']
        except:
            portland_or = None
            
        try:
            bellevue_wa = x['fields']['bellevue-wa']
        except:
            bellevue_wa = None
            
        try:
            plano_tx = x['fields']['plano-tx']
            
        except:
            plano_tx = None
            
        try:
            denver_co = x['fields']['denver-co']
        except:
            denver_co = None
            
        try:
            pdx_airport_or = x['fields']['pdx-airport-or']
        except:
            pdx_airport_or = None
                
        try:
            seattle_wa = x['fields']['seattle-wa']
        except:
            seattle_wa = None
        
        try:
            st_paul = x['fields']['st-paul']
        except:
            st_paul = None
            
        try:
            oakland = x['fields']['oakland']
        except:
            oakland = None
            
        try:
            bloomington = x['fields']['bloomington']
        except:
            bloomington = None
        
        try:
            redondo_beach = x['fields']['redondo-beach']
        except:
            redondo_beach = None
            
        try:
            burlingame = x['fields']['burlingame']
        except:
            burlingame = None
            
        try:
            bellevue = x['fields']['bellevue']
        except: 
            bellevue = None
            
        try:    
            seattle = x['fields']['seattle']
        except:
            seattle = None
            
        try:
            los_angeles = x['fields']['los-angeles']
        except:
            los_angeles = None
            
        try:
            indianapolis = x['fields']['indianapolis']
        except:
            indianapolis = None
            
        try:
            san_francisco = x['fields']['san-francisco']
        except:
            san_francisco = None
        
        try:
            tigard = x['fields']['tigard']
        except:
            tigard = None
        
        try:
            portland_washington = x['fields']['portland-washington']
        except:
            portland_washington = None
        
        try:
            walnut_creek = x['fields']['walnut-creek']
        except:
            walnut_creek = None
        
        try:
            seattle = x['fields']['seattle']
        except:
            seattle = None
            
        try:
            portland_066 = x['fields']['portland-066']
        except:
            portland_066 = None
            
        try:
            portland_082 = x['fields']['portland-082']
        except:
            portland_082 = None
            
        try:
            hillsboro = x['fields']['hillsboro']
        except:
            hillsboro = None
        
        try:
            lake_oswego = x['fields']['lake-oswego']
        except:
            lake_oswego = None
            
        try:
            clackamas = x['fields']['clackamas']
        except:
            clackamas = None
            
        try:  
            tukwila = x['fields']['tukwila']
        except:
            tukwila = None

        #insert parsed JSON values into DB and pass params for stored procedure
        query = "exec MembersSP @Status = ?, @Confirmed_Opt_In = ?, @Account_ID = ?, @StoreCode = ?, @FirstName = ?, @LastName = ?, @Eclub_Member = ?, @Email_Number = ?, @Eclub = ?, @Last_Input_Source = ?, @Fishbowl_Join_Date = ?, @Px_Or_Ot_Joindate = ?, @Ot_Signup = ?, @Member_ID = ?, @Last_Modified_At = ?, @Member_Status_ID = ?, @Plaintext_Preferred = ?, @Email_Error = ?, @Member_Since = ?, @Bounce_Count = ?, @Deleted_At =?, @Email = ?, @PxCardNumber = ?, @Member_Group_ID = ?, @Preferred_Location_Henrys = ?, @Preferred_Location_Palomino = ?, @Preferred_Location_Kincaids = ?,@Preferred_Location_Psc = ?, @Preferred_Location_Stanfords = ?, @portland_or = ?, @bellevue_wa = ?, @plano_tx = ?, @denver_co = ?, @pdx_airport_or = ?, @seattle_wa = ?, @st_paul = ?, @oakland = ?, @bloomington = ?, @redondo_beach = ?, @burlingame = ?, @bellevue = ?, @seattle = ?, @los_angeles = ?, @indianapolis = ?, @san_francisco = ?, @tigard = ?, @portland_washington = ?, @walnut_creek = ?, @portland_066 = ?, @portland_082 = ?, @hillsboro = ?, @lake_oswego = ?, @clackamas = ?, @tukwila = ?"
        values = (x['status'], x['confirmed_opt_in'], x['account_id'], storeCode, firstName, lastName, eClubMember, emailNumber, eClub, lastInputSource, fishbowlJoinDate, pxOrOtJoindate,otSignup, x['member_id'], lastModAt, x['member_status_id'], x['plaintext_preferred'], x['email_error'], memberSince, x['bounce_count'], x['deleted_at'], x['email'], pxCardNumber, membergroupid, henrysprefer, palominoprefer, kincaidsprefer, pscprefer, stanfordsprefer, portland_or, bellevue_wa, plano_tx, denver_co, pdx_airport_or, seattle_wa, st_paul, oakland, bloomington, redondo_beach, burlingame, bellevue, los_angeles, indianapolis, san_francisco, tigard, portland_washington, walnut_creek, seattle, portland_066, portland_082, hillsboro, lake_oswego,clackamas, tukwila)
        cursor.execute(query, values)  
        cnxn.commit()


#new function which passes params and calls other function for each store/brand with the necessary exception handle in place
def funcCall(start, end):
            
    arr = ['3909663', '3954719', '4091935', '4575263', '4627487']
    for j in arr:

        try:
            apiConnect('', '', '', start, end, j)
        except Exception as e:
            print("Error Message: ", e)
        
               
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
funcCall('10000','10500')
funcCall('10500','11000')
funcCall('11000','11500')
funcCall('11500','12000')
funcCall('12000','12500')
funcCall('12500','13000')
funcCall('13000','13500')
funcCall('13500','14000')
funcCall('14000','14500')
funcCall('14500','15000')
funcCall('15000','15500')
funcCall('15500','16000')
funcCall('16000','16500')
funcCall('16500','17000')
funcCall('17000','17500')
funcCall('17500','18000')
funcCall('18000','18500')
funcCall('18500','19000')
funcCall('19000','19500')
funcCall('19500','20000')
funcCall('20000','20500')
funcCall('20500','21000')
funcCall('21000','21500')
funcCall('21500','22000')
funcCall('22000','22500')
funcCall('22500','23000')
funcCall('23000','23500')
funcCall('23500','24000')
funcCall('24000','24500')
funcCall('24500','25000')
funcCall('25000','25500')
funcCall('25500','26000')
funcCall('26000','26500')
funcCall('26500','27000')
funcCall('27000','27500')
funcCall('27500','28000')
funcCall('28000','28500')
funcCall('28500','29000')
funcCall('29000','29500')
funcCall('29500','30000')

#print time
print("Script finished running at: ", datetime.datetime.now().time())