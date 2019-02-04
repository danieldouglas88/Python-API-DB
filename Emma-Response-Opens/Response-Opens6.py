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
    
    arr = ['492213270', '492251158', '492252182', '492309526', '492310550', '492315670', '492814358', '492967958', '493591574', '493592598', '493594646', '493595670', '493596694', '493597718', '493650966', '493651990', '494101526', '494112790', '494143510', '494488598', '495230998', '495637526', '495681558', '495738902', '495739926', '495742998', '495836182', '495924246', '495925270', '495962134', '495984662', '495985686', '495986710', '495987734', '496067606', '496117782', '496124950', '496137238', '496185366', '496189462', '496227350', '496239638', '496424982', '496435222', '496634902', '496740374', '496748566', '496781334', '496786454', '496787478', '496790550', '496840726', '496844822', '497082390', '497083414', '497096726', '497105942', '497106966', '497453078', '497454102', '497455126', '497592342', '497600534', '497614870', '497939478', '497941526', '497944598', '497945622', '497983510', '497985558', '498352150', '498353174', '498469910', '498470934', '499259414', '499260438', '499481622', '499482646', '499623958', '499624982', '500473878', '501831702', '501832726', '501833750', '502025238', '502026262', '502028310', '502030358', '502056982', '502058006', '502237206', '502238230', '502706198', '503511062']
    
    for j in arr:

        try:
            apiConnect('', '', '', start, end, j)
        except Exception as e:
            print("Error Message: ", e)    
        
        
    arr = ['578537493', '579305493', '579306517', '579970069', '579972117', '587001877', '587002901', '587685909', '588389397', '588390421', '588394517', '589113365', '589115413', '597815317', '597816341', '608037909', '608741397', '609478677', '617398293', '639617045', '639618069', '643202069', '643932181', '644598805', '644600853', '644605973', '650333205', '650334229', '651202581', '651203605', '653779989', '653781013', '653784085', '657101845', '657102869', '657103893', '657109013', '657889301', '659518485', '659534869', '660358165', '660359189', '660372501', '660373525', '662098965', '662099989', '662108181', '666318869', '666319893', '667115541', '667982869', '667989013', '668811285', '668813333', '668819477', '671281173', '672117781', '672118805', '676878357', '676879381', '677634069', '678389781', '679179285', '680002581', '683223061', '683224085', '684891157', '684892181', '688437269', '688438293', '688440341', '688441365', '688442389', '688443413', '688472085', '688478229', '688479253', '689403925', '689404949', '690333717', '690334741', '691205141', '691206165', '692078613', '692079637', '695886869', '695887893', '695911445', '697813013', '697814037', '703337493', '713364501', '713365525', '714252309', '714253333', '714254357', '714255381', '715063317', '715064341', '716009493', '716010517', '719769621']
    
    for j in arr:

        try:
            apiConnect('', '', '', start, end, j)
        except Exception as e:
            print("Error Message: ", e)    
            
    
    arr =['430867291', '430891867', '430910299', '430911323', '430912347', '430934875', '430937947', '430941019', '430945115', '430946139', '431004507', '431012699', '431013723', '431073115', '431075163', '431092571', '431093595', '431094619', '431095643', '431118171', '431129435', '431165275', '431166299', '431168347', '431172443', '431173467', '431208283', '431212379', '431214427', '431221595', '431222619', '431227739', '431228763', '431229787', '431645531', '431678299', '431679323', '431680347', '431711067', '431948635', '431981403', '431983451', '431984475', '431985499', '432015195', '432116571', '432117595', '432118619', '432119643', '432120667', '432122715', '432123739', '432124763', '432217947', '432253787', '432285531', '432286555', '432531291', '432558939', '432563035', '432565083', '432623451', '432625499', '432627547', '432691035', '432692059', '432724827', '432758619', '432759643', '432761691', '432792411', '432793435', '432795483', '432796507', '432963419', '432964443', '432965467', '432995163', '432996187', '433062747', '433063771', '433068891', '433131355', '433333083', '433334107', '433335131', '433336155', '433337179', '434720603', '434721627', '434752347', '434786139', '434787163', '434788187', '434789211', '434790235', '434892635', '434893659', '434899803', '434900827', '434901851', '434902875', '435169115', '435170139', '435469147', '435470171', '436890459', '436891483', '436948827', '436949851', '437255003', '437292891', '437294939', '439212891', '439246683', '439247707', '439551835', '439552859']
    
    for j in arr:

        try:
            apiConnect('', '', '', start, end, j)
        except Exception as e:
            print("Error Message: ", e)    
            
            
    arr = ['44391395', '44392419', '44424163', '44425187', '44426211', '44427235', '44428259', '44429283', '44430307', '44431331', '44539875', '44540899', '44542947', '44543971', '44582883', '44637155', '44638179', '44639203', '44640227', '44641251', '44642275', '44677091', '44678115', '44679139', '44680163', '44779491', '44780515', '44781539', '44782563', '44783587', '44784611', '44785635', '44786659', '44814307', '44815331', '44869603', '44870627', '45073379', '45103075', '45104099', '45206499', '45207523', '45209571', '45210595', '45212643', '45214691', '45239267', '45244387', '45245411', '45246435', '45247459', '45249507', '45251555', '45252579', '45253603', '45254627', '45255651', '45256675', '45429731', '45430755', '45431779', '45432803', '45433827', '45436899', '45437923', '45516771', '45570019', '45571043', '45572067', '45573091', '45574115', '45575139', '45576163', '45597667', '45598691', '45599715', '45600739', '45602787', '45608931', '45609955', '45610979', '45612003', '45613027', '45614051', '45615075', '45664227', '45665251', '45666275', '45667299', '45669347', '45706211', '45707235', '45708259', '45709283', '45710307', '45711331', '45780963', '46033891', '46149603', '46154723', '46155747', '46156771', '46157795', '46158819', '46159843', '46160867', '46161891', '46162915', '46163939', '46164963', '46165987', '46167011', '46461923', '46582755', '46583779', '46584803', '46586851', '46587875', '46594019', '46595043', '46596067', '46597091', '46820323', '46821347', '46822371', '46823395', '46824419', '46825443', '46826467', '46827491', '46828515', '46829539', '46929891', '46932963', '46933987', '46935011', '46936035', '46937059', '46938083', '46939107', '46940131', '46941155', '46942179', '48068579', '48069603', '48070627', '48071651', '48072675', '48135139', '48136163', '48137187', '48142307', '48216035', '48217059', '48218083', '48300003', '48302051', '48304099', '48307171', '48308195', '48309219', '48312291', '48495587', '48496611', '48497635', '48498659', '48499683', '48500707', '48579555', '48622563', '48623587', '48624611', '48627683', '48630755', '48631779', '48695267', '48696291', '48697315', '48843747', '48844771', '48845795', '48874467', '48882659', '48989155', '48990179', '49042403', '49043427', '49044451', '49045475', '49047523', '49048547', '49050595', '49051619', '49058787', '49060835', '49061859', '49062883', '49063907', '49064931', '49428451', '49432547', '49433571', '49434595', '49436643', '49438691', '49439715', '49680355', '49681379', '49683427', '49684451', '49685475', '49686499', '49701859', '49702883', '49703907', '49808355', '50094051', '50095075', '50096099', '50097123', '50160611', '50291683', '50363363', '50488291', '50489315', '50490339', '50491363', '50492387', '50493411', '50504675', '50505699', '50506723', '50507747', '50508771', '50637795', '50696163', '50697187', '50822115', '50823139', '50832355', '50867171', '50897891', '50898915', '50904035', '50905059', '50906083', '50907107', '50908131', '50909155', '50910179', '50911203', '50947043', '50948067', '50949091', '50950115', '50951139', '50952163', '50953187', '50954211', '50955235', '50956259', '50957283', '50958307', '50964451', '50967523', '50968547', '50969571', '50970595', '51061731', '51062755', '51063779', '51064803', '51065827', '51066851', '51155939', '51156963', '51157987', '51159011', '51160035', '51161059', '51171299', '51172323', '51174371', '51176419', '51177443', '51178467', '51179491', '51180515', '51222499', '51298275', '51299299', '51300323', '51301347', '51302371', '51303395', '51304419', '51305443', '51311587', '51312611', '51313635', '51314659', '51315683', '51316707', '51317731', '51373027', '51374051', '51375075', '51376099', '51377123', '51378147', '51592163', '51597283', '51598307', '51878883', '51880931', '51881955', '51884003', '51885027', '51886051', '51887075', '51984355', '51985379', '51986403', '51987427', '51988451', '51989475', '51990499', '51991523', '51992547', '51993571', '51994595', '52018147', '52081635', '52418531', '52420579', '52489187', '52490211', '52491235', '52493283', '52494307', '52495331', '52496355', '52497379', '52498403', '52499427', '52500451', '52501475', '52502499', '52503523', '52504547', '52505571', '52506595', '52690915', '52691939', '52692963', '52695011', '52793315', '52794339', '52795363', '52796387', '52797411', '52798435', '52799459', '52800483', '52892643', '52893667', '52894691', '52895715', '52896739', '52897763', '52898787', '52901859', '52902883', '52903907', '52904931', '52905955', '52906979', '52909027', '52910051', '52911075', '52912099', '52913123', '52914147', '52915171', '52949987', '52951011', '52952035', '52953059', '52954083', '52955107', '52956131', '52957155', '52958179', '53046243', '53047267', '53049315', '53050339', '53052387', '53055459', '53056483', '53057507', '53058531', '53059555', '53060579', '53061603', '53070819', '53072867', '53073891', '53074915', '53075939', '53076963', '53079011', '53081059', '53164003', '53165027', '53166051', '53167075', '53168099', '53169123', '53170147', '53210083', '53211107', '53212131', '53214179', '53247971', '53248995', '53250019', '53260259', '53261283', '53262307', '53268451', '53269475', '53270499', '53271523', '53272547', '53273571', '53274595', '53275619', '53276643', '53277667', '53301219', '53302243', '53303267', '53304291', '53305315', '53306339', '53575651', '53578723', '53579747', '53580771', '53581795', '53583843', '53628899', '53629923', '53630947', '53631971', '53632995', '53634019', '53636067', '53637091', '53638115', '53639139', '53640163', '53641187', '53642211', '53644259', '53645283', '53646307', '53655523', '53657571', '53658595', '53659619', '53660643', '53726179', '53729251', '53730275', '53741539', '53742563', '53744611', '53745635', '53746659', '53747683', '53748707', '53749731', '53750755', '53751779', '53752803', '53753827', '53754851', '53756899', '53757923', '53760995', '53762019', '53763043', '53764067', '53765091', '53766115', '53767139', '53815267', '53816291', '53818339', '53819363', '53820387', '53821411', '53824483', '53825507', '53826531', '53827555', '53828579', '53829603', '53830627', '53831651', '53841891', '53842915', '53843939', '53844963', '53847011', '53942243', '53943267', '53944291', '53978083', '53979107', '53983203', '53984227', '53985251', '53986275', '53987299', '53988323', '53989347', '53998563', '54001635', '54004707', '54005731', '54049763', '54050787', '54051811', '54052835', '54053859', '54054883', '54055907', '54061027', '54062051', '54066147', '54067171', '54071267', '54072291', '54073315', '54074339', '54087651', '54088675', '54089699', '54090723', '54091747', '54092771', '54093795', '54094819', '54095843', '54104035', '54414307', '54415331', '54417379', '54418403', '54419427', '54420451', '54421475', '54422499', '54423523', '54424547', '54425571', '54426595', '54427619', '54428643', '54429667', '54430691', '54431715', '54432739', '54504419', '54511587', '54512611', '54514659', '54515683', '54517731', '54518755', '54523875', '54524899', '54525923', '54527971', '54528995', '54530019', '54531043', '54532067', '54533091', '54534115', '54535139', '54536163', '54537187', '54538211', '54578147', '54580195', '54581219', '54582243', '54583267', '54584291', '54585315', '54636515', '54640611', '54641635', '54649827', '54650851', '54651875', '54652899', '54653923', '54803427', '54975459', '54976483', '55050211', '55051235', '55052259', '55053283', '55054307', '55056355', '55057379', '55130083', '55131107', '55132131', '55133155', '55134179', '55135203', '55136227', '55137251', '55138275', '55139299', '55140323', '55141347', '55142371', '55143395', '55145443', '55146467', '55235555', '55236579', '55238627', '55239651', '55240675', '55241699', '55242723', '55243747', '55244771', '55245795', '55246819', '55247843', '55248867', '55249891', '55250915', '55251939', '55252963', '55253987', '55255011', '55566307', '55567331', '55568355', '55569379', '55570403', '55571427', '55572451', '55573475', '55574499', '55575523', '55577571', '55578595', '55579619', '55580643', '55581667', '55582691', '55583715', '55584739', '55585763', '55586787', '55663587', '55664611', '55665635', '55666659', '55768035', '55770083', '55874531', '55876579', '55877603', '55880675', '55881699', '55889891', '55890915', '55891939', '55892963', '55893987', '55895011', '55896035', '55897059', '55898083', '55899107', '55900131', '55901155', '55902179', '55903203', '55904227', '56003555', '56004579', '56005603', '56006627', '56008675', '56009699', '56010723', '56011747', '56231907', '56232931', '56233955', '56234979', '56236003', '56237027', '56275939', '56276963', '56277987', '56279011', '56283107', '56284131', '56285155', '56286179', '56350691', '56351715', '56352739', '56353763', '56356835', '56357859', '56358883', '56359907', '56361955', '56362979', '56364003', '56365027', '56366051', '56367075', '56369123', '56370147', '56459235', '56460259', '56461283', '56462307', '56463331', '56464355', '56465379', '56466403', '56467427', '56468451', '56469475', '56923107', '56924131', '56993763', '56994787', '56995811', '56996835', '56997859', '56998883', '57002979', '57006051', '57469923', '57471971', '57623523', '57864163', '57865187', '57866211', '57867235', '57868259', '57869283', '57870307', '57871331', '57872355', '57873379', '57874403', '57875427', '58029027', '58030051', '58031075', '58032099', '58033123', '58034147', '58035171', '58036195', '58037219', '58038243', '58039267', '58040291', '58041315', '58042339', '58098659', '58099683', '58100707', '58101731', '58102755', '58103779', '58104803', '58105827', '58191843', '58192867', '58193891', '58194915', '58197987', '58199011', '58200035', '58201059', '58234851', '58235875', '58236899', '58237923', '58238947', '58239971', '58240995', '58242019', '58243043', '58244067', '58370019', '58371043', '58372067', '58373091', '58374115', '58375139', '58376163', '58435555', '58436579', '58437603', '58742755', '58743779', '58744803', '58745827', '58746851']
    
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