#run this program with python3
# on linux use the pip3 program to install the needed modules below as such:
# pip3 install yfinance
# pip3 install pymysql
# pip3 install json
#
# It is bad practice to install these using pip3 as root instead of a virtual environment. But.... it works.
# if you do not have pip3 try:
# as root: apt install pip3
# or Redhat as root: yum install pip3
#
# apt install mysql-server
# mysql> create database company_data;
# mysql> CREATE USER 'user1'@'localhost'  BY 'user1';
# mysql> GRANT ALL PRIVILEGES ON company_data.* TO 'user1'@'localhost' WITH GRANT OPTION;
# mysql> FLUSH PRIVILEGES;


import yfinance as yf
import time
import datetime
import pymysql
import json

all_cols_dict = {}

# get all stock in

"""

# get historical market data
hist = msft.history(period="1mo")

# show meta information about the history (requires history() to be called first)
msft.history_metadata

# show actions (dividends, splits, capital gains)
msft.actions
msft.dividends
msft.splits
msft.capital_gains  # only for mutual funds & etfs

# show share count
msft.get_shares_full(start="2022-01-01", end=None)

# show financials:
# - income statement
msft.income_stmt
msft.quarterly_income_stmt
# - balance sheet
msft.balance_sheet
msft.quarterly_balance_sheet
# - cash flow statement
msft.cashflow
msft.quarterly_cashflow
# see `Ticker.get_income_stmt()` for more options

# show holders
msft.major_holders
msft.institutional_holders
msft.mutualfund_holders
msft.insider_transactions
msft.insider_purchases
msft.insider_roster_holders

# show recommendations
msft.recommendations
msft.recommendations_summary
msft.upgrades_downgrades

# Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default. 
# Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
msft.earnings_dates

# show ISIN code - *experimental*
# ISIN = International Securities Identification Number
msft.isin

# show options expirations
msft.options

# show news
msft.news

# get option chain for specific expiration
opt = msft.option_chain('YYYY-MM-DD')
# data available via: opt.calls, opt.puts
"""

insert_template = {
'address1' : '',
'address2' : '',
'city' : '',
'state' : '',
'zip' : '',
'country' : '',
'phone' : '',
'fax' : '',
'website' : '',
'industry' : '',
'industryKey' : '',
'industryDisp' : '',
'sector' : '',
'sectorKey' : '',
'sectorDisp' : '',
'longBusinessSummary' : '',
'fullTimeEmployees' : 0,
'auditRisk' : 0,
'boardRisk' : 0,
'compensationRisk' : 0,
'shareHolderRightsRisk' : 0,
'overallRisk' : 0,
'governanceEpochDate' : 0,
'compensationAsOfEpochDate' : 0,
'irWebsite' : '',
'maxAge' : 0,
'priceHint' : 0,
'previousClose' : 0.0,
'open' : 0.0,
'dayLow' : 0.0,
'dayHigh' : 0.0,
'regularMarketPreviousClose' : 0.0,
'regularMarketOpen' : 0.0,
'regularMarketDayLow' : 0.0,
'regularMarketDayHigh' : 0.0,
'dividendRate' : 0.0,
'dividendYield' : 0.0,
'exDividendDate' : 0,
'payoutRatio' : 0.0,
'fiveYearAvgDividendYield' : 0.0,
'beta' : 0.0,
'trailingPE' : 0.0,
'forwardPE' : 0.0,
'volume' : 0,
'regularMarketVolume' : 0,
'averageVolume' : 0,
'averageVolume10days' : 0,
'averageDailyVolume10Day' : 0,
'bid' : 0.0,
'ask' : 0.0,
'bidSize' : 0,
'askSize' : 0,
'marketCap' : 0,
'fiftyTwoWeekLow' : 0.0,
'fiftyTwoWeekHigh' : 0.0,
'priceToSalesTrailing12Months' : 0.0,
'fiftyDayAverage' : 0.0,
'twoHundredDayAverage' : 0.0,
'trailingAnnualDividendRate' : 0.0,
'trailingAnnualDividendYield' : 0.0,
'currency' : '',
'enterpriseValue' : 0,
'profitMargins' : 0.0,
'floatShares' : 0,
'sharesOutstanding' : 0,
'sharesShort' : 0,
'sharesShortPriorMonth' : 0,
'sharesShortPreviousMonthDate' : 0,
'dateShortInterest' : 0,
'sharesPercentSharesOut' : 0.0,
'heldPercentInsiders' : 0.0,
'heldPercentInstitutions' : 0.0,
'shortRatio' : 0.0,
'shortPercentOfFloat' : 0.0,
'impliedSharesOutstanding' : 0,
'bookValue' : 0.0,
'priceToBook' : 0.0,
'lastFiscalYearEnd' : 0,
'nextFiscalYearEnd' : 0,
'mostRecentQuarter' : 0,
'earningsQuarterlyGrowth' : 0.0,
'netIncomeToCommon' : 0,
'trailingEps' : 0.0,
'forwardEps' : 0.0,
'lastSplitFactor' : '',
'pegRatio' : 0.0,
'lastSplitFactor' : '',
'lastSplitDate' : 0,
'enterpriseToRevenue' : 0.0,
'enterpriseToEbitda' : 0.0,
'52WeekChange' : 0.0,
'SandP52WeekChange' : 0.0,
'lastDividendValue' : 0.0,
'lastDividendDate' : 0,
'exchange' : '',
'quoteType' : '',
'symbol' : '',
'underlyingSymbol' : '',
'shortName' : '',
'longName' : '',
'firstTradeDateEpochUtc' : 0,
'timeZoneFullName' : '',
'timeZoneShortName' : '',
'uuid' : '',
'messageBoardId' : '',
'gmtOffSetMilliseconds' : 0,
'currentPrice' : 0.0,
'targetHighPrice' : 0.0,
'targetLowPrice' : 0.0,
'targetMeanPrice' : 0.0,
'targetMedianPrice' : 0.0,
'recommendationMean' : 0.0,
'recommendationKey' : '',
'numberOfAnalystOpinions' : 0,
'totalCash' : 0,
'totalCashPerShare' : 0.0,
'ebitda' : 0,
'totalDebt' : 0,
'quickRatio' : 0.0,
'currentRatio' : 0.0,
'totalRevenue' : 0,
'debtToEquity' : 0.0,
'revenuePerShare' : 0.0,
'returnOnAssets' : 0.0,
'returnOnEquity' : 0.0,
'freeCashflow' : 0,
'operatingCashflow' : 0,
'earningsGrowth' : 0.0,
'revenueGrowth' : 0.0,
'grossMargins' : 0.0,
'ebitdaMargins' : 0.0,
'operatingMargins' : 0.0,
'financialCurrency' : '',
'trailingPegRatio' : 0.0,
'executiveTeam' : '',
'tradeable' : '',
'language' : '',
'companyOfficers' : '',
'firstTradeDateEpochUtc' : '',
'timeZoneFullName' : '',
'underlyingSymbol' : '',
'timeZoneShortName' : '',
'uuid' : '',
'pegRatio' : '',
'region' : '',
'typeDisp' : ''
}


def build_yfinance_company_info_table(company_info):
    mysql_table_description = "create table "+company_info+" ( row_id BIGINT AUTO_INCREMENT PRIMARY KEY, \
address1 VARCHAR(100),\
address2 VARCHAR(100),\
city VARCHAR(50),\
state VARCHAR(50),\
zip VARCHAR(50),\
country VARCHAR(50),\
region VARCHAR(50),\
phone VARCHAR(50),\
fax VARCHAR(50),\
website BLOB,\
industry VARCHAR(40),\
industryKey VARCHAR(50),\
industryDisp VARCHAR(50),\
sector VARCHAR(50),\
sectorKey VARCHAR(50),\
sectorDisp VARCHAR(50),\
longBusinessSummary BLOB,\
fullTimeEmployees INT,\
companyOfficers BLOB,\
auditRisk INT,\
boardRisk INT,\
compensationRisk INT,\
shareHolderRightsRisk INT,\
overallRisk INT,\
governanceEpochDate INT,\
compensationAsOfEpochDate INT,\
irWebsite VARCHAR(200),\
maxAge INT,\
priceHint INT,\
previousClose FLOAT,\
open FLOAT,\
dayLow FLOAT,\
dayHigh FLOAT,\
regularMarketPreviousClose FLOAT,\
regularMarketOpen FLOAT,\
regularMarketDayLow FLOAT,\
regularMarketDayHigh FLOAT,\
dividendRate FLOAT,\
dividendYield FLOAT,\
exDividendDate INT,\
payoutRatio FLOAT,\
fiveYearAvgDividendYield FLOAT,\
beta FLOAT,\
trailingPE FLOAT,\
forwardPE FLOAT,\
volume INT,\
regularMarketVolume INT,\
averageVolume INT,\
averageVolume10days INT,\
averageDailyVolume10Day INT,\
bid FLOAT,\
ask FLOAT,\
bidSize INT,\
askSize INT,\
marketCap BIGINT,\
fiftyTwoWeekLow FLOAT,\
fiftyTwoWeekHigh FLOAT,\
priceToSalesTrailing12Months FLOAT,\
fiftyDayAverage FLOAT,\
twoHundredDayAverage FLOAT,\
trailingAnnualDividendRate FLOAT,\
trailingAnnualDividendYield BLOB,\
currency VARCHAR(10),\
tradeable VARCHAR(10),\
enterpriseValue BIGINT,\
profitMargins FLOAT,\
floatShares BIGINT,\
sharesOutstanding BIGINT,\
sharesShort INT,\
sharesShortPriorMonth INT,\
sharesShortPreviousMonthDate INT,\
dateShortInterest INT,\
sharesPercentSharesOut FLOAT,\
heldPercentInsiders FLOAT,\
heldPercentInstitutions FLOAT,\
shortRatio FLOAT,\
shortPercentOfFloat FLOAT,\
impliedSharesOutstanding BIGINT,\
bookValue FLOAT,\
priceToBook FLOAT,\
lastFiscalYearEnd INT,\
nextFiscalYearEnd INT,\
mostRecentQuarter INT,\
earningsQuarterlyGrowth FLOAT,\
netIncomeToCommon BIGINT,\
trailingEps FLOAT,\
forwardEps FLOAT,\
pegRatio VARCHAR(2),\
lastSplitFactor VARCHAR(50),\
lastSplitDate INT,\
enterpriseToRevenue FLOAT,\
enterpriseToEbitda FLOAT,\
52WeekChange FLOAT,\
SandP52WeekChange FLOAT,\
lastDividendValue FLOAT,\
lastDividendDate INT,\
exchange VARCHAR(50),\
quoteType VARCHAR(50),\
symbol VARCHAR(50),\
underlyingSymbol VARCHAR(50),\
shortName VARCHAR(50),\
longName BLOB,\
firstTradeDateEpochUtc VARCHAR(2),\
timeZoneFullName BLOB,\
timeZoneShortName BLOB,\
uuid VARCHAR(50),\
messageBoardId VARCHAR(50),\
gmtOffSetMilliseconds INT,\
currentPrice FLOAT,\
targetHighPrice FLOAT,\
targetLowPrice FLOAT,\
targetMeanPrice FLOAT,\
targetMedianPrice FLOAT,\
recommendationMean FLOAT,\
recommendationKey VARCHAR(50),\
numberOfAnalystOpinions INT,\
totalCash BIGINT,\
totalCashPerShare FLOAT,\
ebitda BIGINT,\
totalDebt BIGINT,\
quickRatio FLOAT,\
currentRatio FLOAT,\
totalRevenue BIGINT,\
debtToEquity FLOAT,\
revenuePerShare FLOAT,\
returnOnAssets FLOAT,\
returnOnEquity FLOAT,\
freeCashflow BIGINT,\
operatingCashflow BIGINT,\
earningsGrowth FLOAT,\
revenueGrowth FLOAT,\
grossMargins FLOAT,\
ebitdaMargins FLOAT,\
operatingMargins FLOAT,\
financialCurrency VARCHAR(10),\
grossProfits BIGINT,\
yield FLOAT,\
totalAssets BIGINT,\
navPrice FLOAT,\
category VARCHAR(50),\
ytdReturn FLOAT,\
beta3Year FLOAT,\
fundFamily VARCHAR(50),\
fundInceptionDate INT,\
legalType VARCHAR(20),\
threeYearAverageReturn FLOAT,\
fiveYearAverageReturn FLOAT,\
industrySymbol VARCHAR(10),\
trailingPegRatio VARCHAR(10),\
executiveTeam VARCHAR(70),\
language VARCHAR(20),\
quoteSourceName VARCHAR(40),\
triggerable VARCHAR(10),\
customPriceAlertConfidence VARCHAR(20),\
corporateActions BLOB,\
regularMarketTime VARCHAR(20),\
exchangeTimezoneName VARCHAR(20),\
exchangeTimezoneShortName VARCHAR(20),\
market VARCHAR(20),\
esgPopulated VARCHAR(20),\
priceEpsCurrentYear VARCHAR(20),\
fiftyDayAverageChange FLOAT,\
fiftyDayAverageChangePercent FLOAT,\
twoHundredDayAverageChange FLOAT,\
twoHundredDayAverageChangePercent FLOAT,\
sourceInterval INT,\
exchangeDataDelayedBy VARCHAR(20),\
averageAnalystRating VARCHAR(20),\
cryptoTradeable VARCHAR(10),\
hasPrePostMarketData VARCHAR(10),\
regularMarketChange FLOAT,\
regularMarketDayRange VARCHAR(25),\
fullExchangeName BLOB,\
averageDailyVolume3Month BIGINT,\
fiftyTwoWeekLowChange FLOAT,\
fiftyTwoWeekLowChangePercent FLOAT,\
fiftyTwoWeekRange VARCHAR(30),\
fiftyTwoWeekHighChange FLOAT,\
fiftyTwoWeekHighChangePercent FLOAT,\
fiftyTwoWeekChangePercent FLOAT,\
dividendDate VARCHAR(10),\
earningsTimestamp VARCHAR(10),\
earningsTimestampStart VARCHAR(10),\
earningsTimestampEnd VARCHAR(10),\
earningsCallTimestampStart VARCHAR(10),\
earningsCallTimestampEnd VARCHAR(10),\
isEarningsDateEstimate VARCHAR(5),\
epsTrailingTwelveMonths FLOAT,\
epsForward FLOAT,\
epsCurrentYear FLOAT,\
firstTradeDateMilliseconds BIGINT,\
marketState VARCHAR(10),\
regularMarketChangePercent FLOAT,\
regularMarketPrice FLOAT,\
displayName BLOB,\
ipoExpectedDate VARCHAR(10),\
prevName BLOB,\
nameChangeDate VARCHAR(10),\
netExpenseRatio FLOAT,\
trailingThreeMonthReturns FLOAT,\
trailingThreeMonthNavReturns FLOAT,\
netAssets FLOAT,\
morningStarOverallRating BLOB,\
morningStarRiskRating BLOB,\
annualReportExpenseRatio FLOAT,\
prevExchange BLOB,\
exchangeTransferDate DATE,\
newSymbol BLOB,\
openInterest BLOB,\
typeDisp VARCHAR(10))"
    return mysql_table_description


def dynamic_build_yfinance_company_info_table(company_info, all_cols_dict):
    mysql_table_description = "create table "+company_info+" ( row_id BIGINT AUTO_INCREMENT PRIMARY KEY,"


def sql_query(query):
    query_results = []
    con = pymysql.connect( host="192.168.1.58", user="user1", passwd="user1", db="company_data", port=3306 )
    with con:
        cur = con.cursor()
        print (query)
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            row_data = ""

            #print ("{0}\t {1}\t {2}".format(row[0], row[1], row[2]))
            for item in row:
                row_data = row_data + str(item) + ","
            query_results.append(row_data)

    #con.close()
    return(query_results)

def write_to_table(these_columns):

    con = pymysql.connect( host="192.168.1.58", user="user1", passwd="user1", db="company_data", port=3306, autocommit=True)
    with con:
        cur = con.cursor()
        #print ("Inserting into table")  
        cur.execute(these_columns)
    #con.close()
  

def initialize_company_info_table():

    print ("Using pymysql...")

    con = pymysql.connect( host="192.168.1.58", user="user1", passwd="user1", db="company_data", port=3306)
    with con:
        cur = con.cursor()
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()
        print("Database version: {}".format(version[0]))
        try:
            cur.execute("DROP TABLE company_data")
        except:
            print ("EXCEPTION: DROP TABLE company_data")
        table_build_command =  build_yfinance_company_info_table("company_data")
        print (table_build_command)
        cur.execute(table_build_command)
        tables = sql_query("SHOW TABLES")
        print ("Tables:\n")
        for table in tables:
            print (table)
        
    #con.close()
   
"""
INPUTS: list of stock symbols from a text flat file. Each symbol on one line and ending with a \n
PROCESSING:
        yfinane returns a dictionary of data.
        Each symbol is read from yahoo using yfinance in a loop.
        This may not be necessary but.
        A new dictionary called mydict is built.
        The keys and values are copied in.
        Finally the new dictionary mydict is filled with values from yfinace is turned to JSON
        and written to the file companyinfojson.txt as one long JSON line.
OUTPUTS: text file of JSON /Users/jfall/companyinfojson.txt
        The /Users/jfall/companyinfojson.txt file can be read later and then 
        processed as inputs to the mysql insert statement.
        A count of the companies processed is kept.
        A delay of (2) seconds is inserted so that yahoo! will not detect too rapid reads from a single IP address.
"""
def read_symbols_from_yfinance():
    companies = 0
    myfd = open("eoddata_nyse_nasdaq_stock_list_cleaned.csv","r")
    myfdout = open("/Users/jfall/companyinfojson.txt","w")
    myfdout.close()
    myfdout = open("/Users/jfall/companyinfojson.txt","a")
    for stock_sym in myfd:
        skip = False
        print (stock_sym)
        companies = companies + 1
        mydict = {}
        mysym = yf.Ticker(stock_sym.strip())
        #mysym.info
        try:
            myinfo = mysym.info
        except:
            skip = True
            print ("Exception Ticker: ",stock_sym," Failed. Not Found")
            
        if skip == False:
            for item in myinfo:
                mydict[item] = myinfo[item]
                #print (item, myinfo[item]) 
            print ("processed ", companies, " companies")
            print (mydict)
            generate_columns_list_from_run(mydict)
            myfdout.write(json.dumps(mydict)+"\n")
        time.sleep(1)
    myfdout.close()

def generate_columns_list_from_run(my_columns):
    '''
    This function should be invoked to read the dictionary returned for each
    company and add columns to a dictionary of columns. The dictionary of columns will 
    be used to generate the SQL table in the database.
    '''
    for this_col in my_columns:
        all_cols_dict[this_col] = " "
    print ("All columns dict = ",len(all_cols_dict))
        
    

"""
The scrubbing function simply is created to correct data which has failed insertion during the first tests of the sql insert command.
All the commas need removed.
Remove all the quotes.
Finally if data is string return the string with quotes around it like: 'I am a string'
"""

def scrub_data(data):
    if data == "Infinity":
        return 999999999999.0
    data = data.replace(",","")
    data = data.replace(";",".")
    data = data.replace("'","")
    data = data.replace("'","")
    data = data.replace("'","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    data = data.replace(",","")
    if str(type(data)) == "<class 'str'>":
        data = "'" + data + "'"
    #print ("Cleaned data: ",data)

    return data

"""
read_symbols_from_yfinance() is run first and creates a text flat file /Users/jfall/companyinfojson.txt
The flat file /Users/jfall/companyinfojson.txt is read line by line as JSON
The template which is the full set of columns inserted in the insert statement is copied to a fresh template from the static
template defined statically in the global section of the python code above.
A loop intereates thru each line in companyinfojson.txt file.
Each line is turned by json.loads() to a dictionary.
The dictionary is iterated via the keys.
Each key is copied with the value to the locally defined template overwriting any default values in the local template.
The local template ins_template is iterated and the colums part of the insert command is built.
Next the ins_template is interated over again and the values part of the sql command gains the values portion of the command.
The values must corresspond 1:1 to the columns defined before.
The data from the key lookup in ins_template is scrubbed. All the "'" and "," characters are removed and any extra
characters are removed.
Once the insert command is built the command is executed against the mysql db with write_to_table(insert_statement)
This will go on for all the JSON lines found in companyinfojson.txt
"""   
def process_company_info_json():
    companies = 0
    
    myfd = open("/Users/jfall/companyinfojson.txt","r")
    for line in myfd:
        ins_template = insert_template
        mydict = json.loads(line)
        for item in mydict:
            ins_template[item] = mydict[item]
            #print(item, mydict[item])
        # Here we are ready to create the insert statement
        insert_statement = "INSERT INTO company_data ("
        # build first keys part:
        for key in ins_template:
            insert_statement = insert_statement + key + ", "
        insert_statement = insert_statement[:-2]
        insert_statement = insert_statement + ") VALUES ("
        # build values part:
        for key in ins_template:
            insert_statement = insert_statement + str(scrub_data(str(ins_template[key]))) + ", "
        insert_statement = insert_statement[:-2]
        insert_statement = insert_statement + ");"
        #print (insert_statement)
        print ("===================================================================")
        companies = companies + 1
        print ("companies processed = ",companies)
        write_to_table(insert_statement)
        del ins_template
        #sql_query(insert_statement)
        """
        try:
            sql_query(insert_statement)
        except:
            for key in mydict:
                print (key, mydict[key])
            exit(0)
        """
    return companies
        
"""
Given a run thru the companyinfojson.txt this function will find the longest dictionary made via the data from yahoo yfinance.
This is useful for building the table. In actual practice it was found even longer data sets were returned.
Data sets returned from yfinance are not strictly one length but can vary. 121 key value pairs were the longest dictionaries found
as of 4/2/2024
"""
def size_company_data_dictionary():
    myfd = open("/Users/jfall/companyinfojson.txt","r")
    dict_len = 0
    for myline in myfd:
        mydict = json.loads(myline)
        if dict_len < len(mydict):
            dict_len = len(mydict)
    print ("largest dictinoary length = ",len(mydict))

    myfd.close()
    myfd = open("/Users/jfall/companyinfojson.txt","r")
    full_length = 0
    for myline in myfd:
        mydict = json.loads(myline)
        if dict_len == len(mydict):
            full_length = full_length + 1
            print ("==================================================================================================================")
            print(mydict)
    print ("full length companies at dict = 121 are", full_length)

"""
This function will aid in getting values to create the table for the mysql table create command. 
The values printed from this function are pasted to the actual table code which is defined in the 
global section of this python program. Each type of data is examined for char, int or float and the
mysql definition added to the table create command.
Sample data of dict_121 embedded in the function is used.
"""
def create_table_values_from_121_dict():
    mytable = ""
    dict_121 = {'address1': '2665 South Bayshore Drive', 'address2': 'Suite 901', 'city': 'Miami', 'state': 'FL', 'zip': '33133', 'country': 'United States', 'phone': '305 714 4100', 'fax': '305 858 4492', 'website': 'https://www.watsco.com', 'industry': 'Industrial Distribution', 'industryKey': 'industrial-distribution', 'industryDisp': 'Industrial Distribution', 'sector': 'Industrials', 'sectorKey': 'industrials', 'sectorDisp': 'Industrials', 'longBusinessSummary': 'Watsco, Inc., together with its subsidiaries, engages in the distribution of air conditioning, heating, refrigeration equipment, and related parts and supplies in the United States and internationally. The company distributes equipment, including residential ducted and ductless air conditioners, such as gas, electric, and oil furnaces; commercial air conditioning and heating equipment systems; and other specialized equipment. It also offers parts comprising replacement compressors, evaporator coils, motors, and other component parts; and supplies, such as thermostats, insulation materials, refrigerants, ductworks, grills, registers, sheet metals, tools, copper tubing, concrete pads, tapes, adhesives, and other ancillary supplies, as well as plumbing and bathroom remodeling supplies. The company serves contractors and dealers that service the replacement and new construction markets for residential and light commercial central air conditioning, heating, and refrigeration systems. Watsco, Inc. was founded in 1945 and is headquartered in Miami, Florida.', 'fullTimeEmployees': 7350, 'companyOfficers': [{'maxAge': 1, 'name': 'Mr. Albert H. Nahmad', 'age': 82, 'title': 'Chairman & CEO', 'yearBorn': 1941, 'fiscalYear': 2022, 'totalPay': 1094040, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Mr. Aaron J. Nahmad', 'age': 42, 'title': 'Co-Vice Chairman & President', 'yearBorn': 1981, 'fiscalYear': 2022, 'totalPay': 807873, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Ms. Ana M. Menendez', 'age': 58, 'title': 'CFO & Treasurer', 'yearBorn': 1965, 'fiscalYear': 2022, 'totalPay': 357625, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Mr. Barry S. Logan', 'age': 60, 'title': 'Executive VP of Planning & Strategy, Secretary & Director', 'yearBorn': 1963, 'fiscalYear': 2022, 'totalPay': 442625, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Mr. Steven  Rupp', 'title': 'Chief Technology Officer', 'fiscalYear': 2022, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Mr. Paul W. Johnston', 'age': 70, 'title': 'Executive Vice President', 'yearBorn': 1953, 'fiscalYear': 2022, 'totalPay': 553825, 'exercisedValue': 1150000, 'unexercisedValue': 304050}], 'auditRisk': 6, 'boardRisk': 8, 'compensationRisk': 4, 'shareHolderRightsRisk': 10, 'overallRisk': 8, 'governanceEpochDate': 1711670400, 'compensationAsOfEpochDate': 1672444800, 'irWebsite': 'http://investors.watsco.com/phoenix.zhtml?c=94992&p=irol-irhome', 'maxAge': 86400, 'priceHint': 2, 'previousClose': 431.97, 'open': 432.33, 'dayLow': 428.12, 'dayHigh': 435.757, 'regularMarketPreviousClose': 431.97, 'regularMarketOpen': 432.33, 'regularMarketDayLow': 428.12, 'regularMarketDayHigh': 435.757, 'dividendRate': 10.8, 'dividendYield': 0.0249, 'exDividendDate': 1712880000, 'payoutRatio': 0.71690005, 'fiveYearAvgDividendYield': 3.05, 'beta': 0.871, 'trailingPE': 31.737574, 'forwardPE': 26.800617, 'volume': 284281, 'regularMarketVolume': 284281, 'averageVolume': 355766, 'averageVolume10days': 405310, 'averageDailyVolume10Day': 405310, 'bid': 433.66, 'ask': 433.8, 'bidSize': 800, 'askSize': 900, 'marketCap': 17121408000, 'fiftyTwoWeekLow': 298.79, 'fiftyTwoWeekHigh': 441.29, 'priceToSalesTrailing12Months': 2.3506255, 'fiftyDayAverage': 400.5098, 'twoHundredDayAverage': 382.37036, 'trailingAnnualDividendRate': 9.8, 'trailingAnnualDividendYield': 0.02268676, 'currency': 'USD', 'enterpriseValue': 16541084672, 'profitMargins': 0.07363, 'floatShares': 30360383, 'sharesOutstanding': 33934300, 'sharesShort': 4145896, 'sharesShortPriorMonth': 3797916, 'sharesShortPreviousMonthDate': 1707955200, 'dateShortInterest': 1710460800, 'sharesPercentSharesOut': 0.105, 'heldPercentInsiders': 0.00746, 'heldPercentInstitutions': 1.0851899, 'shortRatio': 11.12, 'shortPercentOfFloat': 0.1398, 'impliedSharesOutstanding': 39434800, 'bookValue': 60.671, 'priceToBook': 7.1561375, 'lastFiscalYearEnd': 1703980800, 'nextFiscalYearEnd': 1735603200, 'mostRecentQuarter': 1703980800, 'earningsQuarterlyGrowth': -0.4, 'netIncomeToCommon': 499371008, 'trailingEps': 13.68, 'forwardEps': 16.2, 'pegRatio': 6.79, 'lastSplitFactor': '3:2', 'lastSplitDate': 903312000, 'enterpriseToRevenue': 2.271, 'enterpriseToEbitda': 20.509, '52WeekChange': 0.39053595, 'SandP52WeekChange': 0.28136122, 'lastDividendValue': 2.45, 'lastDividendDate': 1705363200, 'exchange': 'NYQ', 'quoteType': 'EQUITY', 'symbol': 'WSO', 'underlyingSymbol': 'WSO', 'shortName': 'Watsco, Inc.', 'longName': 'Watsco, Inc.', 'firstTradeDateEpochUtc': 455463000, 'timeZoneFullName': 'America/New_York', 'timeZoneShortName': 'EDT', 'uuid': 'accd0a2c-5815-3f13-a871-b17dcd01147b', 'messageBoardId': 'finmb_313461', 'gmtOffSetMilliseconds': -14400000, 'currentPrice': 434.17, 'targetHighPrice': 500.0, 'targetLowPrice': 350.0, 'targetMeanPrice': 410.08, 'targetMedianPrice': 406.0, 'recommendationMean': 2.7, 'recommendationKey': 'hold', 'numberOfAnalystOpinions': 12, 'totalCash': 210112000, 'totalCashPerShare': 5.716, 'ebitda': 806513984, 'totalDebt': 404792000, 'quickRatio': 1.416, 'currentRatio': 3.359, 'totalRevenue': 7283766784, 'debtToEquity': 15.473, 'revenuePerShare': 200.07, 'returnOnAssets': 0.1336, 'returnOnEquity': 0.26072, 'freeCashflow': 339176000, 'operatingCashflow': 561953984, 'earningsGrowth': -0.413, 'revenueGrowth': 0.014, 'grossMargins': 0.2735, 'ebitdaMargins': 0.11073, 'operatingMargins': 0.06382, 'financialCurrency': 'USD', 'trailingPegRatio': 3.3668}
    for key in dict_121:
        if str(type(dict_121[key])) == "<class 'str'>":
            mytable = mytable + key + " " + "VARCHAR(50)" + ",\\" + "\n"
        elif str(type(dict_121[key])) == "<class 'int'>":
            mytable = mytable + key + " " + "INT" + ",\\"  + "\n"
        elif str(type(dict_121[key])) == "<class 'float'>":
            mytable = mytable + key + " " + "FLOAT" + ",\\"  + "\n"
    print (mytable)

"""
This function will aid in getting values to create the template for the mysql insert command. 
The values printed from this function are pasted to the actual insert code which is defined in the 
global section of this python program. Each type of data is examined for char, int or float and the
default values are added to the sql insert create command.
Sample data of dict_121 embedded in the function is used.
"""
def create_template_values_from_121_dict():
    print ("=========================================================================================================================")
    mydict = {}
    dict_121 = {'address1': '2665 South Bayshore Drive', 'address2': 'Suite 901', 'city': 'Miami', 'state': 'FL', 'zip': '33133', 'country': 'United States', 'phone': '305 714 4100', 'fax': '305 858 4492', 'website': 'https://www.watsco.com', 'industry': 'Industrial Distribution', 'industryKey': 'industrial-distribution', 'industryDisp': 'Industrial Distribution', 'sector': 'Industrials', 'sectorKey': 'industrials', 'sectorDisp': 'Industrials', 'longBusinessSummary': 'Watsco, Inc., together with its subsidiaries, engages in the distribution of air conditioning, heating, refrigeration equipment, and related parts and supplies in the United States and internationally. The company distributes equipment, including residential ducted and ductless air conditioners, such as gas, electric, and oil furnaces; commercial air conditioning and heating equipment systems; and other specialized equipment. It also offers parts comprising replacement compressors, evaporator coils, motors, and other component parts; and supplies, such as thermostats, insulation materials, refrigerants, ductworks, grills, registers, sheet metals, tools, copper tubing, concrete pads, tapes, adhesives, and other ancillary supplies, as well as plumbing and bathroom remodeling supplies. The company serves contractors and dealers that service the replacement and new construction markets for residential and light commercial central air conditioning, heating, and refrigeration systems. Watsco, Inc. was founded in 1945 and is headquartered in Miami, Florida.', 'fullTimeEmployees': 7350, 'companyOfficers': [{'maxAge': 1, 'name': 'Mr. Albert H. Nahmad', 'age': 82, 'title': 'Chairman & CEO', 'yearBorn': 1941, 'fiscalYear': 2022, 'totalPay': 1094040, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Mr. Aaron J. Nahmad', 'age': 42, 'title': 'Co-Vice Chairman & President', 'yearBorn': 1981, 'fiscalYear': 2022, 'totalPay': 807873, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Ms. Ana M. Menendez', 'age': 58, 'title': 'CFO & Treasurer', 'yearBorn': 1965, 'fiscalYear': 2022, 'totalPay': 357625, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Mr. Barry S. Logan', 'age': 60, 'title': 'Executive VP of Planning & Strategy, Secretary & Director', 'yearBorn': 1963, 'fiscalYear': 2022, 'totalPay': 442625, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Mr. Steven  Rupp', 'title': 'Chief Technology Officer', 'fiscalYear': 2022, 'exercisedValue': 0, 'unexercisedValue': 0}, {'maxAge': 1, 'name': 'Mr. Paul W. Johnston', 'age': 70, 'title': 'Executive Vice President', 'yearBorn': 1953, 'fiscalYear': 2022, 'totalPay': 553825, 'exercisedValue': 1150000, 'unexercisedValue': 304050}], 'auditRisk': 6, 'boardRisk': 8, 'compensationRisk': 4, 'shareHolderRightsRisk': 10, 'overallRisk': 8, 'governanceEpochDate': 1711670400, 'compensationAsOfEpochDate': 1672444800, 'irWebsite': 'http://investors.watsco.com/phoenix.zhtml?c=94992&p=irol-irhome', 'maxAge': 86400, 'priceHint': 2, 'previousClose': 431.97, 'open': 432.33, 'dayLow': 428.12, 'dayHigh': 435.757, 'regularMarketPreviousClose': 431.97, 'regularMarketOpen': 432.33, 'regularMarketDayLow': 428.12, 'regularMarketDayHigh': 435.757, 'dividendRate': 10.8, 'dividendYield': 0.0249, 'exDividendDate': 1712880000, 'payoutRatio': 0.71690005, 'fiveYearAvgDividendYield': 3.05, 'beta': 0.871, 'trailingPE': 31.737574, 'forwardPE': 26.800617, 'volume': 284281, 'regularMarketVolume': 284281, 'averageVolume': 355766, 'averageVolume10days': 405310, 'averageDailyVolume10Day': 405310, 'bid': 433.66, 'ask': 433.8, 'bidSize': 800, 'askSize': 900, 'marketCap': 17121408000, 'fiftyTwoWeekLow': 298.79, 'fiftyTwoWeekHigh': 441.29, 'priceToSalesTrailing12Months': 2.3506255, 'fiftyDayAverage': 400.5098, 'twoHundredDayAverage': 382.37036, 'trailingAnnualDividendRate': 9.8, 'trailingAnnualDividendYield': 0.02268676, 'currency': 'USD', 'enterpriseValue': 16541084672, 'profitMargins': 0.07363, 'floatShares': 30360383, 'sharesOutstanding': 33934300, 'sharesShort': 4145896, 'sharesShortPriorMonth': 3797916, 'sharesShortPreviousMonthDate': 1707955200, 'dateShortInterest': 1710460800, 'sharesPercentSharesOut': 0.105, 'heldPercentInsiders': 0.00746, 'heldPercentInstitutions': 1.0851899, 'shortRatio': 11.12, 'shortPercentOfFloat': 0.1398, 'impliedSharesOutstanding': 39434800, 'bookValue': 60.671, 'priceToBook': 7.1561375, 'lastFiscalYearEnd': 1703980800, 'nextFiscalYearEnd': 1735603200, 'mostRecentQuarter': 1703980800, 'earningsQuarterlyGrowth': -0.4, 'netIncomeToCommon': 499371008, 'trailingEps': 13.68, 'forwardEps': 16.2, 'pegRatio': 6.79, 'lastSplitFactor': '3:2', 'lastSplitDate': 903312000, 'enterpriseToRevenue': 2.271, 'enterpriseToEbitda': 20.509, '52WeekChange': 0.39053595, 'SandP52WeekChange': 0.28136122, 'lastDividendValue': 2.45, 'lastDividendDate': 1705363200, 'exchange': 'NYQ', 'quoteType': 'EQUITY', 'symbol': 'WSO', 'underlyingSymbol': 'WSO', 'shortName': 'Watsco, Inc.', 'longName': 'Watsco, Inc.', 'firstTradeDateEpochUtc': 455463000, 'timeZoneFullName': 'America/New_York', 'timeZoneShortName': 'EDT', 'uuid': 'accd0a2c-5815-3f13-a871-b17dcd01147b', 'messageBoardId': 'finmb_313461', 'gmtOffSetMilliseconds': -14400000, 'currentPrice': 434.17, 'targetHighPrice': 500.0, 'targetLowPrice': 350.0, 'targetMeanPrice': 410.08, 'targetMedianPrice': 406.0, 'recommendationMean': 2.7, 'recommendationKey': 'hold', 'numberOfAnalystOpinions': 12, 'totalCash': 210112000, 'totalCashPerShare': 5.716, 'ebitda': 806513984, 'totalDebt': 404792000, 'quickRatio': 1.416, 'currentRatio': 3.359, 'totalRevenue': 7283766784, 'debtToEquity': 15.473, 'revenuePerShare': 200.07, 'returnOnAssets': 0.1336, 'returnOnEquity': 0.26072, 'freeCashflow': 339176000, 'operatingCashflow': 561953984, 'earningsGrowth': -0.413, 'revenueGrowth': 0.014, 'grossMargins': 0.2735, 'ebitdaMargins': 0.11073, 'operatingMargins': 0.06382, 'financialCurrency': 'USD', 'trailingPegRatio': 3.3668}
    for key in dict_121:
        if str(type(dict_121[key])) == "<class 'str'>":
            mydict[key] = "''"
        elif str(type(dict_121[key])) == "<class 'int'>":
            mydict[key] = 0
        elif str(type(dict_121[key])) == "<class 'float'>":
            mydict[key] = 0.0
    for key in mydict:
        print ("'" + key +"'" + " : " + str(mydict[key]) + ",")

def test_yfinance_download():
    msft = yf.Ticker("MSFT")
    msft.info
    myinfo = msft.info
    for item in myinfo:
        print (item, myinfo[item])
    return myinfo

def dump_live_columns():
    print ("all columns found:")
    print ("raw_columns:")
    print (all_cols_dict)
    print ("Individual Columns:")
    for mycol in all_cols_dict:
        print (mycol," : ",all_cols_dict[mycol])
 
def test_company_data_from_disk():
    
    myfd = open("/Users/jfall/companyinfojson.txt","r")
    mylines = 0
    for company in myfd:
        mylines = mylines + 1
        print (company)
    print ("processed: ",mylines)
    myfd.close()
    
     
     
 
    
"********************************************************************************************************************************"
"  MAIN begins here"
"********************************************************************************************************************************"
initialize_company_info_table()
#exit (0)
process_company_info_json()

print ("DONE")
exit (0)
#exit (0)
#compare_real_columns()
test_company_data_from_disk()
exit(0)

""" m
STEP 1)
COLLECT Data from Yahoo using yfinance and file with stock symbols preloaded.
"""
read_symbols_from_yfinance()

#dump_live_columns()

#exit(0)





"""
STEP 2
NOTICE: first create the table in the database. This wipes out the OLD table and data.. beware.
"""
initialize_company_info_table() # Danger - data loss.

"""
STEP 3
This function reads in the flat file saved from a data collection run from Yahoo!
"""
print ("starting process_company_info_json()")

processed = process_company_info_json()


print ("----------------------------------------------------------------------------------------------------------------------")
print ("------------------------------------------ ALL DATA is IN FROM JSON FLAT FILE ----------------------------------------")
print ("----------------------------------------------------------------------------------------------------------------------")
print (" PROCESSED ",processed," companies")

"""
STEP 4 - Query data from mysql db.
As root: mysql (on linux system)
mysql> use company_data;
mysql> select symbol from company data where (conditions)
 data will print to screen
"""





# HELPER functions
#create_table_values_from_121_dict()
#size_company_data_dictionary()

#print ("Initializing company_info table")
#initialize_company_info_table()
#exit (0)




   
#read_stocks_from_csv_download_via_yfinance_to_mysql()

time.sleep(60)
#restart.restart_stocks_rising()
            