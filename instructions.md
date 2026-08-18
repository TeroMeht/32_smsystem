Stock Monitoring System

Main purpose

I want to be able to monitor US stocks in real time and get alarms when my filtering criterias are matching with my strategies. In other words I want to be alarmed when conditions start to point to actual trade potentially occurring.  I don’t want to manually go through different tickers and constantly look for potential patterns. Manual monitoring involves increased risk for me to do 
something out of my initial plan. The longer I monitor, the higher the risk is. I want to know when some stock hits my setup criteria without constant monitoring. 

After the alarm is set my job is to evaluate the stock chart and decide if there is a setup to be taken. This can be considered as a layer between me and the market. 

Stock universe- Weekly filtering process

US mid and large caps
Price over 5$
Some average daily volume threshold? We want to filter out stocks that are not actively traded
How to filter these?
This is probably going to be even thousands of tickers to monitor?


Practical universe-refresh flow (weekly)

1. Pull seed list (S&P 900)
2. Fetch current market cap for each → keep if > $2B
3. Fetch last close → keep if > $5
4. Compute 20-day ADV$ from your daily table → keep if > $25M
5. Optional exclusions: ETFs, ADRs, recent IPOs (< 60 days), pending M&A
6. UPSERT survivors into monitored_symbols; deactivate the rest

Step 1: Security type
  Keep: common stock only
  Drop: ETFs, ETNs, warrants, units, rights, preferred shares,
        SPACs pre-merger, closed-end funds
  Expected survivors: ~4,000-5,000

Step 2: Exchange
  Keep: NYSE, NASDAQ, NYSE American
  Drop: OTC, Pink Sheets, Grey Market
  Expected survivors: ~3,500-4,500

Step 3: Price
  Keep: last close > $5
  Expected survivors: ~2,500-3,000

Step 4: Market cap
  Keep: > $2B (mid + large cap)
  Expected survivors: ~1,800-2,200

Step 5: Liquidity (20-day dollar volume)
  Keep: ADV$ > $10M   (widest — ~1,500-1,800)
   or   ADV$ > $25M   (moderate — ~1,000-1,300)
   or   ADV$ > $50M   (strict — ~700-900)



Data source
In building phase Stock Starter sub 29$/month with 15min delay data
In production real time and Stock Advanced sub 199$/month

Massive API 2 minute candles
Possibility to extend with tick data in the future




Backend functionality

The system needs to listen to a vast number of stocks in realtime. 2 min candles is probably enough. This is going to be Websocket connection to Massive data source 2 min candles. The stock universe has to be filtered according to my criteria. We are not interested in small caps. 

Data engine
Is responsible for data source handling, incoming data and keeping the database up-to-date. Also cleaning up old data from database is its responsibility. 
Historian data handle

Deals with historian data handle. Is responsible to make sure that monitored symbols have recent data for calculations. Has to deal with database layer so that we make sure latest data is in local database. Will seed database so that live stream can continue from last candle.

Will fetch historian data if local database is not up to date. This won’t active when stream is running.

Livestreamer

Handles incoming 2min candle stick data. This will handle the actual stream, its db inserts and service layer is going to handle this stream. Alarm statuses will be updated via service layer strategies which use this stream.
Calculation layer

When data is coming in it needs to go through the calculation layer where we calculate indicators. We need to calculate Relatr and Rvol on top of the data. It means that not only we will need 2 min candle data, but also historical data from daily charts and 2 min 5 day history. The calculation layer will add columns to the incoming data before strategy implementation. 

There will be calculations against historical data and livestream. These will need a bit different approach and functions. 
Database layer

Database will be needed for data stream because if the system restarts in mid session it cannot be allowed to fetch all the historical data all over again. When the system starts there has to be some check if we have data for all of the tickers and if it's up to date. In other words, in every start there has to be checked if we have data until today. When that is verified live streaming can start and Websocket connection opens. The database will be built on PostgreSQL.

In the initializing phase for every monitored ticker data has to be fetched until this moment. So when streaming starts it is able to add incoming candles after initial data fetch. This phase will generate tables into a database for every monitored ticker. This will also historical database table for daily ~14 days data and 2min historical data for last ~ 5 days. 




Example to illustrate its functionality:

When the system starts the next day these tables have to be updated with the latest data before the system starts streaming real time. This means there will be hundreds of thousands tables in the database. What should be taken into account with this?

If system then starts over again during the same day, history datas are not going to be updated from data source but read from local PostgresSQL database. Which is the case has to be checked every time application initializes.  

Database design

Database name: 32_smsystem

Schema in project folder

Service layer

Business logic on top of the data stream will be utilized here. Strategies and alarm rules will be defined here. Strategy parameters will be done over every incoming candle before it’s going to be saved in database. This is also where it’s decided what to do when alarm hits. 

Access to the FastApi endpoints will be configured to this layer

Project structure

Backend - FastAPI
core
app_factory
config
lifespan
routers
datapipe
calculations
incoming data helpers
history data handle
realstream data handle
services
schemas
FastAPI data schemas
database
main.py
dependencies.py 


Frontend functionalities

Visual representation 

UI dashboard with strategy specific tables. For example if DUOL hits Relatr threshold and it has increased Rvol this will be shown on “potential reversals” table. There will be multiple tables available but can be started with just a few. With this one basic idea is to be alerted about situations where price extends from vwap in a short period of time. 

On UI when conditions are met and stock and alarm are brought to the visual view user is able to click it and see candle stick patterns so it will save time not to insert that symbol to Tradingview manually. 



next js structure but with just 1 page multiple alarm windows. possibility to open a 2min chart and preview it.

All the incoming data has to be validated early on and data classes have to be used in the project. It’s not acceptable to pass vague dicts across the process. 
Project steps

Project structure and initialization new project
Stock universe filtering
Database configuration
Historical data infeed
Delayed data stream testing
Calculations layer on incoming data
Strategy implementation



I expect backend operations to be costly since there are a lot of tickers to watch and do calculations on. Should they still be coded using Python? Is this going to be yet another FastAPI + Nextjs project or should we use other languages?


How UI will receive its data?

There will be SSE event sent from backend. I want to avoid constant polling to the endpoint. Whenever there is interesting situation happening SSE event will be sent to the frontend. Basically UI is just listening incoming events.
