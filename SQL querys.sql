											-- =============Stock Market Data Analysis of ITC================
SELECT *
FROM itc_stock;

-- First Create duplicate table
CREATE TABLE `itc_stock2` (
  `Index` int DEFAULT NULL,
  `Date` text,
  `Symbol` text,
  `Series` text,
  `Prev Close` bigint DEFAULT NULL,
  `Open` double DEFAULT NULL,
  `High` double DEFAULT NULL,
  `Low` double DEFAULT NULL,
  `Last` double DEFAULT NULL,
  `Close` double DEFAULT NULL,
  `VWAP` double DEFAULT NULL,
  `Volume` int DEFAULT NULL,
  `Turnover(b)` double DEFAULT NULL,
  `Trades` text DEFAULT NULL,
  `Deliverable Volume` text DEFAULT NULL,
  `%Deliverble` text DEFAULT NULL
) ;
-- Insert the value from "itc_stock" into 2nd table.
INSERT INTO itc_stock2
(`Index` ,
  `Date`,
  `Symbol` ,
  `Series` ,
  `Prev Close` ,
  `Open` ,
  `High` ,
  `Low` ,
  `Last` ,
  `Close` ,
  `VWAP` ,
  `Volume`,
  `Turnover(b)`,
  `Trades` ,
  `Deliverable Volume`,
  `%Deliverble`)
SELECT 
	`Index` ,`Date`,`Symbol` ,`Series` ,`Prev Close` ,`Open` ,
	`High` ,`Low` ,`Last` ,`Close` ,`VWAP` ,`Volume`,`Turnover(b)`,
	`Trades` ,`Deliverable Volume`,`%Deliverble`
FROM itc_stock ;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

# First let's check for duplicates

SELECT *
FROM (
	SELECT `Index` ,`Date`,Symbol ,Series , `Prev Close` ,`Open` ,High ,Low ,`Last` ,`Close` ,VWAP ,Volume,`Turnover(b)`,Trades ,`Deliverable Volume`, `%Deliverble`,
		ROW_NUMBER() OVER (
			PARTITION BY  `Index` ,`Date`,Symbol ,Series , `Prev Close` ,`Open` ,High ,Low ,`Last` ,`Close` ,VWAP ,Volume,`Turnover(b)`,
							Trades ,`Deliverable Volume`, `%Deliverble`) AS row_num
	FROM 
		project.itc_stock2
) duplicates
WHERE 
	row_num > 1;
-- Looks like the data dose not have any duplicate values
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
# Standardize Data
SELECT COUNT(*)
FROM 
	itc_stock2
WHERE `Deliverable Volume` = ''  ;

SELECT COUNT(*)
FROM itc_stock2
WHERE Trades IS NULL;
-- There are 514 records where does not have any value in Deliverable Volume,%Deliverble columns and 2850 in Trades.

-- Rename column header
ALTER TABLE itc_stock2
RENAME COLUMN `Turnover(b)` TO Turnover,
RENAME COLUMN `Deliverable Volume` TO Deliverable_volume,
RENAME COLUMN `%Deliverble` TO Deliverble;

ALTER TABLE itc_stock2
RENAME COLUMN `Date`TO Stc_Date;

-- Let's also fix the date columns:
UPDATE itc_stock2
SET Stc_Date = STR_TO_DATE(Stc_Date, '%d-%m-%Y');

ALTER TABLE itc_stock2
MODIFY COLUMN Stc_Date DATE;

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE itc_stock2
SET Trades  = NULL
WHERE Trades = 'Null' ;

UPDATE itc_stock2
SET Deliverable_volume  = NULL
WHERE Deliverable_volume = 'Null' ;

UPDATE itc_stock2
SET Deliverble  = NULL
WHERE Deliverble = 'Null' ;

-- Convert the data type
ALTER TABLE itc_stock2
MODIFY COLUMN Trades INT,
MODIFY COLUMN Deliverable_volume INT,
MODIFY COLUMN Deliverble FLOAT;

-- remove any columns and rows we need to
ALTER TABLE itc_stock2
DROP COLUMN Symbol,
DROP COLUMN Series;
-- Now the table is ready for analysis
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Total Trading Days
SELECT COUNT(DISTINCT Stc_Date) AS total_days
FROM itc_stock2;
-- 5306 trading days
-- -----------------------------------------------------------------------------------------------------------
# Average Daily Volume:
SELECT AVG(Volume) AS Avg_daily_volume
FROM itc_stock2;
-- Avg daily volume is 7173165.02
-- -------------------------------------------------------------------------------------------------------------
# Highest and Lowest Closing Prices
SELECT MAX(Close) AS Highest_price, MIN(Close) AS Lowest_price
FROM itc_stock2;
-- Hight Closing Price 1940.1 and Lowest Closing Price 115.45
-- -----------------------------------------------------------------------------------------------------------------
# Time-Series Analysis and Trends
-- Calculate the daily percentage change in closing price and identify the top 10 days with the highest percentage change.
WITH Daily_Change AS (
    SELECT
        Stc_Date,
        Close AS Current_Close,
        LAG(Close) OVER (ORDER BY Stc_Date) AS Previous_Close,
        ((Close - LAG(Close) OVER (ORDER BY Stc_Date)) / LAG(Close) OVER (ORDER BY Stc_Date)) * 100 AS Percent_Change
    FROM 
        itc_stock2
)
SELECT 
    Stc_Date,
    Current_Close,
    Previous_Close,
    Percent_Change
FROM 
    Daily_Change
ORDER BY 
    ABS(Percent_Change) DESC
LIMIT 10;


-- Moving Averages 
SELECT 
    Stc_Date,`Close`,
    ROUND(AVG(`Close`) OVER (ORDER BY Stc_Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),3) AS Moving_Avg_7_Day,
	ROUND(AVG(`Close`) OVER (ORDER BY Stc_Date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),3) AS Moving_Avg_30_Day
FROM 
    itc_stock2
ORDER BY 
    Stc_Date;

-- Monthly & Quarterly Aggregates
SELECT 
	MONTHNAME(Stc_Date) AS Months,
    AVG(Close) AS Avg_Close_Monthly,
    SUM(Volume) AS Total_Volume_Monthly,
    SUM(Turnover) AS Total_Turnover_Monthly
FROM 
    itc_stock2
GROUP BY 
    Months;

SELECT 
	QUARTER(Stc_Date) AS Quarter,
    ROUND(AVG(Close),3) AS Avg_Close_Quarterly,
    SUM(Volume) AS Total_Quarterly_Volume,
    ROUND(SUM(Turnover),3) AS Total_Quarterly_Turnover
FROM 
    itc_stock2
GROUP BY 
    Quarter;

-- Volatility Analysis
SELECT 
    MONTHNAME(Stc_Date) AS Months,
    STDDEV(Close) AS Volatility_Monthly,              -- Compute the standard deviation of Close prices within each month to analyze volatility
    RANK() OVER(ORDER BY STDDEV(Close) DESC) AS Volatility_rank     -- Rank months by volatility to identify high and low volatility periods
FROM 
    itc_Stock2
GROUP BY 
    Months;
-- August is the Highest and October is the lowest Volatility months.
-- -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Trading Volume and Liquidity Analysis
-- Volume Patterns by Time of Year
SELECT 
    DAYNAME(Stc_Date) AS Day_of_Week,
    AVG(Volume) AS Avg_Volume
FROM 
    itc_Stock2
GROUP BY 
     Day_of_Week;
     
-- Delivery Percentage Analysis
SELECT 
    Stc_Date, Close, Deliverable_volume, Volume,
    ROUND((Deliverable_volume / Volume) * 100, 2) AS Deliverable_Percentage
FROM 
    itc_Stock2
WHERE 
	Deliverable_volume IS NOT NULL
ORDER BY 
    Deliverable_Percentage DESC;
-- Identify high-Deliverable_Percentage days, as these can indicate more significant investor interest in holding the stock.

-- Turnover and Trades Insights
SELECT 
    Stc_Date,Turnover,IFNULL(Trades,0) AS Trades,
    ROUND(Turnover / IFNULL(Trades, 0), 2) AS Avg_Turnover_per_Trade
FROM 
    itc_Stock2
ORDER BY 
    Stc_Date;
-- Analyze the correlation between turnover and the number of trades to see how trade volume impacts price changes.
-- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
 # Comparative Analysis
 -- Price and Volume Correlation
SELECT 
    Stc_Date, Close,Volume,
    ROUND((Close - LAG(Close) OVER (ORDER BY Stc_Date)),3) AS Price_Change,
    (Volume - LAG(Volume) OVER (ORDER BY Stc_Date)) AS Volume_Change
FROM 
    itc_Stock2
ORDER BY 
    Stc_Date;

-- Identify Top 10 Highest and Lowest Trading Days by Volume and Turnover

-- Top 10 Highest Volume and Turnover Days
SELECT 
    Stc_Date, Volume ,Turnover
FROM 
    itc_Stock2
ORDER BY 
    Volume DESC,
    Turnover DESC
LIMIT 10;

-- Top 10 Lowest Volume and Turnover Days
SELECT 
    Stc_Date, Volume ,Turnover
FROM 
    itc_Stock2
ORDER BY 
    Volume ASC,
    Turnover ASC
LIMIT 10;
-- ------------------------------------------------------------------------------------------------------------------------------------------------------ 
# Store Key Insights as Views
-- View for Daily Price Change and Percentage Change
CREATE VIEW Daily_Price_Change AS
SELECT 
    Stc_Date,
    Close,
    `Prev Close`,
    (Close - `Prev Close`) AS Price_Change,
    ROUND((Close - `Prev Close`) / `Prev Close` * 100, 2) AS Price_Change_Percent
FROM 
	itc_stock2;
    
-- View for Moving Averages (7-day and 30-day):
CREATE VIEW Moving_Averages AS
SELECT 
    Stc_Date,
    Close,
    AVG(Close) OVER (ORDER BY Stc_Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Moving_Avg_7_Day,
    AVG(Close) OVER (ORDER BY Stc_Date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS Moving_Avg_30_Day
FROM 
    itc_Stock2;

-- View for Monthly Aggregates
CREATE VIEW Monthly_Aggregates AS
SELECT 
	MONTHNAME(Stc_Date) AS Months,
    AVG(Close) AS Avg_Close_Monthly,
    SUM(Volume) AS Total_Volume_Monthly,
    SUM(Turnover) AS Total_Turnover_Monthly
FROM 
    itc_stock2
GROUP BY 
    Months;

SELECT * FROM Moving_Averages;



