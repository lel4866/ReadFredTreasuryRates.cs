# ReadFredTreasuryRates.cs
C# program to read treasury rates from FRED database, and supply a function
to interpolate rate for any period from 1 day to 360 days

Supplied functions:

**FredRateReader::FredRateReader(DateTime earliestDate)**

Reads LIBOR rates of durations: 1, 7, 30, 60, 90, 180, amd 360 from FRED database
Removes NaN's and interpolates linearly so that rates are available for all durations from 0
through 360

**float FredRateReader::RiskFreeRate(DateTime requestedDate, int duration)**

Returns a rate (in percent) for a requested date and duration

Throws an Exception if date is prior to earliest date of series (saved in global variable
rates_global_first_date) or later than today

Throws an Exception if duration is less than 0 or greater 360

**SP500DividenYieldReader::SP500DividenYieldReader(DateTime earliestDate)**

Reads S&P 500 dividend yield from Nasdaq Data Link (formerly Quandl)
This series typically only has one value per month, I interpolated between those values. I'm
not sure how good this is, but the values do change over the month so it's probabnly at least
as good as assuming the value stays fixed over the month

**float SP500DividenYieldReader::::SP500DividendYield(DateTime requestedDate)*

Returns an annualized dividend yield for the S&P500 for the requested date

For Python version, see github.com/lel4866/ReadFredTreasuryRates.py

# Programming comments:
This is written using C# 10 and Visual Studio 2022 Preview