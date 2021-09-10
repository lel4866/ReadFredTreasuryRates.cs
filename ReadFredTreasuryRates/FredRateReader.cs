/*
 * function to read FRED (St Louis Fed database) LIBOR interest rates, and a function to return the interpolated rate for
 * any duration, from 0 days to 360 days, for any given date in the FRED series back to the beginning of the FRED series,
 * which appears to be 2001-01-01 (some of the individual rate series go back further, this is the earliest for all the
 * series (rates_global_first_date)
 * 
 * The intention is that this will be used for things like the risk free rate for Black Scholes computations.
 * 
 * read_FRED_interest_rates() reads the FRED LIBOR rates from https://fred.stlouisfed.org/graph/fredgraph.csv
 * and creates a dictionary whose keys are the duration in days (1, 7, 30, 60, 90, 180, 360),
 * and whose values are another dictionary whose keys are a date and whose value is the rate in percent
 * 
 * the FRED LIBOR database goes back to 1986 or so, but has some missing days. Missing days are interpolated so there
 * will be a value for every day of the year from the start of the FRED series forward
 * 
 * After 12/31/2021, LIBOR rates will be replaced by SOFR and this code will be modified appropriately so the caller
 * doesn't need to know whether the rate comes from the LIBOR database or SOFR database
 * 
 * There is, in addition a function to read the monthly S&P 500 dividend yield from Nasdaq Data Link (formerly Quandl),
 * primarily for use in Black Scholes Merton option pricing formula that includes dividends.This is a monthly series,
 * which I interpolate to daily.Not sure if this interpolation is a good idea...
 * 
 * I make no guarantees of any kind for this program...use at your own risk
 * Lawrence E. Lewis
 */

using System.Diagnostics;

namespace ReadFredTreasuryRates;

public class FredRateReader {
    public FredRateReader(DateTime earliestDate) {
        var stopWatch = new Stopwatch();

    }

    public float RiskFreeRate(DateTime requestedDate, int duration) {
        return 0f;
    }
}

public class SP500DividenYieldReader {
    SP500DividenYieldReader(DateTime earliestDate) {

    }
    public float SP500DividendYield(DateTime requestedDate) {
        return 0f;
    }
}
