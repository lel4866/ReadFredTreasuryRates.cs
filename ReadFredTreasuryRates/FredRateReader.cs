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
    static readonly int[] rates_duration_list = { 1, 7, 30, 60, 90, 180, 360 }; // the durations of the available FRED series
    Dictionary<int, string> FRED_interest_rates = new() {
        [1] = "USDONTD156N",
        [7] = "USD1WKD156N",
        [30] = "USD1MTD156N",
        [60] = "USD2MTD156N",
        [90] = "USD3MTD156N",
        [180] = "USD6MTD156N",
        [360] = "USD12MD156N"
    };

    DateTime rates_global_first_date = new(1980, 1, 1);  // will hold earliest existing date over all the FRED series
    DateTime rates_global_last_date = new(); // will hold earliest existing date over all the FRED series
    List<int> rates_interpolation_vector = new(); // for each day, has index of series to use to interpolate
    List<float> rates_array = new(); // the actual rate vector...1 value per day in percent

    public FredRateReader(DateTime earliestDate) {
        var stopWatch = new Stopwatch();
        DateTime today = DateTime.Now.Date;

        stopWatch.Start();

        if (earliestDate < new DateTime(2000, 1, 1))
            throw new ArgumentException($"ReadFredTresuryRates.py:read_risk_free_rates: earliest date ({earliestDate.Date} is before 2000-01-01");
        if (earliestDate.Date > DateTime.Now.Date)
            throw new ArgumentException($"ReadFredTresuryRates.py:read_risk_free_rates: earliest date ({earliestDate.Date} is after today ({today}");

        string today_str = today.ToString("MM/dd/yyyy");
        foreach (var item in FRED_interest_rates) {
            int duration = item.Key;
            string series_name = item.Value;
            Console.WriteLine("Reading ", series_name);
            GetFredDataFromUrl(series_name);
        }

        stopWatch.Stop();
    }

    void GetFredDataFromUrl(string series_name) {
        // read data from FRED website
        string today_str = DateTime.Now.ToString("MM/dd/yyyy");
        string FRED_url = $"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_name}&cosd=1985-01-01&coed={today_str}";
        var hc = new HttpClient();
        Task<string> task = hc.GetStringAsync(url);
        string result_str = task.Result;
        hc.Dispose();

        // split string into lines, then into fields (should be just 2)
        string[] lines = result_str.Split('\n');
        foreach (string line in lines) {
            string[] fields = line.Split(',');
            if (fields.Length != 2 || fields[0].Length < 10)
                throw new InvalidFredDataException(series_name, line);
            if (!DateTime.TryParse(fields[0], out DateTime date))
                throw new InvalidFredDataException(series_name, line);
            float rate = float.NaN;
            if (fields[1] != ".")
                if (!float.TryParse(fields[1], out rate))
                    throw new InvalidFredDataException(series_name, line);
        }
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

[Serializable]
internal class InvalidFredDataException : Exception {
    internal InvalidFredDataException() {}
    internal InvalidFredDataException(string series_name, string row)
        : base($"Invalid Date in series: {series_name}: {row}") {}
}
