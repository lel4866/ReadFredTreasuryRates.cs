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

using System.Collections.Concurrent;
using System.Diagnostics;

namespace ReadFredTreasuryRates;

public class FredRateReader {
    string version = "0.0.3";
    string version_date = "2021-09-11";

    bool rates_valid = false;

    static readonly int[] rates_duration_list = { 1, 7, 30, 60, 90, 180, 360 }; // the durations of the available FRED series

    static readonly Dictionary<int, string> seriesNames = new() {
        [1] = "USDONTD156N",
        [7] = "USD1WKD156N",
        [30] = "USD1MTD156N",
        [60] = "USD2MTD156N",
        [90] = "USD3MTD156N",
        [180] = "USD6MTD156N",
        [360] = "USD12MD156N"
    };

    ConcurrentDictionary<int, List<(DateTime, float)>> rates = new();

    DateTime rates_global_first_date = new(1980, 1, 1);  // will hold earliest existing date over all the FRED series
    DateTime rates_global_last_date = new(); // will hold earliest existing date over all the FRED series
    List<int> rates_interpolation_vector = new(); // for each day, has index of series to use to interpolate
    List<float> rates_array = new(); // the actual rate vector...1 value per day in percent

    public FredRateReader(DateTime earliestDate) {
        var stopWatch = new Stopwatch();
        DateTime today = DateTime.Now.Date;

        stopWatch.Start();

        Debug.Assert(earliestDate >= new DateTime(2000, 1, 1),
            $"FredRateReader.cs: earliest date ({earliestDate.Date}) is before 2000-01-01");
        Debug.Assert(earliestDate.Date <= DateTime.Now.Date,
            $"FredRateReader.cs: earliest date ({earliestDate.Date}) is after today ({today})");

        string today_str = today.ToString("MM/dd/yyyy");
#if true // read data series in parallel
        Parallel.ForEach(seriesNames, item => {
            GetFredDataFromUrl(item.Value, item.Key);
        });
#else // for debugging: read data series sequentially
        foreach ((int duration, string seriesName) in serieNames) {
            GetFredDataFromUrl(seriesName, duration);
            break;
        }
#endif

        // get latest first date over all series, earliest last date over all series
        rates_global_first_date = earliestDate;
        rates_global_last_date = new DateTime(3000, 1, 1);
        foreach ((int duration, List<(DateTime, float)> series) in rates) {
            (DateTime first_date, float rate) = series[0];
            if (first_date > rates_global_first_date)
                rates_global_first_date = first_date;
            (DateTime last_date, rate) = series.Last();
            if (last_date < rates_global_last_date)
                rates_global_last_date = last_date;
        }

        Console.WriteLine();
        Console.WriteLine($"Starting date for risk free rate table will be: {rates_global_first_date.ToString("yyyy-MM-dd")}");
        Console.WriteLine($"Ending date for risk free rate table will be: {rates_global_last_date.ToString("yyyy-MM-dd")}");
        Console.WriteLine();

        // now create numpy array with 1 row for EVERY day (including weekends and holidays) between global_first_date and
        // today, and 1 column for each FRED series named rate_array
        // once we do this, in order to grab a rate for a specific day and duration, we just compute # of days between
        // requested date and global_first_date, and use that as the index into the rates_array, then use the
        // requested duration to compute interpolated_rate (see detailed explanation below)
        int num_rows = (rates_global_last_date.AddDays(1) - rates_global_first_date).Days;
        int num_cols = seriesNames.Count;
        float[,] rates_array = new float[num_rows, num_cols]; // initialized to 0f
        // until Array.Fill works with 2D array
        for (int i = 0; i < num_rows; i++)
            for (int j = 0; j < num_cols; j++)
                rates_array[i, j] = float.NaN;

        // interpolate to replace NaN's
        // rates_array has 1 row for every date between rates_global_first_date and rates_global_last_date, all set to NaN
        // as you iterate through the series, place rates into rates_array. This will leave you with some rows in rates_array
        // that are NaN. Then , interpolate those.
        int i_col = 0, index_of_first_rate = -1, index_of_last_date = -1;
        foreach ((int duration, List<(DateTime, float)> series) in rates) {
            // skip to first date of interest (rates_global_first_date)
            foreach ((DateTime date, float rate) in series) {
                if (date < rates_global_first_date)
                    continue;
                if (date > rates_global_last_date)
                    break;
                int date_index = (date - rates_global_first_date).Days;

                // keep track of first and last rate that is not NaN
                if (!float.IsNaN(rate) {
                    if (index_of_first_rate == -1)
                        index_of_first_rate = date_index;
                    index_of_last_date = date_index;
                }

                rates_array[i_col, date_index] = rate;
            }
            i_col++;

            // now interpolate between NaN's
        }



        // for informational purposes only
        Console.WriteLine("duration = ", rates_duration_list[i_col])
        row = rate_df.iloc[0]
        Console.WriteLine(row.date.date(), row.rate)
        row = rate_df.iloc[-1]
        Console.WriteLine(row.date.date(), row.rate)
        Console.WriteLine()

        i_col = i_col + 1

        stopWatch.Stop();
    }

    void GetFredDataFromUrl(string series_name, int duration) {
        List<(DateTime, float)> data = new();

        // read data from FRED website
        Console.WriteLine("Reading " + series_name);
        string today_str = DateTime.Now.ToString("MM/dd/yyyy");
        string FRED_url = $"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_name}&cosd=1985-01-01&coed={today_str}";
        var hc = new HttpClient();
        Task<string> task = hc.GetStringAsync(FRED_url);
        string result_str = task.Result;
        hc.Dispose();

        // split string into lines, then into fields (should be just 2)
        string[] lines = result_str.Split('\n');
        bool header = true;
        foreach (string line in lines) {
            if (header) {
                header = false;
                continue;
            }
            if (line.Length == 0)
                continue;
            string[] fields = line.Split(',');
            if (fields.Length != 2 || fields[0].Length < 10)
                throw new InvalidFredDataException(series_name, line);
            if (!DateTime.TryParse(fields[0], out DateTime date))
                throw new InvalidFredDataException(series_name, line);
            float rate = float.NaN;
            if (fields[1] != ".")
                if (!float.TryParse(fields[1], out rate))
                    throw new InvalidFredDataException(series_name, line);
            data.Add((date, rate));
        }

        rates.TryAdd(duration, data);
    }

    public float RiskFreeRate(DateTime requestedDate, int duration) {
        return 0f;
    }
}

public class SP500DividenYieldReader {
    bool dividends_valid = false;
    List<float> dividend_array;  // vector containing sp500 dividend yield in percent
    DateTime dividends_global_first_date;  // will hold earliest existing date in dividend_array
    DateTime dividends_global_last_date;

    SP500DividenYieldReader(DateTime earliestDate) {

    }
    public float SP500DividendYield(DateTime requestedDate) {
        return 0f;
    }
}

[Serializable]
internal class InvalidFredDataException : Exception {
    internal InvalidFredDataException() { }
    internal InvalidFredDataException(string series_name, string row)
        : base($"Invalid Date in series: {series_name}: {row}") { }
}
