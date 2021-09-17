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
    public string version = "0.0.3";
    public string version_date = "2021-09-13";

    bool rates_valid = false;

    static readonly Dictionary<int, string> seriesNames = new() {
        [1] = "USDONTD156N",
        [7] = "USD1WKD156N",
        [30] = "USD1MTD156N",
        [60] = "USD2MTD156N",
        [90] = "USD3MTD156N",
        [180] = "USD6MTD156N",
        [360] = "USD12MD156N"
    };

    private ConcurrentDictionary<int, List<(DateTime, float)>> rates = new();

    private DateTime rates_global_first_date = new(1980, 1, 1);  // will hold earliest existing date over all the FRED series
    private DateTime rates_global_last_date = new(); // will hold earliest existing date over all the FRED series
    private List<int> rates_interpolation_vector = new(); // for each day, has index of series to use to interpolate
    public float[,] rates_array; // the actual rate vector...1 value per day between  in percent

    public FredRateReader(DateTime earliestDate) {
        var stopWatch = new Stopwatch();
        DateTime today = DateTime.Now.Date;
        string today_str = today.ToString("MM/dd/yyyy");

        stopWatch.Start();

        Debug.Assert(earliestDate >= new DateTime(2000, 1, 1),
            $"FredRateReader.cs: earliest date ({earliestDate.Date}) is before 2000-01-01");
        Debug.Assert(earliestDate.Date <= DateTime.Now.Date,
            $"FredRateReader.cs: earliest date ({earliestDate.Date}) is after today ({today})");

        // read rate data from FRED into ConcurrentDictionary: rates; rates is only used temporarily in this constructor
        // cleaned rate data for later use is saved in rates_array, which has 1 row for each day of data we are interested in,
        // and 1 column for each data series (each duration)
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

        // now create rates_array with 1 row for EVERY day (including weekends and holidays) between global_first_date and
        // today, and 1 column for each FRED series (of different durations)
        // once we do this, in order to grab a rate for a specific day and duration, we just compute # of days between
        // requested date and global_first_date, and use that as the index into the rates_array
        int num_rows = (rates_global_last_date.AddDays(1) - rates_global_first_date).Days;
        int num_cols = seriesNames.Count;
        rates_array = new float[num_rows, num_cols]; // initialized to 0f
        // until Array.Fill works with 2D array
        for (int i = 0; i < num_rows; i++)
            for (int j = 0; j < num_cols; j++)
                rates_array[i, j] = float.NaN;
        // rates_array now has 1 row for every date between rates_global_first_date and rates_global_last_date, all set to NaN
        // iterate through the series read from FRED, and place each rate read into proper index in rates_array. This will
        // leave you with some locations in rates_array that are NaN. Then, linearly interpolate to get values of those NaN's
        int i_col = 0;
        foreach (var (duration, series) in rates) {
            InterpolateRates(i_col, duration, series);
            i_col++;
        }

        rates.Clear(); // free up memory
        rates_valid = true;

        stopWatch.Stop();
        Console.WriteLine($"FredRateReader: Elapsed time={stopWatch.ElapsedMilliseconds/1000.0}");
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

    void InterpolateRates(int i_col, int duration, List<(DateTime, float)> series) {
        // rates_array has 1 row for every date between rates_global_first_date and rates_global_last_date, all set to NaN
        // as you iterate through the series read from FRED, place each rates read into rates_array. This will leave you
        // with some rows in rates_array that are NaN. Then, interpolate to get those values.
        int num_rows = rates_array.GetLength(0);
        int index_of_first_rate = -1, index_of_last_rate = -1;

        // skip to first date of interest (rates_global_first_date)
        foreach ((DateTime date, float rate) in series) {
            if (date < rates_global_first_date)
                continue;
            if (date > rates_global_last_date)
                break;
            int date_index = (date - rates_global_first_date).Days;

            // keep track of first and last rate that is not NaN
            if (!float.IsNaN(rate)) {
                if (index_of_first_rate == -1)
                    index_of_first_rate = date_index;
                index_of_last_rate = date_index;
            }

            rates_array[date_index, i_col] = rate;
        }

        // now interpolate between NaN's
        if (index_of_first_rate == -1)
            throw new InvalidFredDataException(seriesNames[duration], "All data is missing.");

        // fill front of array with first real rate
        float first_rate = rates_array[index_of_first_rate, i_col];
        for (int i = 0; i < index_of_first_rate; i++)
            rates_array[i, i_col] = first_rate;

        // fill back of array with last real rate
        float last_rate = rates_array[index_of_last_rate, i_col];
        for (int i = index_of_last_rate + 1; i < num_rows; i++)
            rates_array[i, i_col] = last_rate;

        // interpolate between interior NaN's
        int i_row = index_of_first_rate;
        Debug.Assert(!float.IsNaN(rates_array[0, i_col]));
        while (i_row < index_of_last_rate) {
            // find first Nan. We know it will not be in row 0, because we already filled the front of the array
            if (!float.IsNaN(rates_array[i_row, i_col])) {
                i_row++;
                continue;
            }

            // find first non-NaN following found NaN. Again...this will be found because we filled back of array
            int j;
            for (j = i_row + 1; j < index_of_last_rate; j++) {
                if (!float.IsNaN(rates_array[j, i_col]))
                    break;
            }
            i_row--; // index of first NaN

            Debug.Assert(i_row >= index_of_first_rate);
            Debug.Assert(!float.IsNaN(rates_array[i_row, i_col]));
            Debug.Assert(j < index_of_last_rate); // assert we found an Nan
            Debug.Assert(!float.IsNaN(rates_array[j, i_col]));

            // now, linearly interpolate between first NaN and last Nan; i is index of 1st Nan, j is index of next non-NaN
            float denominator = j - i_row;
            float initial_value = rates_array[i_row, i_col];
            float range = rates_array[j, i_col] - initial_value;
            for (int k = i_row+1; k < j; k++) {
                Debug.Assert(float.IsNaN(rates_array[k, i_col]));
                float ratio = (k - i_row) / denominator;
                rates_array[k, i_col] = initial_value + ratio * range;
            }
            i_row = j+1;
        }
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
