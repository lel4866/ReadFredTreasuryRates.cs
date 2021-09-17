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
    const int num_durations = 361; // 0 to 360
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

    private ConcurrentDictionary<int, List<(DateTime, float)>> fred_interest_rates = new();

    private DateTime rates_global_first_date = new(1980, 1, 1);  // will hold earliest existing date over all the FRED series
    private DateTime rates_global_last_date = new(); // will hold earliest existing date over all the FRED series
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

        // read rate data from FRED into ConcurrentDictionary rates; rates is only used temporarily in this constructor
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

        // scan all columns (all durations) in rates Dictionary, and get latest first date , earliest last date.
        // That is, set rates_global_first_date, rates_global_last_date instance variables
        GetFirstAndLastDates(earliestDate);

        // now create rates_array with 1 row for EVERY day (including weekends and holidays) between rates_global_first_date and
        // and rates_global_last_date (usually today), and 1 column for every duration between 0 and 360.
        CreateRatesArray();

        fred_interest_rates.Clear(); // free up memory
        rates_valid = true;

        stopWatch.Stop();
        Console.WriteLine($"FredRateReader: Elapsed time={stopWatch.ElapsedMilliseconds / 1000.0}");
    }

    void GetFredDataFromUrl(string series_name, int duration) {
        List<(DateTime, float)> data = new(); // will contain rates for 1 duration

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

        fred_interest_rates.TryAdd(duration, data); // fred_interest_rates contains rates for each duration read
    }

    // get latest first date over all series, earliest last date over all series
    void GetFirstAndLastDates(DateTime earliestDate) {
        rates_global_first_date = earliestDate;
        rates_global_last_date = new DateTime(3000, 1, 1);
        foreach ((int duration, List<(DateTime, float)> series) in fred_interest_rates) {
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
    }

    // create rates_array with 1 row for EVERY day (including weekends and holidays) between global_first_date and
    // global_last_date (usually today), and 1 column for every duration between 0 and 360. Initialize with NaN's
    void CreateRatesArray() {
        int num_rows = (rates_global_last_date - rates_global_first_date).Days + 1;
        rates_array = new float[num_rows, num_durations]; // initialized to 0f
        // until Array.Fill works with 2D array
        for (int i = 0; i < num_rows; i++)
            for (int j = 0; j < num_durations; j++)
                rates_array[i, j] = float.NaN;
    }

    // fill in rates_array from fred_interest_rates dataframe for durations 1, 7, 30, 60, 90, 180, 360.
    // Each of those columns will still have NaN's because fred_interest_rates dataframe does not have values
    // values for weekends or holidays. Also, most columns representing durations other than 1, 7, 30, etc.,
    // will still have all NaN's. Then, replace NaN's by interpolating using non-NaN's
    void FillRatesArrayFromFRED() {
        int num_rows = rates_array.GetLength(0);
        foreach (var (duration, data) in fred_interest_rates) {
            int index_of_first_rate = -1, index_of_last_rate = -1;
            foreach (var (date, rate) in data) {
                if (date < rates_global_first_date)
                    continue;
                if (date > rates_global_last_date)
                    break;
                int date_index = (date - rates_global_first_date).Days;
                Debug.Assert(date_index >= 0);
                Debug.Assert(date_index < num_rows);
                if (!float.IsNaN(rate)) {
                    rates_array[date_index, duration] = rate;
                    // keep track of first and last rates which are not NaN
                    if (index_of_first_rate == -1)
                        index_of_first_rate = date_index;
                    index_of_last_rate = date_index;
                }
            }

            InterpolateRatesArrayColumn(duration, index_of_first_rate, index_of_last_rate);
        }

        // for each row in rates_array (representing a date), fill in durations that are NaN's 
        InterpolateRatesArrayRow();
    }

    // Replace NaN's in rates_array by piecewise linear interpolation using non-NaN values that surround the NaN's
    // This is only for columns that represent durations read from the FRED database (other columns will be all NaN's)
    void InterpolateRatesArrayColumn(int duration, int index_of_first_rate, int index_of_last_rate) {
        int num_rows = rates_array.GetLength(0);
        // for this duration, get index of first NaN, index of last NaN
        if (index_of_first_rate == -1)
            throw new InvalidFredDataException(seriesNames[duration], $"All data is missing for duration {duration}");
        // fill front of column with first rate that is not NaN
        float first_rate = rates_array[index_of_first_rate, duration];
        for (int i = 0; i < index_of_first_rate; i++)
            rates_array[i, duration] = first_rate;
        // fill back of column with last rate that is not NaN
        float last_rate = rates_array[index_of_last_rate, duration];
        for (int i = index_of_last_rate + 1; i < num_rows; i++)
            rates_array[i, duration] = last_rate;

        // interpolate between interior NaN's for this duration
        int i_row = index_of_first_rate;
        Debug.Assert(!float.IsNaN(rates_array[0, duration]));
        while (i_row < index_of_last_rate) {
            // find index of first NaN
            if (!float.IsNaN(rates_array[i_row, duration])) {
                i_row++;
                continue;
            }
            // we found a NaN in rates_array[i_row, duration]

            // find next non-NaN. This will be found because we filled back of array with non-NaN's
            int j;
            for (j = i_row + 1; j < index_of_last_rate; j++) {
                if (!float.IsNaN(rates_array[j, duration]))
                    break;
            }
            Debug.Assert(j < index_of_last_rate); // assert we found an Nan
            float next_value = rates_array[j, duration]; // next_value is first rate that is not NaN after Nan in i_row
            Debug.Assert(!float.IsNaN(next_value));

            // interpolate between non-NaN in i_row-1 and non-Nan in j;
            int index_range = j - (i_row - 1);
            float value = rates_array[i_row - 1, duration]; // starting value
            float value_range = next_value - value;
            float value_increment = value_range / index_range;

            for (int nan_index = i_row; nan_index < j; nan_index++) {
                value = value + value_increment;
                rates_array[nan_index, duration] = value;

                i_row = j + 1; // set i_row to next row to look for NaN (since we know row j is not NaN)
            }
        }
    }

    // Each row in rates_array contains values for all durations for a given date, but durations that were not read from FRED
    // will have NaN's. Replace those NaN's by piecewise linear interpolation using non-NaN values that surround the NaN's
    // We know that the beginning and ending elements of each row ARE NOT NaN's because they represent durations of 1 and 360
    // that were read from FRED (column 0 of rates_array is unused and remains all NaN)

    void InterpolateRatesArrayRow() {

    }

    // interpolates values between vector[col1] and vector[col2]
    // vector[col1] and vector[col2] MUST NOT be nan's.
    void Interpolate(float[] vector, int col1, int col2) {
        Debug.Assert(!float.IsNaN(vector[col1]));
        Debug.Assert(!float.IsNaN(vector[col2]));

        int idx_range = col2 - col1;
        float interpolated_value = vector[col1];  // starting value
        float value_range = vector[col2] - interpolated_value;
        float value_increment = value_range / idx_range;
        for (int i = col1 + 1; i < col2; i++) {
            interpolated_value = interpolated_value + value_increment;
            vector[i] = interpolated_value;
        }
    }

    public float RiskFreeRate(DateTime requestedDate, int duration) {
        Debug.Assert(requestedDate >= rates_global_first_date,
            $"FredRateReader.cs::RiskFreeRate: requested date ({requestedDate.Date}) is before earliest available ({rates_global_first_date.Date}).");
        Debug.Assert(requestedDate <= rates_global_last_date,
            $"FredRateReader.cs::RiskFreeRate: requested date ({requestedDate.Date}) is after latest available ({rates_global_last_date.Date}).");

        int date_index = (requestedDate - rates_global_first_date).Days;
        return rates_array[date_index, duration];
    }
}

public class SP500DividenYieldReader {
    bool dividends_valid = false;
    List<float> dividend_array = new();  // vector containing sp500 dividend yield in percent
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
