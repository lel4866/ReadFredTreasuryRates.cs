using ReadFredTreasuryRates;

Console.WriteLine("Hello, World!");
//var rate_reader = new FredRateReader(new DateTime(2000, 1, 1));
var dividend_reader = new SP500DividendYieldReader(new DateTime(2010, 1, 1));