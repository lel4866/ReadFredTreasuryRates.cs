using System;
using System.Diagnostics;
using ReadFredTreasuryRates;


//
// test of RiskFreeRates
//
var stopWatch = new Stopwatch();
stopWatch.Start();
var rate_reader = new FredRateReader(new DateTime(2000, 1, 1));
stopWatch.Stop();
Console.WriteLine($"FredRateReader: Elapsed time={stopWatch.ElapsedMilliseconds / 1000.0} seconds");

DateTime date0 = new DateTime(2020, 6, 15);
float rate1 = rate_reader.RiskFreeRate(date0, 1);
float rate9 = rate_reader.RiskFreeRate(date0, 9);
float rate47 = rate_reader.RiskFreeRate(date0, 47);
float rate200 = rate_reader.RiskFreeRate(date0, 200);
float rate360 = rate_reader.RiskFreeRate(date0, 360);
float rate200t = rate_reader.RiskFreeRate(DateTime.Today, 200);

//
// test of DividendYield
//
stopWatch.Restart();
var dividend_reader = new SP500DividendYieldReader(new DateTime(2010, 1, 1));
stopWatch.Stop();
Console.WriteLine($"SP500DividendYieldReader: Elapsed time={stopWatch.ElapsedMilliseconds / 1000.0} seconds");

var yield1 = dividend_reader.DividendYield(new DateTime(2016, 6, 1));
var yield2 = dividend_reader.DividendYield(DateTime.Today);

