SELECT '--- Negative DateTime64';
SELECT dateTruncAllowNegative('year', toDateTime64('1960-03-03 12:55:55', 2));
SELECT dateTruncAllowNegative('quarter', toDateTime64('1960-03-03 12:55:55', 2));
SELECT dateTruncAllowNegative('month', toDateTime64('1960-03-03 12:55:55', 2));
SELECT dateTruncAllowNegative('week', toDateTime64('1960-03-03 12:55:55', 2));
SELECT toDayOfWeek(dateTruncAllowNegative('week', toDateTime64('1960-03-03 12:55:55', 2)));
SELECT dateTruncAllowNegative('day', toDateTime64('1960-03-03 12:55:55', 2));
SELECT dateTruncAllowNegative('hour', toDateTime64('1960-03-03 12:55:55', 2));
SELECT dateTruncAllowNegative('minute', toDateTime64('1960-03-03 12:55:55', 2));
SELECT dateTruncAllowNegative('second', toDateTime64('1960-03-03 12:55:55.1234', 4));
SELECT dateTruncAllowNegative('millisecond', toDateTime64('1960-03-03 12:55:55.1234', 4));

SELECT '--- Negative Date32';
SELECT dateTruncAllowNegative('year', toDate32('1960-03-03'));
SELECT dateTruncAllowNegative('quarter', toDate32('1960-03-03'));
SELECT dateTruncAllowNegative('month', toDate32('1960-03-03'));
SELECT dateTruncAllowNegative('week', toDate32('1960-03-03'));
SELECT toDayOfWeek(dateTruncAllowNegative('week', toDate32('1960-03-03')));
SELECT dateTruncAllowNegative('day', toDate32('1960-03-03'));

SELECT '--- Non-const arguments';
SELECT dateTruncAllowNegative('week', number % 2 = 0 ? toDate32('1970-03-03') : toDate32('1960-03-03')) FROM numbers(5);