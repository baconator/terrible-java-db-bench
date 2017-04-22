## Usage

Just build and run main. It'll execute some tests (printing info as it goes) and then output a CSV file with timing data in it (output.csv).

## Header Definitions

*	tablePreloaded
	*	Whether 10k random entries were inserted before adding another 10k random entries.
*	dbName
	*	The database backend (e.g. SQlite or H2DB)
*	fnName
	*	The name of the test that was run.
*	dbSyncMode
	*	If supported, whether that test was run in 'synchronous' mode. If unsupported, it'll ignore this parameter and run the test anyways.
*	batchCount
	*	How many rows are inserted in a single transaction.
*	batchSum
	*	How many rows are inserted in total. Note: there's a five second transaction time limit, so this can be less than 10k if it takes too long.
*	batchAverage
	*	Size of the average batch.
*	batchMin
	*	Size of the smallest batch.
*	batchMax
	*	Size of the largest batch.
*	transactionCount
	*	The number of batched transactions that were executed.
*	transactionTimeSum
	*	How long all of the transactions took, summed.
*	transactionTimeAverage
	*	How long each transaction took, on average.
*	transactionTimeMin
	*	The shortest transaction.
*	transactionTimeMax
	*	The longest transaction.
