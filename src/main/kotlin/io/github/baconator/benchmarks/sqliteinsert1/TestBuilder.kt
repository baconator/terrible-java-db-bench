package io.github.baconator.benchmarks.sqliteinsert1

import java.sql.Connection
import java.sql.PreparedStatement
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * Indirection layer for raw connections.
 * @property syncOn Whether 'synchronous' is active or not in sqlite (see [https://sqlite.org/pragma.html#pragma_synchronous])
 * @property stats The stats object that results from test execution.
 * @property testFun The test which will modify a provided [Stats] object.
 * @constructor Creates an indirection layer for raw connections.
 */
class TestBuilder(val connection: Connection) {
    var syncOn: Boolean = false
    var stats: Stats? = null
    var testFun: TestF? = null
    fun syncOff(): TestBuilder {
        connection.createStatement().use { it.execute("pragma synchronous=off;") }
        syncOn = false
        return this
    }

    fun syncOn(): TestBuilder {
        connection.createStatement().use { it.execute("pragma synchronous=on;") }
        syncOn = true
        return this
    }

    /**
     * Drops the 'benchmark' table, then creates some indices on it.
     */
    fun prepareTable(): TestBuilder {
        connection.createStatement().use { statement ->
            statement.queryTimeout = 30
            statement.executeUpdate("drop table if exists benchmark;")
            statement.executeUpdate("create table benchmark(i1 double, i2 double, o1 double, o2 double, fitness double, primary key(i1, i2));")
            statement.executeUpdate("create index fitness on benchmark (fitness);")
            statement.executeUpdate("create index outputs on benchmark (o1, o2);")
        }
        return this
    }

    fun <R> prepareStatement(queryString: String, f: (PreparedStatement) -> R): R {
        return connection.prepareStatement(queryString).use(f)
    }

    /**
     * Takes a row and creates a prepared statement inserting one of it into the DB.
     */
    fun <R> insertStatement(row: Row, f: (PreparedStatement) -> R): R {
        return prepareStatement("insert into benchmark(i1, i2, o1, o2, fitness) values (?, ?, ?, ?, ?);") { prepared ->
            row.applyToStatement(prepared)
            f.invoke(prepared)
        }
    }

    /**
     * Takes multiple rows and creates a prepared statement insertion them into the DB.
     */
    fun <R> batchInsertStatement(testData: Array<Row>, f: (PreparedStatement) -> R): R {
        val queryString = "insert into benchmark(i1, i2, o1, o2, fitness) values ${(0..testData.size - 1).map { "(?, ?, ?, ?, ?)" }.joinToString(",")};"
        return prepareStatement(queryString) { prepared ->
            testData.forEachIndexed({ i, row ->
                val base = i * 5 + 1
                row.applyToStatement(prepared, base)
            })
            f.invoke(prepared)
        }
    }

    /**
     * Executes a test with some given data, a max timeout and an execution pool (so that the test can be timed out).
     */
    fun runTest(testData: Set<Row>, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: TestF): TestBuilder {
        val stats = Stats()
        val background = backgroundPool.schedule({ test.execute(testData, this, stats) }, 0, TimeUnit.MILLISECONDS)
        try {
            background.get(maxDurationMs, TimeUnit.MILLISECONDS)
        } catch(e: TimeoutException) {
            print("Cancelled thread after $maxDurationMs ms. ")
        }
        this.stats = stats
        this.testFun = test
        return this
    }

    fun print() {
        println("${testFun?.name} (sync: $syncOn): $stats")
    }

    /**
     * Inserts a bunch of extra garbage. Intended to bloat the size of the DB before testing.
     */
    fun preinsertData(data: Set<Row>): TestBuilder {
        batchInsertStatement(data.toTypedArray()) { prepared ->
            prepared.execute()
        }
        return this
    }
}