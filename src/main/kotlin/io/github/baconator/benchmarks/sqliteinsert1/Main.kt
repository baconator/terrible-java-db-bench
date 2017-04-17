package io.github.baconator.benchmarks.sqliteinsert1

import com.google.common.collect.Lists
import java.nio.file.FileSystems
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

data class Stats(val batchSizes: LongSummaryStatistics = LongSummaryStatistics(), val insertTimes: LongSummaryStatistics = LongSummaryStatistics()) {
    fun accept(size: Long, time: Long) {
        batchSizes.accept(size)
        insertTimes.accept(time)
    }
}

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

    fun <R> batchInsertStatement(testData: Array<Row>, f: (PreparedStatement) -> R): R {
        val queryString = "insert into benchmark(i1, i2, o1, o2, fitness) values ${(0..testData.size - 1).map { "(?, ?, ?, ?, ?)" }.joinToString(",")};"
        return prepareStatement(queryString) { prepared ->
            testData.forEachIndexed({ i, row ->
                val base = i * 5 + 1;
                row.applyToStatement(prepared, base)
            })
            f.invoke(prepared)
        }
    }

    fun runTest(testData: Set<Row>, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: TestF): TestBuilder {
        val stats = Stats()
        runForMs({ test.execute(testData, this, stats) }, maxDurationMs, backgroundPool)
        this.stats = stats
        this.testFun = test
        return this
    }

    fun print() {
        println("${testFun?.name} (sync: $syncOn): $stats")
    }
}

class TestF(val name: String, val f: (Set<Row>, TestBuilder, Stats) -> Unit) {
    fun execute(testData: Set<Row>, testBuilder: TestBuilder, stats: Stats) {
        return f.invoke(testData, testBuilder, stats)
    }
}

fun main(args: Array<String>) {
    val now = System.nanoTime()
    val sampleSize = 10000
    val testData = generateTestData(sampleSize)
    val maxDurationMs = 5 * 1000L;
    val filename = "benchmark-$now.db"
    val backgroundPool = Executors.newScheduledThreadPool(6)
    val singleBatchSize = 10;
    val singleInsert = TestF("Individual insert") { testData, connection, stats ->
        testData.asSequence().map { row ->
            val start = System.nanoTime()
            connection.prepareStatement("insert into benchmark(i1, i2, o1, o2, fitness) values (?, ?, ?, ?, ?);") { prepared ->
                row.applyToStatement(prepared)
                val result = prepared.execute()
                val end = System.nanoTime()
                assert(result, { -> "Failed to insert a row ..." })
                end - start
            }
        }.forEach { stats.accept(1, it) }
    }
    val largeBatchInsert = TestF("Large batch insert") { testData, connection, stats ->
        val start = System.nanoTime()
        val testDataArray = testData.toTypedArray() // In case this is terribly slow
        connection.batchInsertStatement(testDataArray) { prepared ->
            val result = prepared.execute()
            val end = System.nanoTime()
            assert(result, { -> "Failed to insert a row ..." })
            stats.accept(testDataArray.size.toLong(), end - start)
        }
    }
    val smallBatchInsert = TestF("Small batch insert") { testData, connection, stats ->
        Lists.partition(testData.toList(), singleBatchSize).asSequence().forEach { batch ->
            val start = System.nanoTime()
            connection.batchInsertStatement(batch.toTypedArray()) { prepared ->
                val result = prepared.execute()
                val end = System.nanoTime()
                assert(result, { -> "Failed to insert a row ..." })
                stats.accept(batch.size.toLong(), end - start)
            }
        }
    }
    try {
        DriverManager.getConnection("jdbc:sqlite:$filename").use { c ->
            TestBuilder(c).syncOff().prepareTable().runTest(testData, maxDurationMs, backgroundPool, largeBatchInsert).print()
            TestBuilder(c).syncOn().prepareTable().runTest(testData, maxDurationMs, backgroundPool, largeBatchInsert).print()
            TestBuilder(c).syncOff().prepareTable().runTest(testData, maxDurationMs, backgroundPool, smallBatchInsert).print()
            TestBuilder(c).syncOn().prepareTable().runTest(testData, maxDurationMs, backgroundPool, smallBatchInsert).print()
            TestBuilder(c).syncOff().prepareTable().runTest(testData, maxDurationMs, backgroundPool, singleInsert).print()
            TestBuilder(c).syncOn().prepareTable().runTest(testData, maxDurationMs, backgroundPool, singleInsert).print()
        }
    } catch(e: Exception) {
        e.printStackTrace()
    }
    backgroundPool.shutdownNow()
    try {
        val fs = FileSystems.getDefault()
        Files.delete(fs.getPath("./$filename"))
    } catch(e: Exception) {
        e.printStackTrace()
    }
}

/**
 * input1 + input2 are primary keys, output1, output2 and fitness are indexed.
 */
class Row(val input1: Double, val input2: Double, val output1: Double, val output2: Double, val fitness: Double) {
    override fun equals(other: Any?): Boolean {
        val casted = other as? Row ?: return false
        return input1 == casted.input1 && input2 == casted.input2
    }

    override fun hashCode(): Int {
        return Objects.hash(input1, input2)
    }

    fun applyToStatement(prepared: PreparedStatement, base: Int = 1) {
        prepared.setObject(base + 0, this.input1)
        prepared.setObject(base + 1, this.input2)
        prepared.setObject(base + 2, this.output1)
        prepared.setObject(base + 3, this.output2)
        prepared.setObject(base + 4, this.fitness)
    }
}

fun generateTestData(sampleSize: Int): Set<Row> {
    val output = mutableSetOf<Row>()
    val random = Random()
    while (output.size < sampleSize) {
        output.add(Row(random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble()))
    }
    return output
}

fun runTest(connection: TestBuilder, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: (TestBuilder, Stats) -> Unit): Stats {
    val stats = Stats()
    runForMs({ test(connection, stats) }, maxDurationMs, backgroundPool)
    return stats
}

private fun runForMs(f: () -> Unit, maxDurationMs: Long, pool: ScheduledExecutorService) {
    val background = pool.schedule(f, 0, TimeUnit.MILLISECONDS)
    try {
        background.get(maxDurationMs, TimeUnit.MILLISECONDS)
    } catch(e: TimeoutException) {
        print("Cancelled thread after $maxDurationMs ms. ")
    }
}

fun timeBatchInserts(connection: Connection, testData: Set<Row>) {}

fun cleanup() {}