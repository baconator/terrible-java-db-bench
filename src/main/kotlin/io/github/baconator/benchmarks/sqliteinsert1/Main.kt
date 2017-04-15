package io.github.baconator.benchmarks.sqliteinsert1

import java.nio.file.FileSystems
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

fun main(args: Array<String>) {
    val now = System.nanoTime()
    val sampleSize = 10000
    val testData = generateTestData(sampleSize)
    val maxDurationMs = 5 * 1000L;
    val filename = "benchmark-$now.db"
    val backgroundPool = Executors.newScheduledThreadPool(6)
    try {
        DriverManager.getConnection("jdbc:sqlite:$filename").use { connection ->
            val singleInsert: (LongSummaryStatistics) -> Unit = { stats ->
                testData.asSequence().map { row -> insertRow(connection, row) }.forEach { stats.accept(it) }
            }
            timeTestSyncOff(connection, maxDurationMs, backgroundPool, singleInsert)
            timeTestSyncOn(connection, maxDurationMs, backgroundPool, singleInsert)
            timeBatchInserts(connection, testData)
        }
    } catch(e: Exception) {
        e.printStackTrace()
    }
    backgroundPool.shutdown()
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
}

fun generateTestData(sampleSize: Int): Set<Row> {
    val output = mutableSetOf<Row>()
    val random = Random()
    while (output.size < sampleSize) {
        output.add(Row(random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble()))
    }
    return output
}

fun timeTestSyncOff(connection: Connection, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: (LongSummaryStatistics) -> Unit) {
    connection.createStatement().use { it.execute("pragma synchronous=off;") }
    timeTest(connection, maxDurationMs, backgroundPool, test)
}

fun timeTestSyncOn(connection: Connection, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: (LongSummaryStatistics) -> Unit) {
    connection.createStatement().use { it.execute("pragma synchronous=on;") }
    timeTest(connection, maxDurationMs, backgroundPool, test)
}

fun timeTest(connection: Connection, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: (LongSummaryStatistics) -> Unit) {
    connection.createStatement().use { statement ->
        statement.queryTimeout = 30
        statement.executeUpdate("drop table if exists benchmark;")
        statement.executeUpdate("create table benchmark(i1 double, i2 double, o1 double, o2 double, fitness double);")
    }
    val stats = LongSummaryStatistics()
    runForMs({ test(stats) }, maxDurationMs, backgroundPool)
    println("Single insert stats: $stats")
}

private fun runForMs(f: () -> Unit, maxDurationMs: Long, pool: ScheduledExecutorService) {
    val background = pool.schedule(f, 0, TimeUnit.MILLISECONDS)
    try {
        background.get(maxDurationMs, TimeUnit.MILLISECONDS)
    } catch(e: TimeoutException) {
        print("Cancelled thread after $maxDurationMs ms. ")
    }
}

/**
 * Inserts a single row; returns duration in ns.
 */
private fun insertRow(connection: Connection, row: Row): Long {
    val start = System.nanoTime()
    val prepared = connection.prepareStatement("insert into benchmark(i1, i2, o1, o2, fitness) values (?, ?, ?, ?, ?);")
    prepared.setObject(1, row.input1)
    prepared.setObject(2, row.input2)
    prepared.setObject(3, row.output1)
    prepared.setObject(4, row.output2)
    prepared.setObject(5, row.fitness)
    val result = prepared.execute()
    val end = System.nanoTime()
    assert(result, { -> "Failed to insert a row ..." })
    return end - start
}

fun timeBatchInserts(connection: Connection, testData: Set<Row>) {}

fun cleanup() {}