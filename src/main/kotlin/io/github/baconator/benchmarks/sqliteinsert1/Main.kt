package io.github.baconator.benchmarks.sqliteinsert1

import java.nio.file.FileSystems
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.*
import java.util.concurrent.*

data class Stats(val batchSizes: LongSummaryStatistics = LongSummaryStatistics(), val insertTimes: LongSummaryStatistics = LongSummaryStatistics()) {
    fun accept(size: Long, time: Long) {
        batchSizes.accept(size)
        insertTimes.accept(time)
    }
}

fun main(args: Array<String>) {
    val now = System.nanoTime()
    val sampleSize = 10000
    val testData = generateTestData(sampleSize)
    val testDataArray = testData.toTypedArray() // In case this is terribly slow
    val maxDurationMs = 5 * 1000L;
    val filename = "benchmark-$now.db"
    val backgroundPool = Executors.newScheduledThreadPool(6)
    val streamingPool = Executors.newScheduledThreadPool(6)
    try {
        val batchPauseMs = 10L
        val singleValueCreatedMs = 1L
        DriverManager.getConnection("jdbc:sqlite:$filename").use { connection ->
            val singleInsert: (Stats) -> Unit = { stats ->
                testData.asSequence().map { row ->
                    val start = System.nanoTime()
                    val prepared = connection.prepareStatement("insert into benchmark(i1, i2, o1, o2, fitness) values (?, ?, ?, ?, ?);")
                    row.applyToStatement(prepared)
                    val result = prepared.execute()
                    val end = System.nanoTime()
                    assert(result, { -> "Failed to insert a row ..." })
                    end - start
                }.forEach { stats.accept(1, it) }
            }
            val largeBatchInsert: (Stats) -> Unit = { stats ->
                val start = System.nanoTime()
                val prepared = makeBatchInsertStatement(connection, testDataArray)
                val result = prepared.execute()
                val end = System.nanoTime()
                assert(result, { -> "Failed to insert a row ..." })
                stats.accept(testDataArray.size.toLong(), end - start)
            }
            val streamingBatchInsert: (Stats) -> Unit = { stats ->
                val queue = ConcurrentLinkedQueue<Row>()
                val orderedTestData = testData.toList()

                var dataPosition = 0
                val stopLatch = CountDownLatch(1)
                val generation = streamingPool.scheduleAtFixedRate({
                    synchronized(dataPosition) {
                        queue.add(orderedTestData[dataPosition])
                        dataPosition += 1
                        if (dataPosition >= orderedTestData.size) {
                            throw Exception("This is so terrible. Eh.")
                        }
                    }
                }, 0, singleValueCreatedMs, TimeUnit.MILLISECONDS)
                val insertion = streamingPool.scheduleAtFixedRate({
                    val batch = queue.toTypedArray()
                    if (!batch.isEmpty()) {
                        queue.removeAll(batch)
                        val start = System.nanoTime()
                        val prepared = makeBatchInsertStatement(connection, batch)
                        val result = prepared.execute()
                        val end = System.nanoTime()
                        assert(result, { -> "Failed to insert a row ..." })
                        stats.accept(batch.size.toLong(), end - start)
                    }
                    if (generation.isDone) {
                        stopLatch.countDown()
                    }
                }, 0, batchPauseMs, TimeUnit.MILLISECONDS)
                stopLatch.await()
                generation.cancel(true)
                insertion.cancel(true)
            }
            println("Single insert stats (sync off): ${timeTestSyncOff(connection, maxDurationMs, backgroundPool, streamingBatchInsert)}")
            /*println("Single insert stats (sync on): ${timeTestSyncOn(connection, maxDurationMs, backgroundPool, singleInsert)}")
            println("Large batch insert stats (sync off): ${timeTestSyncOff(connection, maxDurationMs, backgroundPool, largeBatchInsert)}")
            println("Large batch insert stats (sync on): ${timeTestSyncOn(connection, maxDurationMs, backgroundPool, largeBatchInsert)}")*/
        }
    } catch(e: Exception) {
        e.printStackTrace()
    }
    backgroundPool.shutdownNow()
    streamingPool.shutdownNow()
    try {
        val fs = FileSystems.getDefault()
        Files.delete(fs.getPath("./$filename"))
    } catch(e: Exception) {
        e.printStackTrace()
    }
}

private fun makeBatchInsertStatement(connection: Connection, testData: Array<Row>): PreparedStatement {
    val prepared = connection.prepareStatement("insert into benchmark(i1, i2, o1, o2, fitness) values ${
    (0..testData.size - 1).map { "(?, ?, ?, ?, ?)" }.joinToString(",")
    };")
    testData.forEachIndexed({ i, row ->
        val base = i * 5 + 1;
        row.applyToStatement(prepared, base)
    })
    return prepared
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

fun timeTestSyncOff(connection: Connection, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: (Stats) -> Unit): Stats {
    connection.createStatement().use { it.execute("pragma synchronous=off;") }
    return timeTest(connection, maxDurationMs, backgroundPool, test)
}

fun timeTestSyncOn(connection: Connection, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: (Stats) -> Unit): Stats {
    connection.createStatement().use { it.execute("pragma synchronous=on;") }
    return timeTest(connection, maxDurationMs, backgroundPool, test)
}

fun timeTest(connection: Connection, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: (Stats) -> Unit): Stats {
    connection.createStatement().use { statement ->
        statement.queryTimeout = 30
        statement.executeUpdate("drop table if exists benchmark;")
        statement.executeUpdate("create table benchmark(i1 double, i2 double, o1 double, o2 double, fitness double, primary key(i1, i2));")
        statement.executeUpdate("create index fitness on benchmark (fitness);")
        statement.executeUpdate("create index outputs on benchmark (o1, o2);")
    }
    val stats = Stats()
    runForMs({ test(stats) }, maxDurationMs, backgroundPool)
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