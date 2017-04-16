package io.github.baconator.benchmarks.sqliteinsert1

import com.google.common.collect.Lists
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

class TestBuilder(val connection: Connection) {
    fun withTestSyncOff(): TestBuilder {
        connection.createStatement().use { it.execute("pragma synchronous=off;") }
        return this
    }

    fun withTestSyncOn(): TestBuilder {
        connection.createStatement().use { it.execute("pragma synchronous=on;") }
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
        val singleBatchSize = 10;
        DriverManager.getConnection("jdbc:sqlite:$filename").use { connection ->
            val singleInsert: (Stats) -> Unit = { stats ->
                testData.asSequence().map { row ->
                    val start = System.nanoTime()
                    connection.prepareStatement("insert into benchmark(i1, i2, o1, o2, fitness) values (?, ?, ?, ?, ?);").use { prepared ->
                        row.applyToStatement(prepared)
                        val result = prepared.execute()
                        val end = System.nanoTime()
                        assert(result, { -> "Failed to insert a row ..." })
                        end - start
                    }
                }.forEach { stats.accept(1, it) }
            }
            val largeBatchInsert: (Stats) -> Unit = { stats ->
                val start = System.nanoTime()
                makeBatchInsertStatement(connection, testDataArray).use { prepared ->
                    val result = prepared.execute()
                    val end = System.nanoTime()
                    assert(result, { -> "Failed to insert a row ..." })
                    stats.accept(testDataArray.size.toLong(), end - start)
                }
            }
            val smallBatchInsert: (Stats) -> Unit = { stats ->
                Lists.partition(testData.toList(), singleBatchSize).asSequence().forEach { batch ->
                    val start = System.nanoTime()
                    makeBatchInsertStatement(connection, batch.toTypedArray()).use { prepared ->
                        val result = prepared.execute()
                        val end = System.nanoTime()
                        assert(result, { -> "Failed to insert a row ..." })
                        stats.accept(batch.size.toLong(), end - start)
                    }
                }
            }
            println("Single insert stats (sync off): ${timeTest(TestBuilder(connection).withTestSyncOff(), maxDurationMs, backgroundPool, smallBatchInsert)}")
            println("Single insert stats (sync on): ${timeTest(TestBuilder(connection).withTestSyncOn(), maxDurationMs, backgroundPool, singleInsert)}")
            println("Large batch insert stats (sync off): ${timeTest(TestBuilder(connection).withTestSyncOff(), maxDurationMs, backgroundPool, largeBatchInsert)}")
            println("Large batch insert stats (sync on): ${timeTest(TestBuilder(connection).withTestSyncOn(), maxDurationMs, backgroundPool, largeBatchInsert)}")
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

fun timeTest(connection: TestBuilder, maxDurationMs: Long, backgroundPool: ScheduledExecutorService, test: (Stats) -> Unit): Stats {
    connection.prepareTable()
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