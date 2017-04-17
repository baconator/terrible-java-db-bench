package io.github.baconator.benchmarks.sqliteinsert1

import com.google.common.collect.Lists
import java.nio.file.FileSystems
import java.nio.file.Files
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.*
import java.util.concurrent.Executors

/**
 * @property batchSizes The # of rows in insert statements sent to the DB in a single transaction.
 * @property insertTimes How long each insert transaction took, in ns.
 */
data class Stats(val batchSizes: LongSummaryStatistics = LongSummaryStatistics(), val insertTimes: LongSummaryStatistics = LongSummaryStatistics()) {
    fun accept(size: Long, time: Long) {
        batchSizes.accept(size)
        insertTimes.accept(time)
    }
}

/**
 * A test function.
 * @property name The name of this test. Used for e.g. printing out test results.
 */
class TestF(val name: String, val f: (Set<Row>, TestBuilder, Stats) -> Unit) {
    fun execute(testData: Set<Row>, testBuilder: TestBuilder, stats: Stats) {
        return f.invoke(testData, testBuilder, stats)
    }
}

fun main(args: Array<String>) {
    val now = System.nanoTime()
    val sampleSize = 10000
    val sourceData = generateTestData(sampleSize * 2)
    val testData = sourceData.take(sampleSize).toSet()
    val preinsertedData = sourceData.drop(sampleSize).take(sampleSize).toSet()
    val maxDurationMs = 5 * 1000L;
    val filename = "benchmark-$now.db"
    val backgroundPool = Executors.newScheduledThreadPool(6)
    val singleBatchSize = 10;
    val singleInsert = TestF("Individual insert") { testData, connection, stats ->
        testData.asSequence().map { row ->
            val start = System.nanoTime()
            connection.insertStatement(row) { prepared ->
                val result = prepared.execute()
                val end = System.nanoTime()
                assert(result, { -> "Failed to insert a row ..." })
                end - start
            }
        }.forEach { stats.accept(1, it) }
    }
    val largeBatchInsert = TestF("Large batch insert") { testData, connection, stats ->
        val testDataArray = testData.toTypedArray()
        val start = System.nanoTime()
        connection.batchInsertStatement(testDataArray) { prepared ->
            val result = prepared.execute()
            val end = System.nanoTime()
            assert(result, { -> "Failed to insert a row ..." })
            stats.accept(testDataArray.size.toLong(), end - start)
        }
    }
    val smallBatchInsert = TestF("Small batch insert") { testData, connection, stats ->
        Lists.partition(testData.toList(), singleBatchSize).asSequence().forEach { batch ->
            val batchArray = batch.toTypedArray()
            val start = System.nanoTime()
            connection.batchInsertStatement(batchArray) { prepared ->
                val result = prepared.execute()
                val end = System.nanoTime()
                assert(result, { -> "Failed to insert a row ..." })
                stats.accept(batch.size.toLong(), end - start)
            }
        }
    }
    try {
        DriverManager.getConnection("jdbc:sqlite:$filename").use { c ->
            TestBuilder(c).syncOff().prepareTable().preinsertData(preinsertedData).runTest(testData, maxDurationMs, backgroundPool, largeBatchInsert).print()
            TestBuilder(c).syncOn().prepareTable().preinsertData(preinsertedData).runTest(testData, maxDurationMs, backgroundPool, largeBatchInsert).print()
            TestBuilder(c).syncOff().prepareTable().preinsertData(preinsertedData).runTest(testData, maxDurationMs, backgroundPool, smallBatchInsert).print()
            TestBuilder(c).syncOn().prepareTable().preinsertData(preinsertedData).runTest(testData, maxDurationMs, backgroundPool, smallBatchInsert).print()
            TestBuilder(c).syncOff().prepareTable().preinsertData(preinsertedData).runTest(testData, maxDurationMs, backgroundPool, singleInsert).print()
            TestBuilder(c).syncOn().prepareTable().preinsertData(preinsertedData).runTest(testData, maxDurationMs, backgroundPool, singleInsert).print()
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

