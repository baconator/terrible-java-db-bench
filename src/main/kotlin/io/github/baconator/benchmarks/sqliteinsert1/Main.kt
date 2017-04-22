package io.github.baconator.benchmarks.sqliteinsert1

import com.google.common.collect.Lists
import java.nio.file.FileSystems
import java.nio.file.Files
import java.sql.Connection
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

interface Database {
    val filename: String
    val connectionString: String
    val name: String
    fun changeSync(connection: Connection, syncState: Boolean)
    fun cleanup()
}

class SqliteDb(override val filename: String) : Database {
    override val name = "SQLite"

    override fun cleanup() {
        val fs = FileSystems.getDefault()
        Files.delete(fs.getPath("./$filename"))
    }

    override fun changeSync(connection: Connection, syncState: Boolean) {
        if (syncState) {
            connection.createStatement().use { it.execute("pragma synchronous=on;") }
        } else {
            connection.createStatement().use { it.execute("pragma synchronous=off;") }
        }
    }

    override val connectionString: String
        get() = "jdbc:sqlite:./$filename"
}

class H2Db(override val filename: String) : Database {
    override val name = "H2Db"

    override fun cleanup() {
        val fs = FileSystems.getDefault()
        Files.delete(fs.getPath("./$filename.mv.db"))
    }

    override fun changeSync(connection: Connection, syncState: Boolean) {
        // noop
    }

    override val connectionString: String
        get() = "jdbc:h2:./$filename" //To change initializer of created properties use File | Settings | File Templates.
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

typealias Step = (TestBuilder) -> TestBuilder

fun prepTest(selectedSteps: List<Step>, builder: () -> TestBuilder, optionalStepGroups: List<List<Step>>): List<() -> TestBuilder> =
        if (optionalStepGroups.isEmpty()) {
            listOf({ selectedSteps.fold(builder.invoke(), { b, s -> s.invoke(b) }) })
        } else {
            optionalStepGroups.first().flatMap { optionalStep ->
                prepTest(selectedSteps + listOf(optionalStep), builder, optionalStepGroups.drop(1))
            }
        }


fun prepTest(builder: () -> TestBuilder, optionalStepGroups: List<List<Step>>): List<() -> TestBuilder> =
        prepTest(listOf(), builder, optionalStepGroups)

fun main(args: Array<String>) {
    val sampleSize = 10000

    // Create some data for insertion later on.
    val sourceData = generateTestData(sampleSize * 2)
    val testData = sourceData.take(sampleSize).toSet()
    val preinsertedData = sourceData.drop(sampleSize).take(sampleSize).toSet() // Used to bulk up db before entering data.

    val benchDbFilename = "benchmark-${System.nanoTime()}.db"
    val maxTestDurationMs = 5 * 1000L // Max duration of a test before it's forcibly stopped (in ms).
    val singleBatchSize = 10 // Defines how large a given 'small' batch should be. Large batches = sampleSize.

    // Setup the different payloads for testing.
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
            val result = try {
                prepared.execute()
            } catch(e: Exception) {
                throw e
            }
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

    // The background pool is used for killing tests after maxTestDurationMs milliseconds.
    val backgroundPool = Executors.newScheduledThreadPool(6)
    try {
        Class.forName("org.h2.Driver")
        val dbBuilders = listOf({ H2Db(benchDbFilename) }, { SqliteDb(benchDbFilename) })
        val steps: List<List<Step>> = listOf(
                listOf({ b -> b.syncOff() },
                        { b -> b.syncOn() }),
                listOf({ b -> b.prepareTable() }),
                listOf({ b -> b.preinsertData(preinsertedData) }),
                listOf({ b -> b.runTest(testData, maxTestDurationMs, backgroundPool, largeBatchInsert) },
                        { b -> b.runTest(testData, maxTestDurationMs, backgroundPool, smallBatchInsert) },
                        { b -> b.runTest(testData, maxTestDurationMs, backgroundPool, singleInsert) }),
                listOf({ b -> b.print() })
        )

        dbBuilders.forEach { db ->
            val prepTest = prepTest({ TestBuilder(db.invoke()) }, steps).toList()
            prepTest.forEach { test ->
                test.invoke().use { }
            }
        }
    } catch(e: Exception) {
        e.printStackTrace()
    }
    backgroundPool.shutdownNow()
    /*try {
        val fs = FileSystems.getDefault()
        Files.delete(fs.getPath("./$benchDbFilename"))
    } catch(e: Exception) {
        e.printStackTrace()
    }*/
}

/**
 * input1 + input2 are primary keys, output1, output2 and fitness are indexed.
 */
class Row(val input1: Float, val input2: Float, val output1: Float, val output2: Float, val fitness: Float) {
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
        output.add(Row(random.nextFloat(), random.nextFloat(), random.nextFloat(), random.nextFloat(), random.nextFloat()))
    }
    return output
}

