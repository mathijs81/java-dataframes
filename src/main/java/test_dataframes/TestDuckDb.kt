package test_dataframes

import com.google.common.base.Stopwatch
import tech.tablesaw.api.Table
import java.sql.DriverManager


/**
 * Test duckdb to do some basic dataframe manipulations.
 *
 * See https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437
 * for more information.
 */
fun main() {
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    val stmt = conn.createStatement()
    var rs = stmt.executeQuery("SELECT * FROM 'urb_cpop1_1_Data.csv'")
    Table.read().db(rs).print().also { println(it) }

    val watch = Stopwatch.createStarted()
    stmt.execute(
        """
         CREATE TEMP TABLE t1 AS (
             WITH cities AS (
                SELECT CITIES || ':' || INDIC_UR as key,
                CAST(Value AS INTEGER) as Value,
                * EXCLUDE (CITIES, INDIC_UR, Value)
                FROM 'urb_cpop1_1_Data.csv' WHERE Value != ':'),
             pivot_table AS (
                 PIVOT cities
                 ON TIME
                 USING AVG(Value)
                 GROUP BY key
             )
             SELECT *, ("2016"::REAL / "2010"::REAL - 1.0 ) * 100.0 as growth
             FROM pivot_table
             WHERE suffix(key, 'January, total')
             ORDER BY growth DESC
         )
     """
    )
    rs = stmt.executeQuery("SELECT * FROM t1")
    Table.read().db(rs).print().also { println(it) }
    val result = stmt.executeQuery("SELECT key FROM t1").use { r ->
        mutableListOf<String>().apply {
            while (r.next()) {
                this += r.getString("key")
            }
        }
    }
    CheckResult.checkResult(result)
    println("Total time: $watch")
}