package test_dataframes

import com.google.common.base.Stopwatch
import krangl.*

/**
 * Test the API of krangl to do some basic dataframe manipulations.
 *
 * https://github.com/holgerbrandl/krangl
 *
 * See https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437
 * for more information.
 */
fun main() {
    val data = DataFrame.readCSV("urb_cpop1_1_Data.csv")

    val watch = Stopwatch.createStarted()
    // remove missing values indicated with ":", convert column to IntCol
    val filtered = data.filter { !(it["Value"] eq ":") }.addColumn("Value") {
        it["Value"].map(String::toInt)
    }
    // replace duplicated rows with mean value, create pivot table
    val cities = filtered.groupBy("CITIES", "INDIC_UR", "TIME")
            .summarize("Value" to { it["Value"].mean() })
            .spread("TIME", "Value").filter {
                it["INDIC_UR"].isMatching<String> { endsWith("January, total") }
            }

    println(cities.select("CITIES", "2017").sortedByDescending("2017").head(10))

    val highestGrowthTable = cities.addColumn("growth") {
        (it["2016"] / it["2010"] - 1.0) * 100.0
    }.sortedByDescending("growth")

    println(highestGrowthTable.select("CITIES", "growth").head(10))

    CheckResult.checkResult(highestGrowthTable["CITIES"].asType<String>().toList())
    println("Total time: $watch")
}