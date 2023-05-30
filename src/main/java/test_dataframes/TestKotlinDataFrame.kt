package test_dataframes

import com.google.common.base.Stopwatch
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.io.*
import org.jetbrains.kotlinx.dataframe.api.*

/**
 * Test the API of Kotlin Dataframes to do some basic dataframe manipulations.
 *
 * See https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437
 * for more information.
 */
fun main() {
    val df = DataFrame.read("urb_cpop1_1_Data.csv", delimiter = ',')
    df.print()
    val watch = Stopwatch.createStarted()
    val key by column<String>()
    // remove missing values indicated with ":", convert column to IntCol
    val filtered = df.filter { "Value"<String>() != ":" }
        .add(key) { "CITIES"<String>() + ":" + "INDIC_UR"<String>() }
        .convert { "Value"<String>() }.toInt()

    var cities = filtered.groupBy(key).pivot("TIME", inward = false).mean { "Value"<Int>() }
    cities.print()

    cities = cities.filter { key().endsWith("January, total") }.sortByDesc("2017")
    cities.print()

    // growth
    val highestGrowthTable =
        cities.filter { "2010"<Double?>() != null && "2016"<Double?>() != null }
            .add("growth") { ("2016"<Double>() / "2010"<Double>() - 1.0) * 100.0 }
            .sortByDesc("growth")
    highestGrowthTable.print()

    CheckResult.checkResult(highestGrowthTable[{ key }].toList())
    println("Total time: $watch")
}