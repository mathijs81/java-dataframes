package test_dataframes;

import static tech.tablesaw.aggregate.AggregateFunctions.mean;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

import com.google.common.base.Stopwatch;

/**
 * Test the API of tablesaw to do some basic dataframe manipulations.
 *
 * https://github.com/jtablesaw/tablesaw
 *
 * See https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437
 * for more information.
 */
public class TestTablesaw {
    public static void main(String[] args) {
        // This automatically makes the ":" values missing
        Table data = Table.read().csv(
            CsvReadOptions.builder("urb_cpop1_1_Data.csv").missingValueIndicator(":").build());
        System.out.println(data.print(5));

        Stopwatch watch = Stopwatch.createStarted();
        Table filtered = data.where(data.column("Value").isNotMissing());

        Table cities = filtered.summarize("Value", mean).by("CITIES", "INDIC_UR", "TIME");
        System.out.println(cities.print(10));

        // Need to transpose/pivot now too
        StringColumn key = filtered.stringColumn("CITIES")
            .join(":", filtered.stringColumn("INDIC_UR")).setName("key");
        filtered.addColumns(key);
        Table finalTable = filtered.pivot("key", "TIME", "Value", mean);

        // sortDescendingOn puts N/A values first unfortunately, so let's remove them
        // before determining and printing.
        Table existing2017 = finalTable.dropWhere(finalTable.column("2017").isMissing());
        System.out.println(filterTotalKeys(existing2017).sortDescendingOn("2017").print(20));

        // Add growth column
        DoubleColumn growthColumn = finalTable.doubleColumn("2016").divide(
            finalTable.doubleColumn("2010")).subtract(1).multiply(100);
        growthColumn.setName("growth");
        finalTable.addColumns(growthColumn);
        
        Table highestGrowthTable = filterTotalKeys(
            finalTable.dropWhere(finalTable.column("growth").isMissing())).sortDescendingOn(
                "growth");
        System.out.println(highestGrowthTable.print(20));
        CheckResult.checkResult(highestGrowthTable.column("key").asList());

        System.out.println("Total time: " + watch);
    }

    private static Table filterTotalKeys(Table existing2017) {
        return existing2017.where(existing2017.stringColumn("key").endsWith("January, total"));
    }
}
