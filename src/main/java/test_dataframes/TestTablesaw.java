package test_dataframes;

import static tech.tablesaw.aggregate.AggregateFunctions.mean;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
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
    public static void main(String[] args) throws Exception {
        // This automatically makes the ":" values missing
        Table data = Table.read().csv(
            CsvReadOptions.builder("urb_cpop1_1_Data.csv").missingValueIndicator(":").build());
        System.out.println(data.print(5));

        Stopwatch watch = Stopwatch.createStarted();
        Table filtered = data.where(data.column("Value").isNotMissing());

        Table cities = filtered.summarize("Value", mean).by("CITIES", "INDIC_UR", "TIME");
        System.out.println(cities.print(10));

        // Need to transpose/pivot now too
        // Next version of tablesaw will make this a one-liner!
        Table finalTable = Table.create("final");
        finalTable.addColumns(StringColumn.create("key"));
        cities.forEach(row -> {
            int year = row.getShort("TIME");
            String yearStr = Integer.toString(year);
            String key = row.getString("CITIES") + ":" + row.getString("INDIC_UR");
            double value = row.getDouble("Mean [Value]");
            if (!finalTable.columnNames().contains(yearStr)) {
                DoubleColumn col = DoubleColumn.create(yearStr);
                // Need to prefill null for rows that exist already.
                for (int i = 0; i < finalTable.rowCount(); i++) {
                    col.appendMissing();
                }
                finalTable.addColumns(col);
            }
            int firstIndex = ((StringColumn)finalTable.column("key")).firstIndexOf(key);
            if (firstIndex == -1) {
                firstIndex = finalTable.rowCount();
                for (Column<?> col : finalTable.columns()) {
                    col.appendMissing();
                }
                ((Column<String>)finalTable.column("key")).set(firstIndex, key);
            }
            ((Column<Double>)finalTable.column(yearStr)).set(firstIndex, value);
        });
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
