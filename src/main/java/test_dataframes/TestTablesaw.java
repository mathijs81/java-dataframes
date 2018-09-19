package test_dataframes;

import static tech.tablesaw.aggregate.AggregateFunctions.mean;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

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
        Table data = Table.read().csv("urb_cpop1_1_Data.csv");
        System.out.println(data.print(5));

        Stopwatch watch = Stopwatch.createStarted();
        // Convert value column to numeric column (":" should become missing)
        Table filtered = data.emptyCopy();
        filtered.replaceColumn("Value", DoubleColumn.create("Value"));
        data.forEach(row -> {
            if (!":".equals(row.getString("Value"))) {
                for (int i = 0; i < row.columnCount(); i++) {
                    String colName = data.column(i).name();
                    Object obj = row.getObject(i);
                    if ("Value".equals(colName)) {
                        obj = Double.parseDouble((String)obj);
                    }
                    filtered.column(colName).appendObj(obj);
                }
            }
        });

        Table cities = filtered.summarize("Value", mean).by("CITIES", "INDIC_UR", "TIME");
        System.out.println(cities.print(10));

        // Need to transpose/pivot now too
        Table finalTable = Table.create("final");
        finalTable.addColumns(StringColumn.create("key"));
        cities.forEach(row -> {
            int year = (int)row.getDouble("TIME");
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
        DoubleColumn growthColumn = DoubleColumn.create("growth");
        finalTable.forEach(row -> {
            Double val2016 = row.getDouble("2016"), val2010 = row.getDouble("2010");
            if (val2010 != null && val2016 != null) {
                growthColumn.append((val2016 / val2010 - 1) * 100);
            } else {
                growthColumn.appendMissing();
            }
        });
        finalTable.addColumns(growthColumn);
        Table highestGrowthTable = filterTotalKeys(
            finalTable.dropWhere(finalTable.column("growth").isMissing())).sortDescendingOn(
                "growth");
        System.out.println(highestGrowthTable.print(20));
        CheckResult.checkResult(highestGrowthTable.column("key").asList());

        System.out.println("Total time: " + watch);
    }

    private static Table filterTotalKeys(Table existing2017) {
        // Here I'd want to do something more functional like
        // return existing2017.filter(row -> row.getString("key").endsWith(...))
        // but there doesn't seem to be an easy way to do that.
        Table onlyTotalKeys = existing2017.emptyCopy();
        existing2017.forEach(row -> {
            if (row.getString("key").endsWith("January, total")) {
                onlyTotalKeys.addRow(row);
            }
        });
        return onlyTotalKeys;
    }
}
