package test_dataframes;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import joinery.DataFrame;
import joinery.DataFrame.KeyFunction;
import joinery.DataFrame.RowFunction;
import joinery.impl.Aggregation.Mean;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Test the API of joinery to do some basic dataframe manipulations.
 *
 * https://github.com/cardillo/joinery
 *
 * See https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437
 * for more information.
 */
public class TestJoinery {
    public static void main(String[] args) throws Exception {
        DataFrame<Object> frame = DataFrame.readCsv("urb_cpop1_1_Data.csv");
        System.out.println(frame.head(5));

        Stopwatch watch = Stopwatch.createStarted();

        // Remove ":" values from the value column
        int valueColIndex = Iterables.indexOf(frame.columns(), name -> name.equals("Value"));
        frame = frame.<Object>transform(new RowFunction<Object, Object>() {
            @Override
            public List<List<Object>> apply(List<Object> values) {
                if (":".equals(values.get(valueColIndex))) {
                    values = Lists.newArrayList(values);
                    values.set(valueColIndex, null);
                }
                return Collections.singletonList(values);
            }
        });
        // This will make the Value column type "Long" now:
        frame.convert();

        // Pivot the table to get year into columns. Would be nice if joinery provides
        // a better method to get the index of a column name without having to resort to a
        // guava function, e.g. frame.columnIndex("CITIES")
        int citiesIndex = Iterables.indexOf(frame.columns(), name -> name.equals("CITIES"));
        int typeIndex = Iterables.indexOf(frame.columns(), name -> name.equals("INDIC_UR"));
        int timeIndex = Iterables.indexOf(frame.columns(), name -> name.equals("TIME"));

        DataFrame<Number> numberFrame = frame.pivot(
            (KeyFunction<Object>)(values -> values.get(citiesIndex) + " - " + values.get(typeIndex)),
            values -> values.get(timeIndex),
            Collections.singletonMap(valueColIndex,
                new Mean<>())
            );

        // Print top 20 items in 2017
        // sortBy supports something like "-2017" but this doesn't work because the
        // column names are of type Long and not String
        // Can use the int based indexing though.
        int _2017index = Iterables.indexOf(numberFrame.columns(), colName -> ((Number)colName).intValue() == 2017);
        // Remove NaN values created by mean() function
        // The key names are now part of the index and not cells anymore, that makes it a bit more tricky to filter,
        // as the Predicate of the select() function doesn't get the row name.
        System.out.println(filterTotalKeys(numberFrame).select(
            row -> !Double.isNaN((Double) row.get(_2017index))).sortBy(-_2017index).head(10));

        // Add growth column
        int _2010index = Iterables.indexOf(numberFrame.columns(), colName -> ((Number)colName).intValue() == 2010);
        int _2016index = Iterables.indexOf(numberFrame.columns(), colName -> ((Number)colName).intValue() == 2016);

        // Great API to add a new calculated column:
        numberFrame = numberFrame.add("growth",
            row -> (row.get(_2016index).doubleValue() / row.get(_2010index).doubleValue() - 1)
                * 100);

        int growthIndex = Iterables.indexOf(numberFrame.columns(), colName -> "growth".equals(colName));
        DataFrame<Number> highestGrowthFrame = filterTotalKeys(numberFrame).
            select(row -> !Double.isNaN((Double) row.get(growthIndex))).
            sortBy("-growth");
        System.out.println(highestGrowthFrame.head(10));
        CheckResult.checkResult(Lists.newArrayList(highestGrowthFrame.index()));

        System.out.println("Total time: " + watch);
    }

    private static DataFrame<Number> filterTotalKeys(DataFrame<Number> df) {
        DataFrame<Number> result = new DataFrame<>(df.columns());
        // I can't find a function like "getRowName(int rowNumber)".
        // Iterating over df.index() is also not possible because there's also
        // no function like List<Number> getRow(int rowNumber), we need to use
        // the forEach function.
        List<Object> rowNames = Lists.newArrayList(df.index());

        df.forEach(new Consumer<List<Number>>() {
            int index = 0;

            @Override
            public void accept(List<Number> row) {
                String rowName = (String) rowNames.get(index);
                if (rowName.endsWith("January, total")) {
                    result.append(rowName, row);
                }
                index++;
            }
        });
        return result;
    }
}
