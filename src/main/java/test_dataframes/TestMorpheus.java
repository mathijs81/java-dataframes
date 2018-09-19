package test_dataframes;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.util.Tuple;

/**
 * Test the API of Morpheus to do some basic dataframe manipulations.
 *
 * https://github.com/zavtech/morpheus-core
 *
 * See https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437
 * for more information.
 */
public class TestMorpheus {
    public static void main(String[] args) {
        DataFrame<Object, String> frame = DataFrame.read().csv("urb_cpop1_1_Data.csv");
        frame.out().print(5);
        System.out.println();

        Stopwatch watch = Stopwatch.createStarted();

        frame.cols().replaceKey("Value", "ValueStr");
        frame.cols().add("Value", Integer.class, value -> {
            String str = value.row().getValue("ValueStr");
            if (":".equals(str)) {
                return null;
            }
            return Integer.parseInt(str);
        });

        DataFrame<Object, String> pivoted = DataFrame.empty();
        frame.rows().select(row -> row.getInt("Value") > 0).rows().groupBy(
            row -> Tuple.of(row.getValue("CITIES") + " - " + row.getValue("INDIC_UR"),
                row.getValue("TIME"))).forEach(1, (tuple, groupedFrame) -> {
                    pivoted.rows().add(tuple.item(0));
                    DataFrameRow<Object, String> thisRow = pivoted.row(tuple.item(0));
                    int sum = 0;
                    for (int row = 0; row < groupedFrame.rowCount(); row++) {
                        sum += groupedFrame.col("Value").getInt(row);
                    }
                    int year = groupedFrame.col("TIME").getInt(0);
                    String yearStr = Integer.toString(year);
                    pivoted.cols().add(yearStr, Double.class);
                    thisRow.setDouble(yearStr, sum / (double) groupedFrame.rowCount());
                });

        // Print top 20 items in 2017
        pivoted.rows().sort(false, "2017");

        pivoted.rows().select(
            row -> ((String) row.key()).endsWith("January, total")).
            out().print(10);
        System.out.println();

        // Add growth column
        pivoted.cols().add("growth", Double.class, value -> {
            double growth = (value.row().getDouble("2016") / value.row().getDouble("2010") - 1)
                * 100.0;
            if (!Doubles.isFinite(growth)) {
                return 0.0;
            }
            return growth;
        });
        DataFrame<Object, String> output = pivoted.rows().sort(false, "growth").rows().select(
            row -> ((String) row.key()).endsWith("January, total"));
        output.out().print(10);
        System.out.println();
        CheckResult.checkResult(Lists.newArrayList(output.rows().keyArray()));

        System.out.println("Total time: " + watch);
    }
}
