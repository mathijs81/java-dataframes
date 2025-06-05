package test_dataframes;

import com.google.common.base.Stopwatch;
import org.apache.commons.csv.CSVFormat;
import org.dflib.DataFrame;
import org.dflib.Printers;
import org.dflib.ValueMapper;
import org.dflib.csv.Csv;
import org.dflib.print.Printer;

import static org.dflib.Exp.*;

/**
 * Test the API of tablesaw to do some basic dataframe manipulations.
 * <p>
 * https://github.com/dflib/dflib
 * <p>
 * See https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437
 * for more information.
 */
public class TestDFLib {
    public static void main(String[] args) {

        Printer printer = Printers.tabular(10, 100);

        DataFrame data = Csv.loader()
                .format(CSVFormat.DEFAULT.builder().setNullString(":").build())
                .col("Value", ValueMapper.stringToInt())
                .load("urb_cpop1_1_Data.csv");
        System.out.println(printer.toString(data));

        Stopwatch watch = Stopwatch.createStarted();
        DataFrame filtered = data.rows($col("Value").isNotNull()).select();

        DataFrame cities = filtered.group("CITIES", "INDIC_UR", "TIME")
                .cols("CITIES", "INDIC_UR", "TIME", "Mean [Value]")
                .agg($col("CITIES"), $col("INDIC_UR"), $col("TIME"), $int("Value").avg().castAsInt());

        System.out.println(printer.toString(cities));

        // Need to transpose/pivot now too
        DataFrame finalTable = cities
                .cols("key").merge(concat($str("CITIES"), ":", $str("INDIC_UR")))
                .pivot().rows("key").cols("TIME").vals("Mean [Value]");

        // sortDescendingOn puts N/A values first unfortunately, so let's remove them
        // before determining and printing.
        DataFrame existing2017 = finalTable
                .rowsExcept($int("2017").isNull()).select()
                .rows($str("key").endsWith("January, total")).select()
                .sort($int("2017").desc());
        System.out.println(printer.toString(existing2017));

        // Add growth column

        DataFrame finalTable1 = finalTable
                .cols("growth").merge($int("2016").castAsDouble().div($int("2010")).sub(1).mul(100));

        DataFrame highestGrowthTable = finalTable1
                .rows($str("key").endsWith("January, total")).select()
                .rowsExcept($col("growth").isNull()).select()
                .sort($double("growth").desc());

        System.out.println(printer.toString(highestGrowthTable));
        CheckResult.checkResult(highestGrowthTable.getColumn("key").toList());

        System.out.println("Total time: " + watch);
    }
}
