package test_dataframes;

import static org.datavec.api.transform.condition.ConditionOp.Equal;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.datavec.api.transform.ReduceOp;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.condition.column.StringColumnCondition;
import org.datavec.api.transform.reduce.Reducer;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.writable.IntWritable;
import org.datavec.api.writable.Writable;
import org.datavec.local.transforms.LocalTransformExecutor;

/**
 * Test the API of DataVec to do some basic dataframe manipulations (unfinished).
 *
 * https://deeplearning4j.org/docs/latest/datavec-overview
 */
public class TestDatavec {
    public static void main(String[] args) throws Exception {
        int numLinesToSkip = 1;
        char delimiter = ',';
        RecordReader recordReader = new CSVRecordReader(numLinesToSkip,delimiter);
        recordReader.initialize(new FileSplit(new File("urb_cpop1_1_Data.csv")));

        // It seems we need to know in advance what the fields and their order
        // are here...
        Schema csvSchema = new Schema.Builder()
            .addColumnInteger("TIME")
            .addColumnsString("CITIES", "INDIC_UR","Value","Flag and Footnotes")
            .build();

        TransformProcess tp = new TransformProcess.Builder(csvSchema)
            .conditionalReplaceValueTransform("Value", new IntWritable(0), new StringColumnCondition("Value", Equal, ":"))
            .convertToInteger("Value")
            .reduce(new Reducer.Builder(ReduceOp.TakeLast)
                .keyColumns("CITIES", "INDIC_UR", "TIME")
                .meanColumns("Value")
                .build())
            // Here we also need to know in advance the range of items
            .integerToOneHot("TIME", 2008, 2017)

            // Now we have one-hot encoded countries, with the Value column separately.
            // We would have to either do a conditionalCopyValueTransform Value -> year column
            // for every year separately, or we probably have to modify integerToOneHot
            // to copy our Value column instead of 1-hot to make the proper pivot.
            // IntegerToOneHotTransform is > 200 lines, so it's not trivial to create
            // such a custom transform.

            .build();


        List<List<Writable>> csvData = new ArrayList<>();
        while(recordReader.hasNext()) {
            csvData.add(recordReader.next());
        }
        printHead(csvData, csvSchema);

        List<List<Writable>> transformedData = LocalTransformExecutor.execute(csvData, tp);

        printHead(transformedData, tp.getFinalSchema());
    }

    private static void printHead(List<List<Writable>> data, Schema schema) {
        for (int j = 0; j < schema.getColumnNames().size(); j++) {
            System.out.printf("%20s", schema.getColumnNames().get(j));
        }
        System.out.println();
        for (int i = 0; i < Math.min(10,  data.size()); i++) {
            List<Writable> row = data.get(i);
            for (int j = 0; j < row.size(); j++) {
                System.out.printf("%20s", row.get(j).toString());
            }
            System.out.println();
        }
    }
}
