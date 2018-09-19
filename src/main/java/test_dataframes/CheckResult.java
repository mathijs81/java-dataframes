package test_dataframes;

import java.util.List;

import com.google.common.base.Verify;

public class CheckResult {
    public static void checkResult(List<?> highestGrowth) {
        String[] expected = { "Bournemouth", "Oulu", "Derry & Strabane", "Southampton", "Blackpool",
            "Valencia", "Granada" };
        Verify.verify(highestGrowth.size() >= expected.length, "Provide at least %s items, got %s",
            expected.length, highestGrowth.size());

        for (int i = 0; i < expected.length; i++) {
            Verify.verify(((String) highestGrowth.get(i)).startsWith(expected[i]),
                "Expected item %s to start with %s, but was %s", i, expected[i],
                highestGrowth.get(i));
        }
    }
}
