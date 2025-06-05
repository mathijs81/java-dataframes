# Java dataframes test
This is the companion repository to the following medium post: [Doing cool data science in Java: how 3 DataFrame libraries stack up](https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437)

## Data
The data was extracted from [Eurostat](http://appsso.eurostat.ec.europa.eu/nui/show.do?dataset=urb_cpop1&lang=en) in the beginning of September 2018. I opened the extracted CSV in LibreOffice and saved it again because there were some illegal UTF-8 characters in the Eurostat output that some csv importers couldn't handle directly.

# Results [June 2025]

| Library                                                 | Maintained | Version   | Time (ms) |
|---------------------------------------------------------|------------|-----------|-----------|
| [DuckDb](https://github.com/duckdb/duckdb-java)         | Y          | 1.3.0     | 93        |
| [DFLib](https://github.com/dflib/dflib)                 | Y          | 1.3.0     | 226       |
| [Kotlin DataFrame](https://github.com/Kotlin/dataframe) | Y          | 1.0-beta2 | 816       |
| [Tablesaw](https://github.com/jtablesaw/tablesaw)       | Y          | 0.44.1    | 820       |
| Joinery                                                 | n          | 1.9       | 1,478     |
| Krangl                                                  | n          | 0.18.4    | 1,796     |
| Morpheus                                                | n          | 0.9.23    | *         |

* Morpheus is no longer maintained and doesn't seem to work on later java versions (error related to accessing `sun.util.calendar.ZoneInfo`)

## Code
The code for the three libraries is present in the `Test{libraryname}.java` files. They all use `CheckResult.java` to do a basic correctness check for the top-growing cities.

As described in the [medium post](https://medium.com/@thijser/doing-cool-data-science-in-java-how-3-dataframe-libraries-stack-up-5e6ccb7b437), I couldn't find a good way to do the pivot step in [datavec](https://deeplearning4j.org/docs/latest/datavec-overview), but I included the code I wrote up until that point.
