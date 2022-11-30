# Style Guide

Don’t import functions as F, instead import the needed functions directly
Since Spark 3.1, the pyspark-stubs project has been merged into the main Apache Spark repository, which means that we can reference pyspark.sql.functions without using the F. workaround. This removes unnecessary code and makes our scripts more readable. This approach has one downside though, if you use functions that are also builtins in Python like min() or max(), you have to alias those on import, so the interpreter knows which function to use. We usually prefix the function with pyspark_ like this: 

from pyspark.sql.functions import min as pyspark_min

```
# bad
from pyspark.sql import functions as F

df = df.select(F.col('colA'))

# good
from pyspark.sql.functions import col

df = df.select(col('colA'))
``` 

## Chaining of expressions
Avoid chaining of expressions into multi-line expressions with different types, particularly if they have different behaviours or contexts. For example mixing column creation or joining with selecting and filtering.
Additionally, use parenthesis instead of using line breaks.

```
# bad
df = df \
    .select('foo', 'bar', 'foobar', 'abc') \
    .filter(F.col('abc') == 123) \
    .join(another_table, 'some_field')


# good
df = df \
    .select('foo', 'bar', 'foobar', 'abc') \
    .filter(F.col('abc') == 123)

df = df.join(another_table, 'some_field', how='inner')

# better
df = (
    df
    .select('foo', 'bar', 'foobar', 'abc')
    .filter(F.col('abc') == 123)
)

df = df.join(another_table, 'some_field', how='inner')
```
## UDFs (user defined functions)
Try to avoid UDFs in all situations, as they are dramatically less performant than native PySpark. In most situations, logic that seems to necessitate a UDF can be refactored to use only native PySpark functions.

 

## Prefer implicit column selection to direct access, except for disambiguation
- If the dataframe variable name is large, expressions involving it quickly become unwieldy

- If the column name has a space or other unsupported character, the bracket operator must be used instead, which generates inconsistency

- Column expressions involving the dataframe aren't reusable and can't be used for defining abstract functions

- Renaming a dataframe variable can be error-prone, as all column references must be updated in tandem.

 
```
# bad
df = df.select(lower(df1.colA), upper(df2.colB))

# good
df = df.select(lower('colA'), upper('colB'))`
```

## Refrain from using the distinct or dropDuplicates function
**Don't** use `.dropDuplicates()` or `.distinct()` as a crutch. If unexpected duplicate rows are observed, there's almost always an underlying reason for why those duplicate rows appear. Adding `.dropDuplicates()` only masks this problem and adds overhead to the runtime.

 

## Other Considerations and Recommendations
- Be wary of functions that grow too large. As a general rule, a file should not be over 250 lines, and a function should not be over 70 lines.
- Try to keep your code in logical blocks. For example, if you have multiple lines referencing the same things, try to keep them together. Separating them reduces context and readability.
- Test your code! If you can run the local tests, do so and make sure that your new code is covered by the tests. If you can't run the local tests, build the datasets on your branch and manually verify that the data looks as expected.
- Do not keep commented out code checked in the repository. This applies to single line of codes, functions, classes or modules. Rely on git and its capabilities of branching or looking at history instead.
- When encountering a large single transformation composed of integrating multiple different source tables, split it into the natural sub-steps and extract the logic to functions. This allows for easier higher level readability and allows for code re-usability and consistency between transforms.
- Try to be as explicit and descriptive as possible when naming functions or variables. Strive to capture what the function is actually doing as opposed to naming it based the objects used inside.