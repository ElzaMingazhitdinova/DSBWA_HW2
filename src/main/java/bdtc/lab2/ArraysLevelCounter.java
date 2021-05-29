package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

@AllArgsConstructor
@Slf4j
public class ArraysLevelCounter {

    /**
     * Расчет больших значений факториала для элементов массива BigInteger
     *
     * @param inputDataset - входной DataSet для анализа
     * @return результат расчета в формате JavaRDD
     */
    public static JavaRDD<Row> calcFactorial(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());
        words.show();

        Dataset<ArrayLevelObj> ArrayLevelObjDataset = words.map(s -> {
                    String[] arrayFields = s.split(",");
                    int key = Integer.parseInt(arrayFields[1]);
                    int value = Integer.parseInt(arrayFields[2]);
                    int index = 0;
                    return new ArrayLevelObj(index, key, value);
                }, Encoders.bean(ArrayLevelObj.class)
        ).coalesce(1);

        // Считаем факториалы
        Dataset<Row> t = ArrayLevelObjDataset
                .select("key", "value")
                .withColumn("factorial", functions.factorial(functions.col("value")))
                .toDF("key", "value", "factorial")
                .sort(functions.asc("key"));
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

    /**
     * Функция группировки массивов
     *
     * @param inputDataset входной DataSet для анализа
     * @return результат расчета в формате JavaRDD
     */

    public static JavaRDD<Row> calcAggregate(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());
        words.show();

        Dataset<ArrayLevelObj> ArrayLevelObjDataset = words.map(s -> {
                    String[] arrayFields = s.split(",");
                    int index = Integer.parseInt(arrayFields[1]);
                    int key = Integer.parseInt(arrayFields[2]);
                    int value = Integer.parseInt(arrayFields[3]);
                    return new ArrayLevelObj(index, key, value);
                }, Encoders.bean(ArrayLevelObj.class)
        ).coalesce(1);

        // группирум массивы
        Dataset<Row> t = ArrayLevelObjDataset
                .select("index", "key", "value")
                .groupBy("key")
                .sum("value")
                .toDF("key", "sum")
                .sort(functions.asc("key"));
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

}
