package bdtc.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.ArraysLevelCounter.calcAggregate;
import static bdtc.lab2.ArraysLevelCounter.calcFactorial;

/**
 * Тесты для двух видов вычислений
 * для первого типа вычислений ( расчета факториала) - мы проверяем что ключ строки 1 и что факториал 14 = 87178291200
 * для второго типа вычислений (группировки массивов) - мы проверяем индекс строки 1 и сумма (складывая значения массивов по 1000 каждый) получается 2000
 */
public class SparkTest {

    final String testStringC1 = "1,1,14,test";
    final String testStringC2 = "2,2,20,test";
    final String testStringD1 = "1,1,1,1000,test";
    final String testStringD2 = "2,2,1,1000,test";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    @Test
    public void testArrayFactorial() {
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testStringC1, testStringC2));
        JavaRDD<Row> result = calcFactorial(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        Row item = rowList.iterator().next();
        assert (item.get(0)).toString().equals("1");
        assert (item.get(2)).toString().equals("87178291200");
    }

    @Test
    public void testArrayAggregate() {
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testStringD1, testStringD2));
        JavaRDD<Row> result = calcAggregate(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        Row item = rowList.iterator().next();
        assert (item.get(0)).toString().equals("1");
        assert (item.get(1)).toString().equals("2000");
    }

}
