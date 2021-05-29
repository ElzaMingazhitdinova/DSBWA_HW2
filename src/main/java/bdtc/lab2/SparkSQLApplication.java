package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Запуск приложения в зависимости от типа вычислений: computeIntensive или dataIntensive
 */
@Slf4j
public class SparkSQLApplication {

    /**
     * @param args - args[0]: входной файл, args[1] - выходная папка
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar input.file outputDirectory calculationType");
        }

        log.info("Appliction started");
        log.debug("Application started");
        SparkSession sc = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQLApplication")
                .getOrCreate();

        if (args[2].equals("computeIntensive")) {
            Dataset<String> df = sc.read().text(args[0]).as(Encoders.STRING());
            log.info("===============COUNTING...================");
            JavaRDD<Row> result = ArraysLevelCounter.calcFactorial(df);
            log.info("============SAVING FILE TO " + args[1] + " directory============");
            result.saveAsTextFile(args[1]);
        } else if (args[2].equals("dataIntensive")) {
            Dataset<String> df = sc.read().text(args[0]).as(Encoders.STRING());
            log.info("===============COUNTING...================");
            JavaRDD<Row> result = ArraysLevelCounter.calcAggregate(df);
            log.info("============SAVING FILE TO " + args[1] + " directory============");
            result.saveAsTextFile(args[1]);
        } else {
            throw new RuntimeException("Usage: calculationType should be computeIntensive or dataIntensive");
        }
    }
}
