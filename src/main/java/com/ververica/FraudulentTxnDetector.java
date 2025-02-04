package com.ververica;

import com.ververica.models.Alert;
import com.ververica.models.AlertType;
import com.ververica.models.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.ververica.config.AppConfig.buildSecurityProps;
import static com.ververica.utils.StreamingUtils.createKafkaAlertSink;
import static com.ververica.utils.StreamingUtils.createTxnConsumer;

public class FraudulentTxnDetector {
    public static void main(String[] args) throws Exception {
        // 1. Set up the execution environment
        final StreamExecutionEnvironment environment
                = StreamExecutionEnvironment.getExecutionEnvironment();

        var properties = buildSecurityProps(new Properties());

        KafkaSource<Transaction> txnSource =  createTxnConsumer(properties);

        var watermarkStrategy =
                WatermarkStrategy
                        .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> Timestamp.valueOf(event.getTimestamp()).getTime());

        // 2. Create a datastream from the Kafka source
        DataStream<Transaction> txnStream = environment
                .fromSource(txnSource, watermarkStrategy, "Transactions Source")
                .name("TransactionSource")
                .uid("TransactionSource")
                .keyBy(Transaction::getUserId);

        // 3. Define the patterns to detect fraudulent transactions
        Pattern<Transaction, ?> highValuePattern = Pattern.<Transaction>begin("first")
                .where(new IterativeCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction, Context<Transaction> context) throws Exception {
                        return transaction.getAmount() > 10;
                    }
                })
                .next("second")
                .where(new IterativeCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction, Context<Transaction> context) throws Exception {
                        return transaction.getAmount() > 10;
                    }
                }).within(Time.seconds(30));

        PatternStream<Transaction> highValuePatternStream = CEP.pattern(txnStream, highValuePattern);

        SingleOutputStreamOperator<Alert> highValueAlerts = highValuePatternStream.select(
                new PatternSelectFunction<Transaction, Alert>() {
                    @Override
                    public Alert select(Map<String, List<Transaction>> pattern) {
                        Transaction first = pattern.get("first").get(0);
                                Transaction second = pattern.get("second").get(0);

                        var message = String.format(
                               "User: %s, Tx1=%.2f, Tx2=%.2f, TxIDs=%s,%s",
                                first.getUserId(), first.getAmount(), second.getAmount(),
                                first.getTransactionId(), second.getTransactionId()
                        );
                        return new Alert(first.getUserId(), AlertType.FREQUENT_HIGH_VALUE_TRANSACTIONS, message);
                    }
                }
        ).name("HighValueAlertsPattern").uid("HighValueAlertsPattern");

        // 4. Multiple Rapid Transactions
        //    Pattern: A user performs 3 transactions in under 10s
        Pattern<Transaction, ?> rapidTxnPattern = Pattern.<Transaction>begin("t1")
                .next("t2")
                .next("t3")
                .within(Time.seconds(10));

        PatternStream<Transaction> rapidTxnPatternStream = CEP.pattern(txnStream, rapidTxnPattern);

        SingleOutputStreamOperator<Alert> rapidTxnAlerts = rapidTxnPatternStream.select(
                new PatternSelectFunction<Transaction, Alert>() {
                    @Override
                    public Alert select(Map<String, List<Transaction>> pattern) {
                        Transaction t1 = pattern.get("t1").get(0);
                        Transaction t2 = pattern.get("t2").get(0);
                        Transaction t3 = pattern.get("t3").get(0);

                        var message = String.format(
                                "User: %s triggered 3 txns within 10s (TxIDs: %s, %s, %s)",
                                t1.getUserId(), t1.getTransactionId(), t2.getTransactionId(), t3.getTransactionId()
                        );
                        return new Alert(t1.getUserId(), AlertType.MULTIPLE_RAPID_TRANSACTIONS, message);
                    }
                }
        ).name("RapidTxnAlertsPattern").uid("RapidTxnAlertsPattern");

        // 5. Location-Based Suspicious Activity
        //    Pattern: A user performs 2 transactions in widely different locations within 1 minute
        Pattern<Transaction, ?> locationPattern = Pattern.<Transaction>begin("loc1")
                .next("loc2")
                .within(Time.minutes(1));

        PatternStream<Transaction> locationPatternStream = CEP.pattern(txnStream, locationPattern);

        SingleOutputStreamOperator<Alert> locationAlerts = locationPatternStream.select(
                new PatternSelectFunction<Transaction, Alert>() {
                    @Override
                    public Alert select(Map<String, List<Transaction>> pattern) {
                        Transaction loc1 = pattern.get("loc1").get(0);
                        Transaction loc2 = pattern.get("loc2").get(0);

                        double distance = haversineDistance(
                                loc1.getLatitude(), loc1.getLongitude(),
                                loc2.getLatitude(), loc2.getLongitude()
                        );
                        // 500 km in <1 min => suspicious
                        if (distance > 500) {
                            String message = String.format(
                                    "User: %s had 2 txns %s -> %s (%.1f km apart) within 1 min. Tx1: %s, Tx2: %s",
                                    loc1.getUserId(),
                                    loc1.getLocation(), loc2.getLocation(),
                                    distance,
                                    loc1.getTransactionId(), loc2.getTransactionId()
                            );
                            return new Alert(loc1.getUserId(), AlertType.LOCATION_BASED_SUSPICIOUS_ACTIVITY, message);
                        }
                        return new Alert();
                    }
                }
        ).filter(alert -> alert.getCustomerId() != null).name("LocationAlertsPattern").uid("LocationAlertsPattern");

        // 6. Union the alert streams
        DataStream<Alert> anomaliesAlertStream
                = highValueAlerts.union(rapidTxnAlerts, locationAlerts);
//        anomaliesAlertStream.print();

        // 7. Sink the alerts to Kafka
        KafkaSink<Alert> kafkaAlertSink = createKafkaAlertSink(properties);
        anomaliesAlertStream.sinkTo(kafkaAlertSink);

        // 8. Execute the job
        environment.execute("Fraudulent Transaction Detector");
    }

    // Helper method to compute great-circle distance in kilometers
    private static double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371; // Earth radius in km
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
