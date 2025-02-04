package com.ververica.serdes;

import com.google.gson.Gson;
import com.ververica.models.Alert;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class AlertSerializer implements KafkaRecordSerializationSchema<Alert> {
    private final String topic;
    private transient Gson gson;

    public AlertSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        this.gson = new Gson();
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Alert alert, KafkaSinkContext kafkaSinkContext, Long aLong) {
        byte[] key = alert.getCustomerId().getBytes();
        byte[] value = gson.toJson(alert).getBytes();
        return new ProducerRecord<>(topic, key, value);    }
}