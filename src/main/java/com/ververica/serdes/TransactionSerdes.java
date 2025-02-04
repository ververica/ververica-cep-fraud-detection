package com.ververica.serdes;

import com.google.gson.Gson;
import com.ververica.models.Transaction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class TransactionSerdes extends AbstractDeserializationSchema<Transaction> {
    private Gson gson;

    @Override
    public void open(InitializationContext context) throws Exception {
        gson = new Gson();
        super.open(context);
    }

    @Override
    public Transaction deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), Transaction.class);
    }
}
