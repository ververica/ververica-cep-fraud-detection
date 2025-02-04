package com.ververica.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    private String transactionId;
    private String userId;
    private String timestamp;
    private double amount;
    private String currency;
    private String transactionType;
    private String merchant;
    private String location;
    private double latitude;
    private double longitude;
}
