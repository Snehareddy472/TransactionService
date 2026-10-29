package com.example.transactions;

public enum TransactionStatus {
    RECEIVED, PROCESSING, PROCESSED, DUPLICATE, FAILED, RETRY_PENDING, PENDING
}
