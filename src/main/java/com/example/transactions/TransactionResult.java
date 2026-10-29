package com.example.transactions;

import java.math.BigDecimal;

public record TransactionResult(
        String transactionId,
        String requestId,
        TransactionStatus status,
        int attempts,
        String message,
        BigDecimal resultingBalance
) {}
