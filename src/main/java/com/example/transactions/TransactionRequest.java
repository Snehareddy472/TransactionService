package com.example.transactions;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;
import java.time.Instant;

public record TransactionRequest(
        @NotBlank String transactionId,
        @NotBlank String requestId,
        @NotBlank String accountId,
        @NotNull TransactionType transactionType,
        @NotNull @DecimalMin(value = "0.01") BigDecimal amount,
        @NotBlank String currency,
        @NotNull @Positive Long sequenceNumber,
        @NotNull Instant eventTime
) {}
