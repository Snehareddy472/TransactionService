package com.example.transactions;

import java.util.List;
import java.util.Map;

public record ProcessingReport(
        int received,
        int accepted,
        int rejected,
        Map<TransactionStatus, Long> countsByStatus,
        List<TransactionResult> results
) {}
