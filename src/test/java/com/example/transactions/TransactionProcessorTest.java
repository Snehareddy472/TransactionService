package com.example.transactions;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class TransactionProcessorTest {
    private TransactionRequest txn(String id, String req, String account, TransactionType type,
                                   String amount, long seq) {
        return new TransactionRequest(id, req, account, type, new BigDecimal(amount), "USD",
                seq, Instant.parse("2026-01-01T00:00:00Z"));
    }

    @Test
    void duplicateDoesNotApplyBalanceTwice() {
        TransactionProcessor p = new TransactionProcessor();
        var request = txn("T1", "R1", "A1", TransactionType.CREDIT, "100", 1);
        p.processBatch(List.of(request));
        var report = p.processBatch(List.of(txn("T1", "R2", "A1", TransactionType.CREDIT, "100", 1)));
        assertEquals(new BigDecimal("100"), p.balancesSnapshot().get("A1"));
        assertEquals(TransactionStatus.DUPLICATE, report.results().get(0).status());
    }

    @Test
    void sortsOutOfOrderRequestsWithinBatch() {
        TransactionProcessor p = new TransactionProcessor();
        var report = p.processBatch(List.of(
                txn("T2", "R2", "A1", TransactionType.DEBIT, "40", 2),
                txn("T1", "R1", "A1", TransactionType.CREDIT, "100", 1)));
        assertEquals(new BigDecimal("60"), p.balancesSnapshot().get("A1"));
        assertTrue(report.results().stream().anyMatch(r -> "T2".equals(r.transactionId())
                && r.status() == TransactionStatus.PROCESSED));
    }

    @Test
    void invalidNegativeAmountIsRejectedWithoutStoppingBatch() {
        TransactionProcessor p = new TransactionProcessor();
        var report = p.processBatch(List.of(
                txn("BAD", "RB", "A1", TransactionType.DEBIT, "-10", 1),
                txn("GOOD", "RG", "A2", TransactionType.CREDIT, "25", 1)));
        assertEquals(1, report.rejected());
        assertEquals(new BigDecimal("25"), p.balancesSnapshot().get("A2"));
    }

    @Test
    void transientFailureRetriesWithoutDuplicateEffect() {
        TransactionProcessor p = new TransactionProcessor();
        var report = p.processBatch(List.of(txn("T5", "REQ-5", "A5", TransactionType.CREDIT, "500", 1)));
        assertEquals(TransactionStatus.PROCESSED, report.results().get(0).status());
        assertEquals(2, report.results().get(0).attempts());
        assertEquals(new BigDecimal("500"), p.balancesSnapshot().get("A5"));
    }

    @Test
    void sequenceGapIsPending() {
        TransactionProcessor p = new TransactionProcessor();
        var report = p.processBatch(List.of(txn("T2", "R2", "A1", TransactionType.CREDIT, "10", 2)));
        assertEquals(TransactionStatus.PENDING, report.results().get(0).status());
        assertFalse(p.balancesSnapshot().containsKey("A1"));
    }
}
