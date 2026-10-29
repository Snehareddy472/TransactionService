package com.example.transactions;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Service
public class TransactionProcessor {
    private static final Logger LOG = Logger.getLogger(TransactionProcessor.class.getName());
    private static final int MAX_ATTEMPTS = 3;

    // In-memory stores are intentional for this time-boxed assessment.
    private final Map<String, TransactionResult> resultsByTransactionId = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> balances = new ConcurrentHashMap<>();
    private final Map<String, Long> lastProcessedSequence = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> requestIdsByTransaction = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> simulatedFailures = new ConcurrentHashMap<>();
    private final Map<String, TransactionRequest> pendingRequests = new ConcurrentHashMap<>();

    public synchronized ProcessingReport processBatch(List<TransactionRequest> requests) {
        int received = requests == null ? 0 : requests.size();
        if (requests == null) requests = List.of();

        List<TransactionResult> batchResults = new ArrayList<>();
        int rejected = 0;

        // Validate each record independently so one malformed record cannot stop the batch.
        List<TransactionRequest> valid = new ArrayList<>();
        for (TransactionRequest request : requests) {
            String error = validateBusinessRules(request);
            if (error != null) {
                rejected++;
                String txnId = request == null ? null : request.transactionId();
                String reqId = request == null ? null : request.requestId();
                batchResults.add(new TransactionResult(txnId, reqId, TransactionStatus.FAILED, 0, error, null));
                LOG.warning("Rejected transaction requestId=" + safe(reqId) + " reason=" + error);
            } else {
                valid.add(request);
            }
        }

        // Stable ordering: account, sequence, event time. Sequence numbers are scoped per account.
        valid.sort(Comparator.comparing(TransactionRequest::accountId)
                .thenComparing(TransactionRequest::sequenceNumber)
                .thenComparing(TransactionRequest::eventTime));

        for (TransactionRequest request : valid) {
            TransactionResult prior = resultsByTransactionId.get(request.transactionId());
            if (prior != null && prior.status() == TransactionStatus.PROCESSED) {
                batchResults.add(new TransactionResult(request.transactionId(), request.requestId(),
                        TransactionStatus.DUPLICATE, prior.attempts(), "Already applied; no side effect repeated.",
                        balances.getOrDefault(request.accountId(), BigDecimal.ZERO)));
                LOG.info("Duplicate detected transactionId=" + request.transactionId());
                continue;
            }

            Set<String> knownRequestIds = requestIdsByTransaction.computeIfAbsent(
                    request.transactionId(), k -> ConcurrentHashMap.newKeySet());
            // A pending record may be replayed after its missing predecessor arrives.
            // Do not classify that recovery attempt as a duplicate solely because its requestId is known.
            knownRequestIds.add(request.requestId());

            long last = lastProcessedSequence.getOrDefault(request.accountId(), 0L);
            if (request.sequenceNumber() > last + 1) {
                TransactionResult pending = new TransactionResult(request.transactionId(), request.requestId(),
                        TransactionStatus.PENDING, 0,
                        "Waiting for sequence " + (last + 1) + " on account " + request.accountId() + ".", null);
                resultsByTransactionId.put(request.transactionId(), pending);
                pendingRequests.put(request.transactionId(), request);
                batchResults.add(pending);
                LOG.info("Transaction pending transactionId=" + request.transactionId()
                        + " expectedSequence=" + (last + 1));
                continue;
            }
            if (request.sequenceNumber() <= last) {
                TransactionResult stale = new TransactionResult(request.transactionId(), request.requestId(),
                        TransactionStatus.DUPLICATE, 0, "Sequence is already behind the account watermark; no effect applied.",
                        balances.getOrDefault(request.accountId(), BigDecimal.ZERO));
                batchResults.add(stale);
                continue;
            }

            TransactionResult result = processWithRetry(request);
            batchResults.add(result);
            if (result.status() == TransactionStatus.PROCESSED) {
                lastProcessedSequence.put(request.accountId(), request.sequenceNumber());
                drainPendingForAccount(request.accountId(), batchResults);
            }
        }

        Map<TransactionStatus, Long> counts = new EnumMap<>(TransactionStatus.class);
        for (TransactionStatus status : TransactionStatus.values()) counts.put(status, 0L);
        batchResults.forEach(r -> counts.merge(r.status(), 1L, Long::sum));
        return new ProcessingReport(received, valid.size(), rejected, counts, List.copyOf(batchResults));
    }

    private TransactionResult processWithRetry(TransactionRequest request) {
        int attempts = 0;
        while (attempts < MAX_ATTEMPTS) {
            attempts++;
            resultsByTransactionId.put(request.transactionId(), new TransactionResult(
                    request.transactionId(), request.requestId(), TransactionStatus.PROCESSING, attempts,
                    "Processing attempt " + attempts, null));
            try {
                // Deterministic test hook: REQ-5 fails once, then succeeds on retry.
                AtomicInteger remaining = simulatedFailures.computeIfAbsent(request.requestId(),
                        k -> new AtomicInteger("REQ-5".equals(k) ? 1 : 0));
                if (remaining.getAndUpdate(v -> Math.max(0, v - 1)) > 0) {
                    throw new TransientProcessingException("Simulated temporary downstream failure");
                }

                BigDecimal current = balances.getOrDefault(request.accountId(), BigDecimal.ZERO);
                BigDecimal next = apply(current, request);
                balances.put(request.accountId(), next);
                TransactionResult success = new TransactionResult(request.transactionId(), request.requestId(),
                        TransactionStatus.PROCESSED, attempts, "Applied successfully.", next);
                resultsByTransactionId.put(request.transactionId(), success);
                LOG.info("Processed transactionId=" + request.transactionId() + " attempts=" + attempts);
                return success;
            } catch (TransientProcessingException ex) {
                LOG.warning("Transient failure transactionId=" + request.transactionId() + " attempt=" + attempts);
                if (attempts == MAX_ATTEMPTS) {
                    TransactionResult failed = new TransactionResult(request.transactionId(), request.requestId(),
                            TransactionStatus.FAILED, attempts, "Retry limit exhausted.", null);
                    resultsByTransactionId.put(request.transactionId(), failed);
                    return failed;
                }
                resultsByTransactionId.put(request.transactionId(), new TransactionResult(
                        request.transactionId(), request.requestId(), TransactionStatus.RETRY_PENDING, attempts,
                        "Transient failure; retry scheduled immediately in this demo.", null));
            } catch (RuntimeException ex) {
                TransactionResult failed = new TransactionResult(request.transactionId(), request.requestId(),
                        TransactionStatus.FAILED, attempts, "Business processing failed safely.", null);
                resultsByTransactionId.put(request.transactionId(), failed);
                return failed;
            }
        }
        throw new IllegalStateException("Unreachable retry state");
    }

    private void drainPendingForAccount(String accountId, List<TransactionResult> output) {
        while (true) {
            long nextSequence = lastProcessedSequence.getOrDefault(accountId, 0L) + 1;
            TransactionRequest next = pendingRequests.values().stream()
                    .filter(r -> r.accountId().equals(accountId) && r.sequenceNumber() == nextSequence)
                    .findFirst().orElse(null);
            if (next == null) return;

            pendingRequests.remove(next.transactionId());
            TransactionResult result = processWithRetry(next);
            output.add(result);
            if (result.status() != TransactionStatus.PROCESSED) return;
            lastProcessedSequence.put(accountId, next.sequenceNumber());
        }
    }

    private BigDecimal apply(BigDecimal current, TransactionRequest request) {
        return switch (request.transactionType()) {
            case CREDIT -> current.add(request.amount());
            case DEBIT -> {
                if (current.compareTo(request.amount()) < 0) {
                    throw new BusinessRuleException("Insufficient funds");
                }
                yield current.subtract(request.amount());
            }
            case REVERSAL -> current.add(request.amount());
        };
    }

    private String validateBusinessRules(TransactionRequest r) {
        if (r == null) return "Record is null.";
        if (blank(r.transactionId()) || blank(r.requestId()) || blank(r.accountId())) return "Required identifiers are missing.";
        if (r.transactionType() == null || r.amount() == null || r.sequenceNumber() == null || r.eventTime() == null)
            return "Required transaction fields are missing.";
        if (r.amount().compareTo(BigDecimal.ZERO) <= 0) return "Amount must be greater than zero.";
        if (r.sequenceNumber() <= 0) return "Sequence number must be positive.";
        if (r.currency() == null || !r.currency().matches("[A-Z]{3}")) return "Currency must be a 3-letter uppercase code.";
        return null;
    }

    public Map<String, BigDecimal> balancesSnapshot() { return Map.copyOf(balances); }
    public List<TransactionResult> allResults() { return List.copyOf(resultsByTransactionId.values()); }

    private static boolean blank(String value) { return value == null || value.isBlank(); }
    private static String safe(String value) { return value == null ? "n/a" : value.replaceAll("[\\r\\n]", "_"); }

    static class TransientProcessingException extends RuntimeException {
        TransientProcessingException(String message) { super(message); }
    }
    static class BusinessRuleException extends RuntimeException {
        BusinessRuleException(String message) { super(message); }
    }
}
