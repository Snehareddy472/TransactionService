package com.example.transactions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class TransactionController {
    private final TransactionProcessor processor;
    public TransactionController(TransactionProcessor processor) { this.processor = processor; }

    @PostMapping("/transactions/batch")
    public ProcessingReport processBatch(@RequestBody List<TransactionRequest> requests) {
        if (requests == null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Request body is required");
        return processor.processBatch(requests);
    }

    @GetMapping("/transactions")
    public List<TransactionResult> transactions() { return processor.allResults(); }

    @GetMapping("/balances")
    public Map<String, BigDecimal> balances() { return processor.balancesSnapshot(); }
}
