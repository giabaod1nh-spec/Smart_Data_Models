package com.traffic.server.control.command;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Separate TX boundary for concurrent acceptance tests and lock claim isolation. */
@Service
public class ControlCommandTransactionHelper {

    private final ControlCommandService commandService;

    public ControlCommandTransactionHelper(ControlCommandService commandService) {
        this.commandService = commandService;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ControlCommandService.AcceptResult acceptInNewTransaction(
            CreateControlCommandRequest req, String operatorId) {
        return commandService.accept(req, operatorId);
    }
}
