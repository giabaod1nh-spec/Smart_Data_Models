package com.traffic.server.control.command;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class ControlResourceLockService {

    private final ControlResourceLockClaimRepository claimRepository;

    public ControlResourceLockService(ControlResourceLockClaimRepository claimRepository) {
        this.claimRepository = claimRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryClaim(String resourceKey, UUID commandId, Instant expiresAt) {
        return claimRepository.tryClaim(resourceKey, commandId, expiresAt);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void release(String resourceKey) {
        claimRepository.release(resourceKey);
    }
}
