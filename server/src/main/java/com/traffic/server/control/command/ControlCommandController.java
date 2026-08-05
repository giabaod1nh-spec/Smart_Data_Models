package com.traffic.server.control.command;



import com.traffic.server.payload.ApiResponse;

import org.springframework.http.HttpStatus;

import org.springframework.http.ResponseEntity;

import org.springframework.security.core.Authentication;

import org.springframework.web.bind.annotation.*;



import java.net.URI;

import java.util.UUID;



@RestController

@RequestMapping("/api/control/commands")

public class ControlCommandController {



    private final ControlCommandService commandService;

    private final ControlCommandDispatchService dispatchService;



    public ControlCommandController(

            ControlCommandService commandService,

            ControlCommandDispatchService dispatchService) {

        this.commandService = commandService;

        this.dispatchService = dispatchService;

    }



    @PostMapping

    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> create(

            @RequestBody CreateControlCommandRequest request,

            @RequestHeader(value = "X-Request-Id", required = false) String requestId,

            Authentication authentication) {

        if (!commandService.isEnabled()) {

            return ResponseEntity.status(HttpStatus.NOT_FOUND)

                    .body(ApiResponse.error(404, "command domain disabled"));

        }

        String operator = authentication != null ? authentication.getName() : "anonymous";

        String corrId = requestId != null ? requestId : UUID.randomUUID().toString();

        try {

            ControlCommandService.AcceptResult accepted = commandService.accept(request, operator);

            ControlCommandStatusResponse body = accepted.status();

            if (accepted.newlyCreated()) {

                body = dispatchService.dispatchIfNeeded(body.commandId(), corrId);

            }

            HttpStatus status = accepted.newlyCreated() ? HttpStatus.ACCEPTED : HttpStatus.OK;

            return ResponseEntity.status(status)

                    .location(URI.create("/api/control/commands/" + body.commandId()))

                    .body(ApiResponse.success(body));

        } catch (IdempotencyConflictException e) {

            return ResponseEntity.status(HttpStatus.CONFLICT)

                    .body(ApiResponse.error(409, e.getMessage()));

        } catch (ResourceBusyException e) {

            return ResponseEntity.status(HttpStatus.CONFLICT)

                    .body(ApiResponse.error(409, e.getMessage()));

        }

    }



    @GetMapping("/{commandId}")

    public ResponseEntity<ApiResponse<ControlCommandStatusResponse>> get(@PathVariable UUID commandId) {

        if (!commandService.isEnabled()) {

            return ResponseEntity.status(HttpStatus.NOT_FOUND)

                    .body(ApiResponse.error(404, "command domain disabled"));

        }

        return commandService.get(commandId)

                .map(r -> ResponseEntity.ok(ApiResponse.success(r)))

                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)

                        .body(ApiResponse.error(404, "COMMAND_NOT_FOUND")));

    }

}


