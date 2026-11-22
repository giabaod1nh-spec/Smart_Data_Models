# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

- Primary: the project author operating and demonstrating a live SUMO traffic-control experiment.
- Review audience: graduation-defense reviewers who need to understand network conditions, adaptive-control behavior, pipeline health, and experiment evidence quickly.

## Product Purpose

Provide one operational interface for observing a SUMO road network, inspecting intersections, controlling supported simulation behavior, monitoring the adaptive controller and data pipeline, and evaluating Fixed-Time versus Adaptive control experiments.

Success means a reviewer can trace the closed loop from SUMO observations through the context pipeline to traffic monitoring, control decisions, applied signal behavior, and defensible experiment results without confusing simulated traffic performance with system health.

## Positioning

This is an engineering operations and experiment-evaluation surface over a real closed-loop traffic simulation stack: SUMO and TraCI produce observations, FIWARE Orion-LD provides current context, the controller applies signal decisions back to SUMO, and the analytics layer evaluates outcomes.

## Operating Context

- Used on desktop during development, live simulation operation, debugging, and graduation-defense demonstrations.
- The application uses authenticated Spring endpoints for realtime context, analytics, system health, and reverse control.
- The SUMO network geometry and current-run identifiers are authoritative; dashboard diagrams must not invent road geography or individual vehicle truth.

## Capabilities and Constraints

- Preserve existing React, TypeScript, Vite, React Query, Recharts, React Konva, and Spring API architecture.
- Preserve existing backend contracts and route control commands through Spring rather than calling the Python control API directly.
- Reuse actual SUMO network geometry and current-run-safe realtime entities.
- Keep traffic performance, simulation runtime health, data-pipeline health, projection health, and controller health visually distinct.
- Label unavailable, stale, inconsistent, demo, and live data explicitly. Never silently replace failed live data with mock values.
- Do not infer movement-level metrics, controller explanations, experiment equivalence, or performance improvements unless the backend provides enough evidence.
- Existing supported operations include scenario, signal phase, green duration, demand profile, and control-mode changes. Controls may be unavailable when their runtime dependency is offline.
- Experiment comparison requires equivalent network, demand, scenario, duration, and seed conditions where those fields are available. Missing comparison datasets remain an explicit empty state.

## Brand Commitments

- Product name: Smart Traffic Operations Center.
- The interface must feel like a professional intelligent-transportation operations console and scientific experiment platform, not a generic SaaS admin template.
- Visual direction is a restrained dark operational environment using neutral slate surfaces, cyan/blue for neutral live data, amber for warnings and congestion, red for critical conditions, and green only for healthy or positive states.

## Evidence on Hand

- Actual SUMO network geometry: `frontend/public/live-network.json` and `frontend/src/data/liveNetwork.json`.
- Current-run realtime intersection, traffic-light, vehicle-sensor, and metadata contracts in `frontend/src/types/realtime.ts`.
- Analytics window, congestion, priority, comparison, trend, and signal-operation contracts in `frontend/src/types/analytics.ts`.
- Reverse-control contracts in `frontend/src/api/controlApi.ts` and `frontend/src/types/control.ts`.
- Live vehicle and cooperative DQN status contracts in `frontend/src/types/liveTraffic.ts`.
- No confirmed endpoint currently provides a complete catalog of comparable Fixed-Time and Adaptive experiment runs; the experiments surface must not fabricate conclusions.

## Product Principles

1. Operational truth before decoration.
2. Separate network performance from platform health.
3. Make every control action and freshness state legible.
4. Prefer actual topology and measured metrics over illustrative substitutes.
5. Present scientific comparisons only when conditions are valid and explicit.

## Accessibility & Inclusion

- Support keyboard navigation, visible focus, semantic labels, and non-color status cues.
- Maintain readable contrast and tabular numerals for rapidly changing operational values.
- Adapt the primary workflows to smaller desktop and mobile web widths without hiding critical status.
