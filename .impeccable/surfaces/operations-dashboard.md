# Operations Dashboard Surface

## Purpose

Let an operator or reviewer understand the active simulation, network pressure, controller behavior, and data freshness within the first viewport, then move into intersection, experiment, or system evidence without losing the active run context.

## First Viewport

1. Persistent compact simulation context bar.
2. Compact horizontal performance instrument strip below the header.
3. A wide network-performance time-series plot as the primary focal region, pairing speed, waiting, queue, and traffic volume only where the backend exposes them.
4. Congestion ranking and controller-state panels beside the primary plot.
5. Experiment evidence and decision-impact/data-quality regions in the lower row. Pipeline health belongs only on System Monitoring.

## Interaction Contract

- Selecting an intersection from a chart, ranking, or table opens its operational detail.
- Navigation never fabricates data for a disabled backend capability.
- Freshness, consistency, and source state remain visible during live updates.
- Wide tables scroll rather than compressing values into unreadable cards.

## Responsive Contract

- At 1180px and below, secondary rails move beneath the primary chart.
- At 760px and below, the navigation becomes a compact overlay and the context bar wraps into two rows.
- No critical simulation status disappears at smaller widths.

## Data Integrity

- Overview charts use actual realtime and Gold analytics data; no illustrative series are rendered as live.
- Topology and vehicles remain available on intersection detail using actual SUMO geometry/stream data.
- Current-run Orion data and analytics data are labeled separately.
- Experiment pages show an evidence-ready empty state until comparable run catalogs exist.
- Operations may show freshness and consistency because they qualify the displayed traffic data, but it must not duplicate infrastructure health.

## Rejected Exploration

- `.impeccable/mocks/operations-overview-approved.png` is retained only as rejected exploration evidence.
- Rejection reason: the map consumed the primary viewport without adding enough analytical value.

## Approved Composition

- North-star comp: `.impeccable/mocks/operations-data-dashboard-approved.png`
- Approval basis: the user explicitly replaced the map-first direction with a data-visualization-led dashboard.
- Translate the hierarchy, chart emphasis, and density. Replace the comp's pipeline strip with decision-impact and data-quality evidence. Replace every illustrative number with live API data, user-selected run data, or an explicit unavailable state.
