# Smart Traffic Operations Center — Design System

<!-- impeccable:design-schema 1 -->

## Visual World

A dense but calm traffic-operations console inspired by transport control rooms and scientific instrumentation. The interface behaves like an operational surface first: measured traffic trends, comparisons, rankings, and controller evidence dominate. Network geometry is a secondary drill-down tool rather than the homepage centerpiece.

## Mode

Operate. Users scan the network, identify exceptions, inspect evidence, issue supported controls, and compare experiments.

## Palette

- Canvas: `#060A0F`
- Sidebar: `#090F16`
- Primary surface: `#0D151E`
- Raised surface: `#111C27`
- Hairline: `#223140`
- Primary text: `#F1F5F9`
- Secondary text: `#A8B5C3`
- Muted text: `#718294`
- Neutral live data: `#35BCE8`
- Secondary data: `#6E8BFF`
- Healthy / positive: `#35C98A`
- Warning / congestion: `#F3B84B`
- Critical: `#F06464`

Color always carries a written label, icon, shape, or position cue. Green is reserved for healthy states and positive experimental outcomes.

## Typography

- UI and prose: Inter-compatible system sans stack for compact, readable operations use.
- Measurements, timestamps, identifiers, and run IDs: JetBrains Mono-compatible monospace stack.
- Page titles are restrained; hierarchy comes from weight, spacing, and information grouping rather than oversized display type.

## Geometry and Materials

- Mostly square operational panels with 12px radii and 1px hairlines.
- Shadows appear only for overlays and sticky context surfaces; ordinary panels use borders, not shadow-plus-border.
- Data visualization is the dominant material: aligned time-series plots, ranked congestion bars, comparison bands, decision timelines, and evidence tables.
- Network geometry remains available in intersection detail for spatial investigation, using dark asphalt geometry, thin lane markings, and actual signal states.
- Tables use aligned columns, tabular numerals, sticky headers where useful, and density appropriate for scanning.

## Spacing

Use a 4px base rhythm. Primary gaps: 8, 12, 16, 24, and 32px. Related control groups stay tight; page regions receive more separation than their internal elements.

## Application Frame

- A compact left navigation rail identifies the five product areas.
- A persistent top context bar shows simulation status, time, scenario, control strategy, and freshness without pretending unsupported controls exist.
- Desktop favors a wide operational canvas. Tablet collapses the navigation. Mobile stacks panels and converts wide tables into horizontally scrollable regions.

## Component Character

- Buttons are compact action controls with verbs, visible focus, disabled explanations, and no decorative gradients.
- Status chips are small, textual, and semantic; pills are reserved for these compact states.
- KPI summaries are an instrument strip rather than a grid of identical hero cards.
- Empty and error states explain whether data is unavailable, disabled, stale, inconsistent, or not yet collected.

## Data Visualization

- Prioritize time-series traffic performance, congestion rankings, control-decision evidence, and equivalent-run comparisons on overview surfaces.
- Use actual SUMO network geometry only where spatial investigation materially helps, especially intersection detail.
- Use line charts for time evolution, horizontal bars for rankings, grouped comparisons for equivalent experiment runs, and tables for traceable evidence.
- Avoid gauges, decorative sparklines, and charts without a decision question.
- Absolute values remain visible beside any improvement percentage.

## Motion

One authored application entrance fades and slightly resolves the main workspace. Live state changes use short color and opacity transitions. Respect `prefers-reduced-motion`; no looping decorative animation beyond meaningful live-status pulses.

## Accessibility

All interactive elements have visible focus, semantic labels, and minimum 40px practical targets where space allows. Color-coded states include text. Scrollbars, text selection, disabled states, and keyboard navigation are themed and testable.

