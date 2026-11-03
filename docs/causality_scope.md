# Causality Scope

This document records the current project boundary for causal inference.

## Current Position

The active lakehouse does not run causal-inference notebooks. The previous Gold 04 notebooks for Granger-style testing, conditional Granger testing, and PCMCI/PCMCI+ were removed from the active implementation.

The reason is methodological, not technical. Those methods require assumptions that are not justified by the currently collected FRED/ALFRED/current-only dataset alone, especially the absence of unobserved confounding. A macroeconomic lakehouse with a limited set of observed series cannot safely claim that all relevant common causes have been measured.

## What Remains Active

The active Gold analytical flow is:

```text
gold/03_gold_fred_asof_features.py
  -> gold.fred_transformed_features_long
  -> optional gold/03b_gold_lagged_correlation_analysis.py
```

`03b` is correlation analysis only. It ranks lagged linear co-movement for EDA and dashboards. It must not be used as causal evidence, and it must not be used to exclude variables from future causal analysis.

## What Counts As Causal Work Later

Future causal work should start from an explicit research design rather than from correlation rankings. At minimum it should state:

- target causal question
- treatment/exposure definition
- outcome definition
- time scale and as-of semantics
- proposed adjustment set
- known and suspected unobserved confounders
- identification assumptions
- sensitivity analysis plan
- falsification or placebo checks

Candidate methods may still be useful later, but only after the assumptions are made explicit and judged plausible for a concrete research question.

## Practical Rule

Use the current lakehouse for:

- point-in-time feature construction
- unit-aware transformations
- stationarity-aware exploratory analysis
- lagged correlation dashboards
- research preparation

Do not use the current lakehouse outputs to state that one series causes another.
