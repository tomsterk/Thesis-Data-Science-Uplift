# Thesis Code — Uplift Modeling MTUM

This code repository contains all the SQL queries, datasets and notebooks that were used to benchmark the binary uplift RF models versus the MTUM can be found. 
---

## Project Structure

```
Thesis Code/
│
├── Functions/
│   ├── __init__.py
│   └── data_utils.py                  # Functions used for modeling, and visualization
│
├── Queries Metabase/
│   ├── Pretreatment covariates.sql              # Covariates query (modeling)
│   ├── Pretreatment covariates deployement.sql  # Covariates query (deployment)
│   ├── Churned bought categories 2026-02-11.sql # Purchase history of churned customers
│   ├── Total churned customers 2026-02-11.sql   # Full churned customer population
│   ├── Performance experimental phase 1.sql     # Evaluation dataset phase 1
│   ├── Performance experimental phase 2.sql     # Evaluation dataset phase 1
|
├── Data/
│   ├── covariates_modeling_uplift_models_2026-03-13.csv
│   ├── covariates_deployment_dataset_2026-03-17.csv
│   ├── df_preds.csv
│   ├── experimental_phase_1.csv        # Customer-level results Phase 1
│   └── Output_phase_2/                 # Customer-level results Phase 2
│
├── Input_phase_2/
│   ├── control_selected_binary.csv     # Control group selection — Binary RF
│   ├── control_selected_multi.csv      # Control group selection — MTUM
│   ├── treatment_selected_binary.csv   # Treatment group selection — Binary RF
│   ├── treatment_selected_multi.csv    # Treatment group selection — MTUM
│   ├── *_2.csv                         # Revised versions after CRM correction
│   ├── Correction_crm_users.csv        # CRM correction file
│   └── Upload_crm_phase_2_winback.csv  # Final upload file for CRM system
│
├── Output/
│   ├── phase1_significance_results_2026-04-15.xlsx  # Significance tests Phase 1
│   ├── phase2_significance_results_2026-04-15.xlsx  # Significance tests Phase 2
│   ├── significance_tests.xlsx
│   ├── uplift_significance_results.xlsx
│   ├── ttest_results.xlsx
│   ├── gender_results.xlsx
│   ├── qini_bins_binary_uplift.xlsx
│   ├── cate_explorer.html               # Interactive CATE exploration
│   ├── cate_explorer_v2.html
│   ├── Output_qini_curves/              # Qini curve plots
│   ├── MTUM_phase_1_output_distribution/ # Distribution plots MTUM predictions
│   └── classification_output/
│
├── Notebooks (see below)
└── catboost_info/                        # Auto-generated CatBoost training logs
```

## Notebooks — Execution Order

The notebooks follow the two-phase experimental design of the thesis. Below is the recommended reading/execution order.

### Phase 1 — Model Training & Evaluation

| # | Notebook | Description |
|---|----------|-------------|
| 1 | `Covariates analysis.ipynb` | Exploratory analysis of pretreatment covariates used as model features. |
| 2 | `MMOA multi outcome prediction phase 1.ipynb` | Trains the multi-outcome prediction models (reaction, sales, margin) that feed into MTUM. |
| 3 | `MMOA calibration and evaluation phase 1.ipynb` | Evaluates calibration and predictive performance of the multi-outcome models. |
| 4 | `MMOA uplift phase 1.ipynb` | Computes MTUM uplift scores and Qini curves on Phase 1 experimental data. |
| 5 | `Binary RF uplift experimental phase 1.ipynb` | Trains and evaluates the Binary Random Forest uplift model on Phase 1 data. |
| 6 | `Randomness check experimental phase 1.ipynb` | Validates randomisation quality of treatment/control assignment in Phase 1. |
| 7 | `Effectiveness experiments.ipynb` | Runs significance tests (pooled & per-arm) across both phases. |

