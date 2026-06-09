# TrustRank: Building Trust-Based Anti-Manipulation Domain Rankings for Regional Networks

**TrustRank** is a graph-based domain reputation ranking framework designed for regional networks. It addresses two critical problems with existing global top lists: (1) the **representativeness deficit** — globally prominent domains crowd out locally relevant ones due to censorship and socioeconomic diversity, and (2) **manipulation vulnerability** — volume-based rankings are easily gamed by localized botnets and DGAs in smaller networks.

> 📄 This repository accompanies the paper: *"TrustRank: Building Trust-Based Anti-Manipulation Domain Rankings for Regional Networks"* (ICICS 2026).

---

## Framework Overview

TrustRank constructs a **heterogeneous graph** linking three entity types — client IPs, domains, and registrars — then employs **dual propagation channels** that independently model benign authority and malicious risk:

`
                    ┌──────────────────────────┐
                    │   PDNS Query Logs (CSV)   │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │  Stage 1: Prior Vector    │
                    │  Spatiotemporal features  │
                    │  + IP authority weighting │
                    │  + Weighted Borda-1000    │
                    └────────────┬─────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                                     │
  ┌───────────▼───────────┐           ┌─────────────▼─────────────┐
  │  Stage 2: TrustRank    │           │  Stage 3: BadRank         │
  │  Forward PageRank on   │           │  Reverse PageRank with    │
  │  heterogeneous graph   │           │  Dual-Bayesian Armor      │
  │  (IP→Domain→Registrar) │           │  (Risk backflow + armor)  │
  └───────────┬───────────┘           └─────────────┬─────────────┘
              │                                     │
              └──────────────────┬──────────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │  Stage 4: TOPSIS Fusion   │
                    │  Asymmetric normalization │
                    │  + Geometric ranking       │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │  FINAL_TRUSLIST_RANKING   │
                    └──────────────────────────┘
`

### Key Design Principles

| Principle | Implementation |
|-----------|---------------|
| **Anti-manipulation** | IP weight uses log-smoothing: W_IP = √(norm(ln(C+1)) · norm(ln(D+1))) — suppresses single-IP traffic inflation |
| **Registrar anchoring** | Registrar → Domain edges use 1/N equal weight — boutique registrars provide strong backing; mega-registrars are naturally sparse |
| **Dual-Bayesian Armor** | High-popularity domains dilute risk flow (Matrix Armor); popular seeds receive less initial risk energy (Seed Armor) |
| **Asymmetric normalization** | Trust: Log-MinMax preserves magnitude stratification; Risk: linear MinMax crushes floating-point noise |
| **Geometric fusion** | TOPSIS evaluates each domain against ideal (max trust, min risk) and nadir (min trust, max risk) in a clean 2D space |

---

## Experimental Results

Evaluation on a **regional ISP network with one million users** over 8 days of PDNS data.

### 1. Ranking Representativeness

Global lists fundamentally fail to represent regional realities:

| Comparison | Top-100 Overlap | Top-1K Overlap |
|------------|:---:|:---:|
| TrustRank ↔ SecRank (national) | 27% | 41% |
| TrustRank ↔ Tranco (global) | 18% | 25% |
| TrustRank ↔ Umbrella (global) | 14% | 22% |
| SecRank ↔ Tranco | 6% | 15% |

The minimal overlap between national and global rankings (6–15%) confirms the **structural representativeness deficit** of global lists in regional contexts.

### 2. Temporal Stability

Measured via Spearman rank correlation (ρ) against Day-1 baseline over 7 consecutive days. While both SecRank and Umbrella exhibit sharp declines on Day 2, **TrustRank maintains consistently higher stability** by decoupling behavioral dynamics from multi-source topological relationships.

### 3. Manipulation Resistance (CDF Analysis)

Under diverse traffic inflation attacks (varying IP count, request volume, and attack intensity):

- SecRank and Umbrella CDF curves are **steep and left-shifted** — manipulated domains readily penetrate top tiers
- TrustRank curves are **consistently right-shifted** — demonstrating superior anti-manipulation under both strong and weak attacks
- The **Dual-Bayesian Armor** produces a bimodal penalty distribution: top-tier infrastructure is protected, while middle/tail malicious campaigns are systematically demoted

### 4. Phishing Domain Coverage

Using Jaro-Winkler string similarity (≥0.75) to resolve phishing targets to legitimate domains:

| Top-K | TrustRank | SecRank | Improvement |
|-------|:---:|:---:|:---:|
| Top-100 | **143** | 24 | **5.96×** |
| Top-500 | — | — | **2.85×** |

TrustRank surfaces high-utility legitimate services that adversaries actively impersonate, making it a **practical security primitive** for brand protection and phishing defense.

---

## Repository Structure

`
TrustRank/
├── README.md
├── LICENSE
├── requirements.txt
│
├── src/
│   ├── daily_pipeline.py            # End-to-end pipeline orchestrator
│   ├── prior_v.py                   # Stage 1: Spatiotemporal prior vector
│   ├── build_forward_graph.py       # Forward heterogeneous graph builder
│   ├── build_backward_graph.py      # Reverse risk graph builder
│   ├── pageRank_forward.py          # Stage 2: TrustRank (forward PageRank)
│   ├── pageRank_backward.py         # Stage 3: BadRank + Dual-Bayesian Armor
│   └── topist.py                    # Stage 4: TOPSIS fusion & ranking
│
├── output/global/                   # Pre-built global graph data
│   ├── graph_edges.csv              # Domain→Domain ecological edges
│   ├── domain_registrar_full.csv    # Domain→Registrar mappings
│   ├── malicious_domains.txt        # Malicious seed domains (CTI feeds)
│   ├── forward_transition_matrix.npz
│   ├── reverse_transition_matrix.npz
│   ├── forward_node_mapping.csv
│   └── reverse_node_mapping.csv
│
└── data/
    └── sample.csv                   # Sample PDNS log (500 rows)
`

---

## Quick Start

### 1. Install dependencies

`ash
pip install -r requirements.txt
`

### 2. Run the full pipeline

Place your PDNS query logs (CSV) under data/pdns/, then:

`ash
python src/daily_pipeline.py
`

**Required CSV columns:** domain (dns.rrname / qdn / domain), client IP (src_ip / client_ip / ip), timestamp. Optional: type (only type==1 is kept), equest_cnt.

### 3. Or run individual stages

`ash
python src/prior_v.py               # Stage 1 only
python src/pageRank_forward.py      # Stage 2 only
python src/pageRank_backward.py     # Stage 3 only
python src/topist.py                # Stage 4 only
`

### 4. Rebuild the global graph (optional)

The pre-built transition matrices are included. To rebuild from scratch:

`ash
python src/build_forward_graph.py
python src/build_backward_graph.py
`

### 5. Output

Final ranking is written to output/daily/<task>/FINAL_TRUSLIST_RANKING.csv:

| Column | Description |
|--------|-------------|
| inal_rank | Overall rank (1 = most trusted) |
| 
ode | Second-level domain (SLD) |
| is_seed | Known malicious seed flag |
| 	ruslist_score | Final composite TOPSIS score [0, 1] |
| 	rust_norm | Normalized trust dimension |
| isk_norm | Normalized risk dimension |
| 	rust_score | Raw TrustRank PageRank score |
| isk_score | Raw BadRank PageRank score |

---

## Dependencies

- Python ≥ 3.8
- 
umpy, scipy (sparse matrix operations)
- pandas (data processing)
- matplotlib (TOPSIS scatter visualization)
- 	ldextract (SLD extraction)
- 	qdm (progress bars)

---

## Citation

`ibtex
@inproceedings{trustrank2026,
  title     = {TrustRank: Building Trust-Based Anti-Manipulation
               Domain Rankings for Regional Networks},
  author    = {Anonymous},
  booktitle = {Proceedings of the 28th International Conference on
               Information and Communications Security (ICICS)},
  year      = {2026}
}
`

## License

MIT — see [LICENSE](LICENSE).
