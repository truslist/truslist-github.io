# TrusList: A Regional Domain Ranking Methodology


> **Anonymous submission** -- All author information is withheld for double-blind review.
> This repository is the open-source artifact accompanying the paper submission.
>
> **WARNING: The raw passive DNS dataset cannot be released due to privacy and
> data-use agreement restrictions. Only the source code is provided.**

---

## What is TrusList?

The Domain Name System (DNS) underpins virtually all network communications, and
**domain top lists** -- rankings of the most popular or frequently accessed domains --
are widely used in DNS security research, malicious domain detection, and anti-phishing
benchmarking. However, existing lists such as Alexa, Umbrella, Tranco, and SecRank
suffer from two fundamental problems when applied to **regional networks**:

1. **Regional representativeness deficit.** Global lists aggregate worldwide traffic and
   systematically overlook locally important domains (government, academic, and
   public-service sites), while globally popular but locally irrelevant domains dominate.
2. **Manipulation vulnerability.** Rankings based purely on traffic volume can be gamed
   by botnets inflating DNS query counts -- a risk amplified in regional networks whose
   smaller scale makes them more susceptible to such attacks.

**TrusList** addresses both problems through a multi-dimensional ranking methodology that
fuses behavioral, institutional, and structural signals derived from passive DNS (PDNS) data.

---

## Methodology

TrusList evaluates each domain across four complementary dimensions and fuses them via
the **Analytic Hierarchy Process (AHP)**:

| Score | Dimension                          | AHP Weight |
|-------|------------------------------------|-----------|
| Sd    | Spatio-temporal query behavior     | 0.479     |
| St    | TLD credibility                    | 0.338     |
| Sr    | Registrar reputation               | 0.112     |
| Sp    | Structural inter-domain link influence | 0.071  |

The final ranking score is:

```
F = 0.479 * Sd + 0.338 * St + 0.112 * Sr + 0.071 * Sp
```

AHP weights are derived from a pairwise comparison matrix (CR = 0.008 < 0.1), ensuring
expert-guided consistency without requiring labeled training data.

### Scoring Dimensions

- **Sd -- Spatio-temporal Query Behavior (weight 0.479).**
  Each IP is modeled as an independent voter. Its preference for a domain is measured by
  the geometric mean of normalized query volume and temporal spread (active 10-minute slots
  per day). IP weights are adjusted by a KL-divergence-based behavioral consistency filter
  that penalizes crawlers and botnets. Scores are aggregated via weighted Borda count.

- **St -- TLD Credibility (weight 0.338).**
  Strictly verified TLDs (.gov, .edu, .gov.cn, .edu.cn, .ac.cn, etc.) require official
  credentials from applicants, yielding substantially lower abuse rates. TrusList assigns
  higher scores to these namespaces.

- **Sr -- Registrar Reputation (weight 0.112).**
  Derived from three components: popularity of managed domains, Bayesian-smoothed malicious
  domain rate, and a compliance score reflecting jurisdiction-level identity verification
  rigor -- aggregated via geometric mean.

- **Sp -- Structural Link Influence (weight 0.071).**
  Inspired by PageRank: link values propagate from a curated set of ~25,000 reference domains
  (top-10K union of Tranco, SecRank, and TrusList) to domains they hyperlink to, using
  web-graph data.

---

## Experimental Results

All experiments use large-scale PDNS data collected from a regional network with over one
million users.

### Representativeness

Top-N overlap between TrusList and reference rankings reveals a gradient aligned with
data-source proximity to the regional network:

| Reference List | Overlap (Top-100 to Top-10K) | Scope            |
|----------------|------------------------------|------------------|
| SecRank        | 10% to 42%                   | National (China) |
| Tranco         | 10% to 21%                   | Global aggregate |
| Umbrella       | 7%  to 10%                   | Global DNS       |

The non-trivial overlap with Tranco (up to 21%) confirms TrusList is not insular -- it
captures both internationally recognized platforms and critical local domains.

### Temporal Stability (7-day Spearman rho)

| System       | rho range     | Trend     |
|--------------|---------------|-----------|
| **TrusList** | **0.94-0.97** | Flat      |
| SecRank      | 0.87-0.94     | Declining |
| Umbrella     | 0.87-0.94     | Declining |

Stability stems from multi-factor fusion: registrar reputation, TLD credibility, and link
structure are structurally stable signals that attenuate transient query spikes.

### Attack Resistance (DGA Simulation)

Even a partially adaptive attacker (high-reputation registrar, common TLD .com) faces a
hard score ceiling:

```
F_max_adv = 0.479 * Sd + 0.268 <= 0.747
```

This falls well below the F ~= 0.85 achieved by typical top-tier legitimate domains,
because Sp remains near zero for any newly registered domain and St awards full credit
only to institutionally restricted namespaces.

### Phishing Target Coverage

Using Jaro-Winkler similarity >= 0.75 against real-world phishing feeds:

| Top-K   | TrusList vs. SecRank           |
|---------|--------------------------------|
| Top-100 | **5.96x** (143 vs. 24 domains) |
| Top-500 | **2.85x**                      |

### Ablation Study

Removing each module individually confirms that **registrar reputation (Sr) is the primary
defensive bottleneck**: its removal causes the largest degradation in both stability
(-0.0311 Spearman) and attack resistance (malicious domain average rank drops by -3,167).
TLD credibility (St) and link structure (Sp) provide secondary defensive layers.

---

## Repository Structure

```
DomainRanking/
|-- config/
|   |-- setting.py              # Global configuration (paths, AHP weights, module parameters)
|-- src/
|   |-- main.py                 # Pipeline entry point
|   |-- modules/
|   |   |-- user_behavior.py    # Sd: spatio-temporal query scoring
|   |   |-- tld_score.py        # St: TLD credibility scoring
|   |   |-- registrar_score.py  # Sr: registrar reputation scoring
|   |   |-- link_structure.py   # Sp: structural link influence scoring
|   |   |-- ahp.py              # AHP weight fusion
|   |   |-- umbrella.py         # Umbrella-style baseline ranker
|   |-- units/
|       |-- merge.py            # Rank merging utilities
|-- data/                       # Data directory (see notice below)
    |-- filtered/               # Pre-processed PDNS records (NOT PROVIDED)
    |-- rank/                   # Reference rankings: SecRank, Tranco
    |-- raw/                    # Auxiliary data: registrar DB, TLD policy, phishing feeds
    |-- result/                 # Pipeline outputs
```

> **Data Availability Notice:**
> The passive DNS dataset used in this paper was collected under a data-sharing agreement
> with the network operator and contains potentially sensitive network traffic information.
> It **cannot be publicly released** due to privacy obligations and the terms of the
> data-use agreement. The `data/` directory structure above is provided for reference only.
> To run the pipeline, you must supply your own PDNS data in the same format.

---

## Running the Pipeline

### Requirements

```
Python >= 3.8
pandas
numpy
tldextract
tqdm
```

Install dependencies:

```bash
pip install pandas numpy tldextract tqdm
```

### Configuration

Edit `config/setting.py` to point to your own data files:

```python
DATA_SOURCES = {
    'pdns_data_path': 'path/to/your/pdns/files',
    'registrar_data_path': 'path/to/registrar_info.csv',
    'tld_data_path': 'path/to/import_domain.csv',
    'link_data_path': 'path/to/Link_data.csv',
    'phish_tank_path': 'path/to/threat_intelligence.csv',
    'registrar_accredited': 'path/to/Accredited-Registrars.csv',
    'secrank_data_path': 'path/to/SecRank/YYYY-MM-DD',
    'tranco_data_path': 'path/to/tranco.csv',
}
```

Also update the AHP weights and module parameters in the same file if needed.

### Execution

```bash
cd DomainRanking
python src/main.py
```

The pipeline will interactively prompt you to select a PDNS input file, then execute all
four scoring modules sequentially and save the final ranked list to `data/result/`.

---

## Citation

> **Anonymous submission under double-blind review.**
> Citation information will be provided upon paper acceptance.

---

## License

To be determined upon acceptance. The source code is provided solely for artifact
evaluation purposes during the review process.
