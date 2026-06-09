'''
TrustRank Terminal Fusion Engine (Asymmetric Normalization + TOPSIS)
---------------------------------------------------------------------
Fuses the dual-track TrustRank and BadRank scores into a final ranking.

Normalization strategy (asymmetric):
  1. Trust dimension: Log-MinMax normalization preserves order-of-magnitude
     stratification in the long-tail continuous distribution.
  2. Risk dimension: Pure linear MinMax normalization leverages the massive
     magnitude gap between extreme values to naturally crush floating-point noise.

Node cleansing: Strict registrar physical separation and anomaly character filtering.

TOPSIS: Computes geometric distances in the clean [0,1] x [0,1] space,
        generating the final composite reputation ranking.

Output: FINAL_TRUSLIST_RANKING.csv and TOPSIS scatter plot (PNG/PDF/SVG)
'''

import pandas as pd
import numpy as np
import os
import sys
import matplotlib.pyplot as plt


def _configure_plot_style():
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = ['Arial', 'DejaVu Sans', 'Microsoft YaHei', 'SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300


def _save_topsis_scatter(final_df, daily_dir):
    if final_df.empty:
        print("  WARNING: Current result is empty, skipping scatter plot generation.")
        return

    _configure_plot_style()
    fig, ax = plt.subplots(figsize=(8, 6))

    base_df = final_df.copy()
    if 'is_seed' in base_df.columns:
        seed_df = base_df[base_df['is_seed'] == True]
        base_df = base_df[base_df['is_seed'] == False]
    else:
        seed_df = pd.DataFrame(columns=base_df.columns)

    scatter = ax.scatter(
        base_df['trust_norm'],
        base_df['risk_norm'],
        s=12,
        alpha=0.35,
        c=base_df['truslist_score'],
        cmap='viridis',
        edgecolors='none'
    )

    if not seed_df.empty:
        ax.scatter(
            seed_df['trust_norm'],
            seed_df['risk_norm'],
            s=22,
            alpha=0.8,
            color='#ef4444',
            label='Seed / Malicious'
        )

    if 'is_hidden_target' in final_df.columns:
        hidden_df = final_df[final_df['is_hidden_target'] == True]
        if not hidden_df.empty:
            ax.scatter(
                hidden_df['trust_norm'],
                hidden_df['risk_norm'],
                s=26,
                alpha=0.9,
                color='#f97316',
                label='Hidden Target'
            )

    ax.scatter(1.0, 0.0, s=80, color='#22c55e', label='Ideal (A+)')
    ax.scatter(0.0, 1.0, s=80, color='#0ea5e9', label='Nadir (A-)')

    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.set_xlabel('Trust Dimension (Normalized)', fontsize=12)
    ax.set_ylabel('Risk Dimension (Normalized)', fontsize=12)
    ax.set_title('TrustRank TOPSIS Scatter Space', fontsize=14, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.4)

    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('TrustRank Score', fontsize=10)

    if not seed_df.empty or ('is_hidden_target' in final_df.columns and not hidden_df.empty):
        ax.legend(frameon=True, fontsize=9, loc='upper left')

    out_png = os.path.join(daily_dir, 'topsis_scatter.png')
    out_pdf = os.path.join(daily_dir, 'topsis_scatter.pdf')
    out_svg = os.path.join(daily_dir, 'topsis_scatter.svg')
    plt.tight_layout()
    plt.savefig(out_png, bbox_inches='tight')
    plt.savefig(out_pdf, bbox_inches='tight')
    plt.savefig(out_svg, format='svg', bbox_inches='tight')
    plt.close(fig)

    print(f"  TOPSIS scatter plot saved to: {out_png}")


def run_truslist_fusion(global_dir, daily_dir):
    print("\n" + "="*60)
    print("  Starting TrustRank Terminal Fusion Engine")
    print("  (Asymmetric Normalization + Node Cleansing + TOPSIS)")
    print("="*60)
    sys.stdout.flush()

    # ---------------------------------------------------------
    # Step 1 & 2: Load daily data and perform dual-dimension alignment
    # ---------------------------------------------------------
    print("  Step 1 & 2: Reading daily data and performing node dual-dimension alignment...")
    try:
        trust_df = pd.read_csv(os.path.join(daily_dir, 'final_trust_list.csv'))
        risk_df = pd.read_csv(os.path.join(daily_dir, 'final_badrank_scores.csv'))
    except Exception as e:
        print(f"  ERROR: Failed to read files. Check if Trust and Risk lists exist under {daily_dir}: {e}")
        return

    merged_df = pd.merge(trust_df[['node', 'trust_score']],
                         risk_df[['node', 'risk_score', 'is_seed']],
                         on='node', how='outer')

    # Fill missing values to zero
    merged_df['trust_score'] = merged_df['trust_score'].fillna(0)
    merged_df['risk_score'] = merged_df['risk_score'].fillna(0)
    merged_df['is_seed'] = merged_df['is_seed'].fillna(False)

    # ---------------------------------------------------------
    # Step 2.5: Node cleansing (registrar removal + anomaly filtering)
    # ---------------------------------------------------------
    print("  Step 2.5: Strict node cleansing (registrar removal + anomaly filtering)...")
    try:
        reg_df = pd.read_csv(os.path.join(global_dir, 'domain_registrar_full.csv'))
        all_registrars = set(reg_df['registrar'].astype(str).unique())
    except Exception:
        all_registrars = set()

    before_clean = len(merged_df)

    # 1. Remove registrar nodes
    if all_registrars:
        merged_df = merged_df[~merged_df['node'].isin(all_registrars)]

    # 2. Remove nodes without dots (invalid domains)
    merged_df = merged_df[merged_df['node'].str.contains(r'\.', na=False)]

    # 3. Remove nodes with illegal characters
    illegal_pattern = r'[\{\}\[\]\(\)<>\|\\\^\*\t\n\r]'
    merged_df = merged_df[~merged_df['node'].str.contains(illegal_pattern, na=False, regex=True)]

    cleaned_count = before_clean - len(merged_df)
    print(f"    Nodes removed: {cleaned_count:,} | Remaining valid nodes: {len(merged_df):,}")

    if merged_df.empty:
        print("  ERROR: All nodes removed after cleansing. Cannot continue.")
        return

    # ---------------------------------------------------------
    # Step 3: Asymmetric normalization
    # ---------------------------------------------------------
    print("\n  Step 3: Executing asymmetric normalization...")
    print("    - Trust dimension: Log-MinMax (preserving magnitude stratification)")
    print("    - Risk dimension: Pure linear MinMax (natural noise crushing)")

    # Trust dimension: Log-MinMax
    trust_log = np.log1p(merged_df['trust_score'])
    trust_log_min, trust_log_max = trust_log.min(), trust_log.max()
    if trust_log_max > trust_log_min:
        merged_df['trust_norm'] = (trust_log - trust_log_min) / (trust_log_max - trust_log_min)
    else:
        merged_df['trust_norm'] = 0.0
    print(f"    Trust: [{trust_log_min:.4f}, {trust_log_max:.4f}] -> [0.0, 1.0]")

    # Risk dimension: Pure linear MinMax
    risk_min, risk_max = merged_df['risk_score'].min(), merged_df['risk_score'].max()
    if risk_max > risk_min:
        merged_df['risk_norm'] = (merged_df['risk_score'] - risk_min) / (risk_max - risk_min)
        print(f"    Risk:  [{risk_min:.2e}, {risk_max:.2e}] -> [0.0, 1.0]")
    else:
        merged_df['risk_norm'] = 0.0
        print(f"    Risk:  uniform distribution, all zeros")

    # ---------------------------------------------------------
    # Step 4: TOPSIS geometric dimensionality reduction
    # ---------------------------------------------------------
    print("\n  Step 4: Computing objective geometric distances (TOPSIS space mapping)...")

    ideal_point = np.array([1.0, 0.0])   # Max trust, min risk
    nadir_point = np.array([0.0, 1.0])   # Min trust, max risk

    data_points = merged_df[['trust_norm', 'risk_norm']].values

    d_plus = np.sqrt(np.sum((data_points - ideal_point)**2, axis=1))
    d_minus = np.sqrt(np.sum((data_points - nadir_point)**2, axis=1))

    merged_df['truslist_score'] = d_minus / (d_plus + d_minus + 1e-12)

    # ---------------------------------------------------------
    # Step 5: Generate final ranking
    # ---------------------------------------------------------
    print("  Step 5: Exporting daily final authority ranking...")
    final_df = merged_df.sort_values(by='truslist_score', ascending=False).reset_index(drop=True)
    final_df['final_rank'] = final_df.index + 1

    cols_order = ['final_rank', 'node', 'is_seed', 'truslist_score',
                  'trust_norm', 'risk_norm', 'trust_score', 'risk_score']

    # Preserve blind-test labels if present from ablation experiments
    if 'is_hidden_target' in merged_df.columns:
        cols_order.insert(3, 'is_hidden_target')

    final_df = final_df[cols_order]

    out_path = os.path.join(daily_dir, 'FINAL_TRUSLIST_RANKING.csv')
    final_df.to_csv(out_path, index=False)

    print(f"  DONE: Fusion complete! Daily final ranking saved to: {out_path}")

    # ---------------------------------------------------------
    # Step 5.5: Generate TOPSIS scatter plot
    # ---------------------------------------------------------
    print("  Step 5.5: Generating TOPSIS spatial scatter plot...")
    _save_topsis_scatter(final_df, daily_dir)

    # ---------------------------------------------------------
    # Result report
    # ---------------------------------------------------------
    print("\n  === TrustRank Top 10 Safety Benchmarks (Most Trusted & Absolutely Safe) ===")
    print(final_df[['final_rank', 'node', 'trust_norm', 'risk_norm', 'truslist_score']].head(10).to_string(index=False))

    print("\n  --- Algorithm Highlight: High-Trust Nodes with Elevated Risk (Abused Infrastructure) ---")
    abused_nodes = final_df[(final_df['trust_norm'] > 0.3) & (final_df['risk_norm'] > 1e-4)]
    if not abused_nodes.empty:
        abused_nodes = abused_nodes.sort_values(by='risk_norm', ascending=False)
        print(abused_nodes[['final_rank', 'node', 'trust_norm', 'risk_norm', 'truslist_score']].head(10).to_string(index=False))
    else:
        print("    (No high-trust nodes currently exhibit elevated risk)")

    print("\n  --- Bottom 5 Sunk Nodes (Pure Malicious Seeds) ---")
    if 'is_seed' in final_df.columns:
        print(final_df[['final_rank', 'node', 'is_seed', 'trust_norm', 'risk_norm', 'truslist_score']].tail(5).to_string(index=False))
    else:
        print(final_df[['final_rank', 'node', 'trust_norm', 'risk_norm', 'truslist_score']].tail(5).to_string(index=False))


# ==========================================
# Standalone debug entry point
# ==========================================
if __name__ == "__main__":
    try:
        SRC_DIR = os.path.dirname(os.path.abspath(__file__))
        PROJECT_ROOT = os.path.dirname(SRC_DIR)

        TEST_GLOBAL = os.path.join(PROJECT_ROOT, 'output', 'global')
        TEST_DAILY = os.path.join(PROJECT_ROOT, 'output', 'daily', 'test_day')

        if not os.path.exists(TEST_DAILY):
            print(f"  Test directory {TEST_DAILY} does not exist, creating...")
            os.makedirs(TEST_DAILY, exist_ok=True)

        print(f"  [Standalone Debug Mode] TOPSIS Fusion")
        print(f"    Project root: {PROJECT_ROOT}")
        print(f"    Global directory: {TEST_GLOBAL}")
        print(f"    Daily directory: {TEST_DAILY}")

        run_truslist_fusion(global_dir=TEST_GLOBAL, daily_dir=TEST_DAILY)

    except KeyboardInterrupt:
        print("\n  User interrupted.")
    except Exception as e:
        print(f"\n  ERROR: Fatal error during execution: {e}")
