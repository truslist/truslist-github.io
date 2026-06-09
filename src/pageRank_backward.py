'''
TrustRank BadRank Reverse Risk Walk Engine (Dual-Bayesian Armor Version)
-------------------------------------------------------------------------
Performs reverse risk propagation on the heterogeneous graph with
two complementary defense mechanisms:

  1. Matrix Armor (Propagation Armor): Suppresses risk conduction efficiency
     when risk flow passes through high-popularity infrastructure nodes.
     High-traffic legitimate domains naturally dilute risk as it passes through.

  2. Seed Armor (Source Armor): Suppresses the initial risk energy released
     by high-popularity malicious seeds. Seeds that are also globally popular
     receive proportionally less initial risk energy.

The armor factor for each node is: 1 - normalized_log10(trust_score)
Only nodes with trust_score approaching the maximum (truly large factories)
achieve near-zero permeability.

Output: final_badrank_scores.csv containing risk scores and percentile ranks
'''

import pandas as pd
import numpy as np
import scipy.sparse as sp
import os
import sys

def run_bad_walk(global_dir, daily_dir, alpha=0.85, max_iter=100, tol=1e-8):
    print("\n" + "="*60)
    print("  Starting BadRank Risk-Seeking Engine (Dual-Bayesian Armor Version)")
    print("="*60)
    sys.stdout.flush()

    # ---------------------------------------------------------
    # Step 1: Load data (global + daily)
    # ---------------------------------------------------------
    print("  Step 1: Loading global reverse graph and daily popularity prior (Trust Armor)...")
    try:
        M_rev = sp.load_npz(os.path.join(global_dir, 'reverse_transition_matrix.npz'))
        node_mapping = pd.read_csv(os.path.join(global_dir, 'reverse_node_mapping.csv'))

        # Daily data: the forward trust list computed earlier (used to hang armor)
        trust_df = pd.read_csv(os.path.join(daily_dir, 'final_trust_list.csv'))

        n_nodes = M_rev.shape[0]
    except Exception as e:
        print(f"  ERROR: Load failed. Check global or daily directory for required files: {e}")
        return

    node_to_idx = dict(zip(node_mapping['node'].astype(str), node_mapping['idx']))

    # ---------------------------------------------------------
    # Step 1.5: Build universal Bayesian armor factors (1 - Pop_Pct)
    # ---------------------------------------------------------
    print("  Step 1.5: Computing network-wide Bayesian permeability rates (Armor Factors)...")
    pop_scores = np.zeros(n_nodes)
    trust_map = dict(zip(trust_df['node'].astype(str), trust_df['trust_score']))

    for node, idx in node_to_idx.items():
        pop_scores[idx] = trust_map.get(node, 0.0)

    # Log-MinMax normalization: preserves order-of-magnitude stratification
    # Add 1e-12 to prevent log(0)
    log_pop = np.log10(pop_scores + 1e-12)
    pop_min, pop_max = log_pop.min(), log_pop.max()

    # Map trust prestige to [0, 1] absolute space (preserving magnitude layers)
    pop_norm = (log_pop - pop_min) / (pop_max - pop_min + 1e-12)

    # Core Bayesian factor: 1 - probability_of_being_generic_infrastructure
    # Only truly large factories (pop_norm near 1) achieve extremely low permeability
    armor_factors = 1.0 - pop_norm
    armor_factors = np.clip(armor_factors, 0.001, 1.0)  # Physical floor to prevent absolute deadlock

    # [Defense A] Matrix suppression: risk dissipates when flowing through large factories
    Armor_Matrix = sp.diags(armor_factors)
    M_rev = M_rev.dot(Armor_Matrix)

    print(f"    Propagation armor mounted (max factory immunity: {100*(1-np.min(armor_factors)):.1f}%)")

    # ---------------------------------------------------------
    # Step 2: Build Bayesian-weighted seed vector (Seed Injection)
    # ---------------------------------------------------------
    print("  Step 2: Executing Bayesian risk energy injection (reading seeds from global directory)...")
    v_bad = np.zeros(n_nodes)

    mal_path = os.path.join(global_dir, 'malicious_domains.txt')
    try:
        with open(mal_path, 'r', encoding='utf-8') as f:
            seeds = [line.strip().lower() for line in f if line.strip()]
    except Exception as e:
        print(f"  ERROR: Failed to read malicious seeds file: {e}")
        return

    matched_indices = []
    for domain in seeds:
        if domain in node_to_idx:
            idx = node_to_idx[domain]
            # [Defense B] Initial weighting: higher popularity seeds receive less initial energy
            v_bad[idx] = armor_factors[idx]
            matched_indices.append(idx)

    if not matched_indices:
        print("  ERROR: No valid seeds matched in the graph")
        return

    # Normalize total energy to 1.0
    v_bad = v_bad / np.sum(v_bad)
    print(f"    Successfully injected {len(matched_indices):,} seeds")
    print(f"    After Bayesian correction, max seed energy: {np.max(v_bad):.4e}")

    # ---------------------------------------------------------
    # Step 3: Iterative computation
    # ---------------------------------------------------------
    print(f"  Step 3: Risk energy backflow iteration (alpha={alpha})...")
    R = v_bad.copy()

    for i in range(1, max_iter + 1):
        R_next = alpha * (R @ M_rev) + (1 - alpha) * v_bad
        err = np.sum(np.abs(R_next - R))
        R = R_next

        if i % 10 == 0 or i == 1:
            print(f"    - Iteration {i:2d}: error={err:.2e} | total system risk energy={np.sum(R):.4f}")

        if err < tol:
            print(f"    Reached stability")
            break
    sys.stdout.flush()

    # ---------------------------------------------------------
    # Step 4: Output results (save to daily directory)
    # ---------------------------------------------------------
    print("  Step 4: Exporting daily risk ranking list...")
    node_mapping['risk_score'] = R
    seed_set = set(seeds)
    node_mapping['is_seed'] = node_mapping['node'].apply(lambda x: x in seed_set)

    # Compute risk percentile rank for downstream fusion
    node_mapping['risk_pct'] = node_mapping['risk_score'].rank(pct=True)

    out_path = os.path.join(daily_dir, 'final_badrank_scores.csv')
    node_mapping.sort_values(by='risk_score', ascending=False).to_csv(out_path, index=False)
    print(f"  DONE: Risk-seeking complete! Daily results saved to: {out_path}")


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
            os.makedirs(TEST_DAILY)

        print(f"  [Standalone Debug Mode] BadRank Reverse Walk")
        print(f"    Project root: {PROJECT_ROOT}")
        print(f"    Global directory: {TEST_GLOBAL}")
        print(f"    Daily directory: {TEST_DAILY}")

        run_bad_walk(global_dir=TEST_GLOBAL, daily_dir=TEST_DAILY)

    except KeyboardInterrupt:
        print("\n  User interrupted.")
    except Exception as e:
        print(f"\n  ERROR: Fatal error during execution: {e}")
