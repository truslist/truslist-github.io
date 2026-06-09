'''
TrustRank Forward Trust Walk Engine (Personalized PageRank)
------------------------------------------------------------
Performs forward trust propagation on the heterogeneous graph.

Logic:
  1. Load the global forward transition matrix M and node mapping from global_dir
  2. Inject the daily prior energy vector v from pagerank_prior_v.csv
  3. Run iterative PageRank: PR = alpha * PR @ M + (1 - alpha) * v
  4. Filter out registrars and invalid characters; output the daily trust list

Note: This is a trust-only computation. Malicious seed filtering is
      deliberately removed here and deferred to the BadRank + TOPSIS stages,
      maintaining strict separation of concerns.
'''

import pandas as pd
import numpy as np
import scipy.sparse as sp
import os
import sys

def run_trust_walk(global_dir, daily_dir, alpha=0.85, max_iter=100, tol=1e-6):
    print("\n" + "="*60)
    print("  Initializing TrustRank Forward Trust Walk Engine...")
    print("="*60)
    sys.stdout.flush()

    # ---------------------------------------------------------
    # 1. Load global graph data
    # ---------------------------------------------------------
    print("  Step 1: Loading graph matrix and dictionary mapping from global directory...")
    try:
        M = sp.load_npz(os.path.join(global_dir, 'forward_transition_matrix.npz'))
        node_mapping = pd.read_csv(os.path.join(global_dir, 'forward_node_mapping.csv'))

        reg_df = pd.read_csv(os.path.join(global_dir, 'domain_registrar_full.csv'))
        all_registrars = set(reg_df['registrar'].astype(str).unique())

        n_nodes = M.shape[0]
        print(f"    Global graph loaded successfully: {n_nodes:,} nodes")
    except Exception as e:
        print(f"  ERROR: Load failed. Please check files under {global_dir}: {e}")
        return

    node_to_idx = dict(zip(node_mapping['node'].astype(str), node_mapping['idx']))

    # ---------------------------------------------------------
    # 2. Build prior vector v
    # ---------------------------------------------------------
    print("  Step 2: Reading daily spatiotemporal allocation, injecting prior energy (Teleportation Vector)...")
    try:
        prior_df = pd.read_csv(os.path.join(daily_dir, 'pagerank_prior_v.csv'))
        v = np.zeros(n_nodes)
        matched = 0
        for _, row in prior_df.iterrows():
            domain = str(row['sld'])
            if domain in node_to_idx:
                v[node_to_idx[domain]] = row['prior_probability']
                matched += 1

        v = v / np.sum(v) if np.sum(v) > 0 else v
        print(f"    Injected initial trust backing for {matched:,} active domains")
    except Exception as e:
        print(f"  ERROR: Failed to build prior vector. Check if prior file exists under {daily_dir}: {e}")
        return

    # ---------------------------------------------------------
    # 3. Execute graph dynamics iteration
    # ---------------------------------------------------------
    print(f"  Step 3: Starting global graph dynamics backflow iteration (alpha={alpha})...")
    PR = v.copy()
    for i in range(1, max_iter + 1):
        PR_next = alpha * (PR @ M) + (1 - alpha) * v
        err = np.sum(np.abs(PR_next - PR))
        PR = PR_next

        if i % 10 == 0 or i == 1:
            print(f"    - Iteration {i:2d}: error = {err:.2e}")

        if err < tol:
            print(f"    Model converged (iteration {i})")
            break
    sys.stdout.flush()

    # ---------------------------------------------------------
    # 4. Filter and output daily results
    # ---------------------------------------------------------
    print("  Step 4: Performing list cleansing (filtering registrars and invalid characters)...")
    node_mapping['trust_score'] = PR

    # Malicious domain filtering is intentionally removed here.
    # Punishing malicious nodes is delegated to the Risk dimension and TOPSIS.

    final_list = node_mapping[
        (~node_mapping['node'].isin(all_registrars)) &
        (node_mapping['node'].str.contains(r'\.', na=False))  # ensure valid domain with dot
    ].copy()

    final_list = final_list.sort_values(by='trust_score', ascending=False).reset_index(drop=True)

    out_path = os.path.join(daily_dir, 'final_trust_list.csv')
    final_list[['node', 'trust_score']].to_csv(out_path, index=False)

    print(f"  DONE: Daily forward trust list generated and saved to: {out_path}")


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

        print(f"  [Standalone Debug Mode] TrustRank Forward Walk")
        print(f"    Project root: {PROJECT_ROOT}")
        print(f"    Global directory: {TEST_GLOBAL}")
        print(f"    Daily directory: {TEST_DAILY}")

        run_trust_walk(global_dir=TEST_GLOBAL, daily_dir=TEST_DAILY)

    except KeyboardInterrupt:
        print("\n  User interrupted.")
    except Exception as e:
        print(f"\n  ERROR: Fatal error during execution: {e}")
