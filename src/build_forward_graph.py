'''
TrustRank Global Forward Heterogeneous Graph Builder
-----------------------------------------------------
Constructs the forward trust graph with three edge types:
  1. Domain -> Domain   (lateral ecological edges, log-smoothed frequency)
  2. Domain -> Registrar (vertical contribution edges, fixed weight 0.1)
  3. Registrar -> Domain (vertical feedback edges, equal weight 1/N)

Maintains strict objectivity: no malicious seed prior is introduced at this stage.
Cleanses Unknown/invalid registrars to prevent parasitic trust chains.

Output:
  - forward_transition_matrix.npz   (row-normalized Markov transition matrix)
  - forward_node_mapping.csv        (node -> index mapping)
  - forward_graph_stats.csv         (summary statistics)
'''

import pandas as pd
import numpy as np
import scipy.sparse as sp
from collections import defaultdict
import os
import sys
from tqdm import tqdm

def build_forward_graph(global_dir):
    print("=" * 60)
    print("  Building TrustRank Global Forward Objective Trust Graph")
    print("  (Unknown registrar immunity filter enabled; all-node routing preserved)")
    print("=" * 60)
    sys.stdout.flush()

    # ---------------------------------------------------------
    # Step 1: Load input files
    # ---------------------------------------------------------
    print("\n  Step 1: Loading global graph base data...")
    try:
        edges_df = pd.read_csv(os.path.join(global_dir, 'graph_edges.csv'))
        registrar_df = pd.read_csv(os.path.join(global_dir, 'domain_registrar_full.csv'))
    except Exception as e:
        print(f"  ERROR: Failed to read data. Ensure {global_dir} contains required CSV files: {e}")
        return

    print(f"    graph_edges.csv: {len(edges_df):,} ecological edges")
    print(f"    domain_registrar_full.csv: {len(registrar_df):,} registrar mapping records")

    edges_df['source'] = edges_df['source'].astype(str)
    edges_df['target'] = edges_df['target'].astype(str)
    registrar_df['sld'] = registrar_df['sld'].astype(str)
    registrar_df['registrar'] = registrar_df['registrar'].astype(str)

    # ---------------------------------------------------------
    # Step 1.5: Cleanse Unknown registrars (prevent parasitic chains)
    # ---------------------------------------------------------
    print("\n  Step 1.5: Cleansing registrar data (removing invalid/unknown entries)...")
    forbidden_names = {'unknown', 'unknow', 'nan', 'none', '', 'null', '<unknown>'}

    initial_reg_count = len(registrar_df)
    registrar_df = registrar_df[
        ~registrar_df['registrar'].str.strip().str.lower().isin(forbidden_names)
    ].copy()

    filtered_count = initial_reg_count - len(registrar_df)
    print(f"    Removed {filtered_count:,} invalid 'Unknown' records")
    print(f"    Remaining valid registrar backing records: {len(registrar_df):,}")

    # ---------------------------------------------------------
    # Step 2: Build complete node set
    # ---------------------------------------------------------
    print("\n  Step 2: Building network-wide objective node set...")
    all_domains = set(edges_df['source']).union(set(edges_df['target']))
    all_domains = all_domains.union(set(registrar_df['sld']))
    all_registrars = set(registrar_df['registrar'])

    all_nodes = list(all_domains) + list(all_registrars)
    all_nodes = list(set(all_nodes))  # deduplicate

    print(f"    Total domain nodes: {len(all_domains):,}")
    print(f"    Total real registrar nodes: {len(all_registrars):,}")
    print(f"    Full graph node count (K): {len(all_nodes):,}")

    node_to_idx = {node: idx for idx, node in enumerate(all_nodes)}
    idx_to_node = {idx: node for node, idx in node_to_idx.items()}

    # ---------------------------------------------------------
    # Step 3: Build edge lists for all three edge types
    # ---------------------------------------------------------
    print("\n  Step 3: Mapping network edge weights...")

    # 1. Lateral ecological edges: Domain -> Domain (log-smoothed frequency)
    print("    + Mapping Domain -> Domain edges...")
    d2d_rows, d2d_cols, d2d_weights = [], [], []
    freq_counter = defaultdict(int)
    for _, row in edges_df.iterrows():
        freq_counter[(row['source'], row['target'])] += 1

    for (source, target), freq in tqdm(freq_counter.items(), desc="      Processing D2D"):
        d2d_rows.append(node_to_idx[source])
        d2d_cols.append(node_to_idx[target])
        d2d_weights.append(np.log1p(freq))

    # 2. Vertical contribution edges: Domain -> Registrar (fixed weight 0.1)
    print("    + Mapping Domain -> Registrar edges...")
    d2r_rows, d2r_cols, d2r_weights = [], [], []
    for _, row in tqdm(registrar_df.iterrows(), desc="      Processing D2R"):
        d2r_rows.append(node_to_idx[row['sld']])
        d2r_cols.append(node_to_idx[row['registrar']])
        d2r_weights.append(0.1)

    # 3. Vertical feedback edges: Registrar -> Domain (equal weight 1/N)
    print("    + Mapping Registrar -> Domain edges...")
    r2d_rows, r2d_cols, r2d_weights = [], [], []
    registrar_groups = defaultdict(list)
    for _, row in registrar_df.iterrows():
        registrar_groups[row['registrar']].append(row['sld'])

    for registrar, domains in tqdm(registrar_groups.items(), desc="      Processing R2D"):
        weight = 1.0 / len(domains) if domains else 0
        reg_idx = node_to_idx[registrar]
        for domain in domains:
            r2d_rows.append(reg_idx)
            r2d_cols.append(node_to_idx[domain])
            r2d_weights.append(weight)

    # ---------------------------------------------------------
    # Step 4 & 5: Create sparse matrix & Markov row-normalize
    # ---------------------------------------------------------
    print("\n  Step 4 & 5: Creating sparse matrix and performing Markov row-normalization...")
    total_edges = len(d2d_weights) + len(d2r_weights) + len(r2d_weights)
    all_rows = d2d_rows + d2r_rows + r2d_rows
    all_cols = d2d_cols + d2r_cols + r2d_cols
    all_weights = d2d_weights + d2r_weights + r2d_weights

    n_nodes = len(all_nodes)
    adjacency_matrix = sp.csr_matrix((all_weights, (all_rows, all_cols)), shape=(n_nodes, n_nodes))

    # Row normalization: each row sums to 1.0 (Markov property)
    row_sums = np.array(adjacency_matrix.sum(axis=1)).flatten()
    row_sums[row_sums == 0] = 1.0  # Prevent dangling node division by zero
    D = sp.diags(1.0 / row_sums)
    transition_matrix = D.dot(adjacency_matrix)

    print(f"    Matrix normalization complete (theoretical row sum = 1.0)")

    # ---------------------------------------------------------
    # Step 6: Persist all results to global_dir
    # ---------------------------------------------------------
    print("\n  Step 6: Serializing and saving global graph structures...")
    sp.save_npz(os.path.join(global_dir, 'forward_transition_matrix.npz'), transition_matrix)

    pd.DataFrame({
        'node': list(idx_to_node.values()),
        'idx': list(idx_to_node.keys())
    }).to_csv(os.path.join(global_dir, 'forward_node_mapping.csv'), index=False)

    pd.DataFrame([{
        'total_nodes': n_nodes,
        'total_edges': total_edges,
        'domains_count': len(all_domains),
        'registrars_count': len(all_registrars),
        'graph_type': 'objective_forward_cleaned',
        'normalized': True
    }]).to_csv(os.path.join(global_dir, 'forward_graph_stats.csv'), index=False)

    print(f"\n  DONE: Objective network topology graph saved to: {global_dir}")
    return transition_matrix, node_to_idx, idx_to_node


# ==========================================
# Standalone entry point
# ==========================================
if __name__ == "__main__":
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(SRC_DIR)
    GLOBAL_DIR = os.path.join(PROJECT_ROOT, 'output', 'global')

    if not os.path.exists(GLOBAL_DIR):
        print(f"  Global directory {GLOBAL_DIR} not found, creating...")
        os.makedirs(GLOBAL_DIR)

    try:
        build_forward_graph(global_dir=GLOBAL_DIR)
    except KeyboardInterrupt:
        print("\n  User interrupted.")
    except Exception as e:
        print(f"\n  ERROR: Fatal error during execution: {e}")
