'''
TrustRank Reverse Risk Graph Builder (BadRank Topology)
---------------------------------------------------------
Constructs the reverse risk graph by flipping all edge directions
from the forward graph, establishing risk backflow channels with
Markov row-normalization.

The reverse graph enables risk propagation from known malicious seeds
backward through the network, applying "guilt-by-association" penalties.

Output:
  - reverse_transition_matrix.npz   (row-normalized reverse transition matrix)
  - reverse_node_mapping.csv        (node -> index mapping)
'''

import pandas as pd
import numpy as np
import scipy.sparse as sp
from collections import defaultdict
import os
import sys
from tqdm import tqdm

def build_reverse_graph(global_dir):
    print("=" * 60)
    print("  Building Reverse Risk-Seeking Graph (BadRank Topology)")
    print("  (Unknown registrar immunity enabled; all edge directions reversed)")
    print("=" * 60)
    sys.stdout.flush()

    # ---------------------------------------------------------
    # Step 1: Load input files (from global directory)
    # ---------------------------------------------------------
    print("\n  Step 1: Loading global graph base data...")
    try:
        edges_df = pd.read_csv(os.path.join(global_dir, 'graph_edges.csv'))
        registrar_df = pd.read_csv(os.path.join(global_dir, 'domain_registrar_full.csv'))
    except Exception as e:
        print(f"  ERROR: Failed to read data. Ensure {global_dir} contains required CSV files: {e}")
        return

    edges_df['source'] = edges_df['source'].astype(str)
    edges_df['target'] = edges_df['target'].astype(str)
    registrar_df['sld'] = registrar_df['sld'].astype(str)
    registrar_df['registrar'] = registrar_df['registrar'].astype(str)

    # Step 1.5: Cleanse Unknown registrars (prevent risk flooding)
    print("\n  Step 1.5: Cleansing registrar data (removing Unknown/invalid entries)...")
    forbidden_names = {'unknown', 'unknow', 'nan', 'none', '', 'null', '<unknown>'}

    initial_reg_count = len(registrar_df)
    registrar_df = registrar_df[
        ~registrar_df['registrar'].str.strip().str.lower().isin(forbidden_names)
    ].copy()

    filtered_count = initial_reg_count - len(registrar_df)
    print(f"    Removed {filtered_count:,} invalid 'Unknown' records")
    print(f"    Remaining real registrar backing records: {len(registrar_df):,}")

    # ---------------------------------------------------------
    # Step 2: Build complete node set (identical to forward graph)
    # ---------------------------------------------------------
    print("\n  Step 2: Building network-wide objective node set...")
    all_domains = set(edges_df['source']).union(set(edges_df['target'])).union(set(registrar_df['sld']))
    all_registrars = set(registrar_df['registrar'])

    all_nodes = list(all_domains) + list(all_registrars)
    all_nodes = list(set(all_nodes))  # deduplicate

    node_to_idx = {node: idx for idx, node in enumerate(all_nodes)}
    idx_to_node = {idx: node for node, idx in node_to_idx.items()}
    print(f"    Node count K = {len(all_nodes):,}")

    # ---------------------------------------------------------
    # Step 3: Build reverse edge lists (core: direction reversal)
    # ---------------------------------------------------------
    print("\n  Step 3: Building reverse edge lists (Risk Flow)...")

    rev_rows, rev_cols, rev_weights = [], [], []

    # 1. Reverse lateral edges: Target -> Source (risk backflow)
    print("    + Building reverse lateral edges: Target -> Source")
    freq_counter = defaultdict(int)
    for _, row in edges_df.iterrows():
        freq_counter[(row['source'], row['target'])] += 1

    for (source, target), freq in tqdm(freq_counter.items(), desc="      Processing reverse edges"):
        # Row = target, Col = source (risk flows from target back to source)
        rev_rows.append(node_to_idx[target])
        rev_cols.append(node_to_idx[source])
        rev_weights.append(np.log1p(freq))

    # 2. Reverse contribution edges: Registrar -> Domain (originally Domain->Registrar)
    print("    + Building reverse contribution edges: Registrar -> Domain (weight 0.1)")
    for _, row in tqdm(registrar_df.iterrows(), desc="      Processing reverse contribution"):
        rev_rows.append(node_to_idx[row['registrar']])
        rev_cols.append(node_to_idx[row['sld']])
        rev_weights.append(0.1)

    # 3. Reverse feedback edges: Domain -> Registrar (originally Registrar->Domain)
    print("    + Building reverse feedback edges: Domain -> Registrar (weight 1/N)")
    registrar_groups = defaultdict(list)
    for _, row in registrar_df.iterrows():
        registrar_groups[row['registrar']].append(row['sld'])

    for registrar, domains in tqdm(registrar_groups.items(), desc="      Processing reverse feedback"):
        n_domains = len(domains)
        weight = 1.0 / n_domains if n_domains > 0 else 0
        reg_idx = node_to_idx[registrar]
        for domain in domains:
            rev_rows.append(node_to_idx[domain])
            rev_cols.append(reg_idx)
            rev_weights.append(weight)

    # ---------------------------------------------------------
    # Step 4 & 5: Create sparse matrix & row-normalize
    # ---------------------------------------------------------
    print("\n  Step 4: Creating reverse transition matrix and performing row-normalization (Markov property)...")
    n_nodes = len(all_nodes)
    adjacency_matrix = sp.csr_matrix((rev_weights, (rev_rows, rev_cols)), shape=(n_nodes, n_nodes))

    row_sums = np.array(adjacency_matrix.sum(axis=1)).flatten()
    row_sums[row_sums == 0] = 1.0  # avoid dangling nodes
    D = sp.diags(1.0 / row_sums)
    transition_matrix = D.dot(adjacency_matrix)

    print(f"    Row normalization complete, matrix shape: {transition_matrix.shape}")

    # ---------------------------------------------------------
    # Step 6: Persist results to global_dir
    # ---------------------------------------------------------
    print("\n  Step 6: Serializing and saving reverse graph structures...")
    sp.save_npz(os.path.join(global_dir, 'reverse_transition_matrix.npz'), transition_matrix)

    pd.DataFrame({
        'node': list(idx_to_node.values()),
        'idx': list(idx_to_node.keys())
    }).to_csv(os.path.join(global_dir, 'reverse_node_mapping.csv'), index=False)

    print(f"\n  DONE: Network-wide reverse topology graph saved to: {global_dir}")
    print(f"    - Matrix: reverse_transition_matrix.npz")
    print(f"    - Mapping: reverse_node_mapping.csv")

    return transition_matrix, node_to_idx


# ==========================================
# Standalone entry point
# ==========================================
if __name__ == "__main__":
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(SRC_DIR)
    GLOBAL_DIR = os.path.join(PROJECT_ROOT, 'output', 'global')

    if not os.path.exists(GLOBAL_DIR):
        print(f"  Global directory {GLOBAL_DIR} not found. Please ensure prerequisite data is prepared.")
        os.makedirs(GLOBAL_DIR, exist_ok=True)

    try:
        build_reverse_graph(global_dir=GLOBAL_DIR)
    except KeyboardInterrupt:
        print("\n  User interrupted.")
    except Exception as e:
        print(f"\n  ERROR: Fatal error during execution: {e}")
