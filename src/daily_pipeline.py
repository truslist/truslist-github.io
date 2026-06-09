'''
TrustRank Daily Automated Pipeline

Orchestrates the four-stage TrustRank domain ranking pipeline:
  1. Prior Vector   - Spatiotemporal feature extraction & IP weighting (prior_v.py)
  2. TrustRank      - Forward trust propagation on heterogeneous graph (pageRank_forward.py)
  3. BadRank        - Reverse risk propagation with dual-Bayesian armor (pageRank_backward.py)
  4. TOPSIS Fusion  - Asymmetric normalization & geometric ranking (topist.py)

Input:  PDNS query logs (CSV) under data/pdns/
Output: FINAL_TRUSLIST_RANKING.csv under output/daily/<task>/
'''

import os
import time
import sys
import traceback

from prior_v import TrusListPriorEngine
from pageRank_forward import run_trust_walk
from pageRank_backward import run_bad_walk
from topist import run_truslist_fusion

def run_daily_pipeline(only_tasks=None):
    print("\n" + "=" * 70)
    print("  Starting TrustRank Daily Graph Signal Injection Pipeline")
    print("=" * 70)
    sys.stdout.flush()

    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(SRC_DIR)
    BASE_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'pdns')
    GLOBAL_DIR = os.path.join(PROJECT_ROOT, 'output', 'global')
    BASE_DAILY_OUT = os.path.join(PROJECT_ROOT, 'output', 'daily')

    # 1. Check global files exist
    required_global_files = ['forward_transition_matrix.npz', 'reverse_transition_matrix.npz', 'malicious_domains.txt']
    for f in required_global_files:
        if not os.path.exists(os.path.join(GLOBAL_DIR, f)):
            print(f"\n  ERROR: Missing global file: {f}")
            return

    # 2. Identify tasks: folder mode vs. file mode
    daily_folders = sorted([d for d in os.listdir(BASE_DATA_DIR) if os.path.isdir(os.path.join(BASE_DATA_DIR, d))])

    tasks = []

    if daily_folders:
        print(f"  Mode A: Detected {len(daily_folders)} date folder(s).")
        for folder in daily_folders:
            tasks.append((folder, False, None))
    else:
        csv_files = sorted([f for f in os.listdir(BASE_DATA_DIR) if f.endswith('.csv')])
        if csv_files:
            print(f"  Mode B: Detected {len(csv_files)} standalone CSV file(s).")
            for f in csv_files:
                task_name = f.replace('.csv', '')
                tasks.append((task_name, True, f))
        else:
            print("  WARNING: No processable data found.")
            return

    # 3. Optional task filtering
    if only_tasks:
        only_set = set(only_tasks)
        tasks = [t for t in tasks if t[0] in only_set]

    # 4. Core loop
    for task_name, is_single_file, filename in tasks:
        print("\n" + "*" * 70)
        print(f"  [Processing batch] --> {task_name}")
        print("*" * 70)

        daily_output_dir = os.path.join(BASE_DAILY_OUT, task_name)
        os.makedirs(daily_output_dir, exist_ok=True)

        start_time = time.time()

        try:
            # --- Stage 1: Prior Vector ---
            engine = TrusListPriorEngine(
                data_dir=BASE_DATA_DIR if is_single_file else os.path.join(BASE_DATA_DIR, task_name),
                output_dir=daily_output_dir
            )
            final_pairs, days_count = engine.extract_spatiotemporal_features(
                specific_file=filename if is_single_file else None
            )
            engine.calculate_prior_vector(final_pairs, days_count)

            # --- Stage 2: TrustRank (forward) ---
            run_trust_walk(global_dir=GLOBAL_DIR, daily_dir=daily_output_dir)

            # --- Stage 3: BadRank (reverse) ---
            run_bad_walk(global_dir=GLOBAL_DIR, daily_dir=daily_output_dir)

            # --- Stage 4: TOPSIS Fusion ---
            run_truslist_fusion(global_dir=GLOBAL_DIR, daily_dir=daily_output_dir)

            print(f"\n  DONE: Task {task_name} completed in {time.time() - start_time:.2f}s")

        except Exception:
            traceback.print_exc()
            continue

    print("\n" + "=" * 70)
    print(f"  Pipeline finished! Results for {len(tasks)} task(s) under: {BASE_DAILY_OUT}")
    print("=" * 70)

if __name__ == "__main__":
    run_daily_pipeline()
