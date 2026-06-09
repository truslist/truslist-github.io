'''
TrustRank Prior Vector Engine (prior_v.py)
------------------------------------------
Computes the initial trust prior vector from PDNS query logs.
Core mechanisms:
  - Rolling spatiotemporal aggregation (adaptive 10-minute time slices)
  - IP authority weighting with anti-manipulation logarithmic smoothing
  - Weighted Borda-1000 voting to aggregate IP-domain preferences
  - L1 normalization into a valid PageRank teleportation vector

Output: pagerank_prior_v.csv containing SLD-level prior probabilities
'''

import pandas as pd
import numpy as np
import os
import tldextract
from tqdm import tqdm
import sys
import warnings
warnings.filterwarnings('ignore')

class TrusListPriorEngine:
    def __init__(self, data_dir, output_dir):
        self.data_dir = data_dir
        self.output_dir = output_dir

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # Initialize TLD extractor with local cache for massive speedup
        self.extractor = tldextract.TLDExtract(include_psl_private_domains=True)
        self.sld_cache = {}

    def _get_sld(self, fqdn):
        '''Fast second-level domain (SLD) extraction with cache.'''
        if pd.isna(fqdn):
            return None
        if fqdn not in self.sld_cache:
            ext = self.extractor(fqdn)
            self.sld_cache[fqdn] = f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain
        return self.sld_cache[fqdn]

    @staticmethod
    def _build_time_slice(ts_series):
        '''Build 10-minute granularity time slices (SecRank-style).'''
        ts_str = ts_series.astype(str)
        if ts_str.str.len().min() >= 16:
            date = ts_str.str.slice(0, 10)
            hour = ts_str.str.slice(11, 13)
            minute = pd.to_numeric(ts_str.str.slice(14, 16), errors="coerce").fillna(0).astype(int)
            minute_bucket = (minute // 10) * 10
            minute_bucket_str = minute_bucket.astype(str).str.zfill(2)
            return date + "_" + hour + minute_bucket_str
        dt = pd.to_datetime(ts_series, errors="coerce")
        minute_bucket = (dt.dt.minute // 10) * 10
        return dt.dt.strftime("%Y-%m-%d_") + dt.dt.hour.astype(str).str.zfill(2) + minute_bucket.astype(str).str.zfill(2)

    def extract_spatiotemporal_features(self, specific_file=None):
        '''
        Core Steps 1 & 2: Extract spatiotemporal features from PDNS logs.

        Returns:
            final_pairs: DataFrame of aggregated IP-domain pairs with pref_ij scores
            total_days:  Number of input files (days)
        '''
        print("=" * 60)
        print("  Stage 1: Rolling Spatiotemporal Feature Extraction")
        print("=" * 60)
        sys.stdout.flush()

        if specific_file:
            files = [specific_file]
        else:
            files = sorted([f for f in os.listdir(self.data_dir) if f.endswith('.csv')])

        if not files:
            raise FileNotFoundError(f"ERROR: No CSV data found under {self.data_dir}")

        global_agg = pd.DataFrame()
        total_rows_processed = 0
        total_days = len(files)

        for file in files:
            file_path = os.path.join(self.data_dir, file)
            print(f"\n  Processing file: {file} ...")

            # Chunked reading: 1M rows per chunk to avoid OOM
            chunk_iterator = pd.read_csv(
                file_path,
                chunksize=1000000,
                sep=',',
                on_bad_lines='skip',
                engine='c'
            )

            for chunk in tqdm(chunk_iterator, desc="    Processing chunks"):
                total_rows_processed += len(chunk)

                # 1. Column name compatibility & basic cleaning
                domain_col = 'dns.rrname' if 'dns.rrname' in chunk.columns else ('domain' if 'domain' in chunk.columns else 'fqdn')
                ip_col = 'src_ip' if 'src_ip' in chunk.columns else ('client_ip' if 'client_ip' in chunk.columns else 'ip')
                ts_col = 'timestamp' if 'timestamp' in chunk.columns else ('time' if 'time' in chunk.columns else 'ts')

                if 'rtype' in chunk.columns:
                    chunk = chunk[chunk['rtype'] == 1]

                if 'request_cnt' in chunk.columns:
                    chunk = chunk.rename(columns={'request_cnt': 'req'})
                elif 'req' in chunk.columns:
                    chunk = chunk.rename(columns={'req': 'req'})
                elif 'count' in chunk.columns:
                    chunk = chunk.rename(columns={'count': 'req'})
                else:
                    chunk['req'] = 1

                chunk = chunk.rename(columns={domain_col: 'domain', ip_col: 'src_ip', ts_col: 'timestamp'})
                chunk = chunk.dropna(subset=['domain', 'src_ip', 'timestamp']).reset_index(drop=True)

                # Filter out reverse DNS (.arpa) and numeric domains
                chunk = chunk[~chunk['domain'].astype(str).str.contains('.arpa', na=False)]
                chunk = chunk[~chunk['domain'].astype(str).str.match(r'^\d+\.\d+\.\d+\.\d+$', na=False)]
                if chunk.empty:
                    continue

                # 2. SLD aggregation with cache
                chunk['sld'] = chunk['domain'].map(self._get_sld)
                chunk = chunk.dropna(subset=['sld'])
                if chunk.empty:
                    continue

                # 3. Build 10-min time slices
                chunk['time_slice'] = self._build_time_slice(chunk['timestamp'])

                # 4. Aggregate per (IP, SLD, time_slice)
                agg_chunk = chunk.groupby(['src_ip', 'sld', 'time_slice'], as_index=False).agg(
                    vol=('req', 'sum')
                )

                # Log-smooth volume: ln(vol+1) for anti-manipulation
                agg_chunk['log_vol'] = np.log1p(agg_chunk['vol'])

                global_agg = pd.concat([global_agg, agg_chunk], ignore_index=True)

        print(f"\n  Total rows processed: {total_rows_processed:,}")

        if global_agg.empty:
            raise RuntimeError("No valid data extracted. Check CSV fields and content.")

        # --- Step 5: Build IP-domain preference matrix ---
        print("  Building IP-domain preference matrix...")

        # Aggregate to IP-SLD level: total log-volume, total active time slices
        final_pairs = global_agg.groupby(['src_ip', 'sld'], as_index=False).agg(
            total_vol=('log_vol', 'sum'),
            total_ad=('time_slice', 'nunique')
        )

        return final_pairs, total_days

    def calculate_prior_vector(self, final_pairs, total_days):
        '''
        Core Steps 3-5: Compute the prior probability vector via weighted voting.

        IP weight W_IP: geometric mean of normalized log(volume) and log(diversity).
        Domain preference Pref_ij: geometric mean of active-duration ratio and log-count ratio.
        Uses Weighted Borda-1000 voting to aggregate IP preferences per domain.
        Output is L1-normalized to sum=1.0 for use as a PageRank teleportation vector.

        Returns:
            DataFrame with columns: sld, raw_score, unique_ips, prior_probability
        '''
        print("\n" + "=" * 60)
        print("  Stage 1b: Computing Prior Vector (Weighted Voting)")
        print("=" * 60)
        sys.stdout.flush()

        # ---------------------------------------------------------
        # Step 1: Compute IP authority weight W_IP
        #   W_IP = sqrt( norm(ln(total_vol+1)) * norm(ln(num_unique_SLDs+1)) )
        # Uses log-smoothing to suppress single-IP traffic inflation attacks.
        # ---------------------------------------------------------
        print("  1. Computing IP authority weights (W_IP)...")
        ip_vol = final_pairs.groupby('src_ip')['total_vol'].sum().reset_index(name='ip_total_vol')
        ip_div = final_pairs.groupby('src_ip')['sld'].nunique().reset_index(name='ip_sld_count')
        ip_stats = pd.merge(ip_vol, ip_div, on='src_ip')

        max_vol = ip_stats['ip_total_vol'].max()
        max_vol_log = np.log1p(max_vol) if max_vol > 0 else 1.0
        max_div_log = np.log1p(ip_stats['ip_sld_count'].max()) if ip_stats['ip_sld_count'].max() > 0 else 1.0

        ip_stats['delta_nor'] = np.log1p(ip_stats['ip_total_vol']) / max_vol_log
        ip_stats['theta_nor'] = np.log1p(ip_stats['ip_sld_count']) / max_div_log
        ip_stats['w_ip'] = np.sqrt(ip_stats['delta_nor'] * ip_stats['theta_nor'])

        df = pd.merge(final_pairs, ip_stats[['src_ip', 'w_ip']], on='src_ip')

        # ---------------------------------------------------------
        # Step 2: Compute spatiotemporal composite preference Pref_ij
        #   Pref_ij = sqrt( norm(active_duration) * norm(log_count) )
        # ---------------------------------------------------------
        print("  2. Computing spatiotemporal composite preference (Pref_ij)...")
        max_possible_ad = 144.0 * total_days  # 144 = 24h * 6 slices/hour
        max_cnt = df['total_vol'].max()
        max_cnt_log = np.log1p(max_cnt) if max_cnt > 0 else 1.0
        df['nor_ad'] = df['total_ad'] / max_possible_ad
        df['nor_cnt'] = np.log1p(df['total_vol']) / max_cnt_log
        df['pref_ij'] = np.sqrt(df['nor_ad'] * df['nor_cnt'])

        # ---------------------------------------------------------
        # Step 3: Weighted Borda-1000 Voting (SecRank core)
        # ---------------------------------------------------------
        print("  3. Executing Weighted Borda-1000 Voting...")
        pref_df = df.sort_values(by=['src_ip', 'pref_ij', 'sld'], ascending=[True, False, True])
        pref_df['index'] = pref_df.groupby('src_ip').cumcount() + 1
        pref_df['borda_1000'] = np.maximum(1001 - pref_df['index'], 0) * pref_df['w_ip']

        # ---------------------------------------------------------
        # Step 4: Physical aggregation of votes
        # ---------------------------------------------------------
        print("  4. Aggregating network-wide consensus, generating raw scores...")
        domain_scores = pref_df.groupby('sld', as_index=False).agg(
            raw_score=('borda_1000', 'sum'),
            unique_ips=('src_ip', 'nunique')
        )

        # ---------------------------------------------------------
        # Step 5: L1 normalization (as PageRank initial vector)
        # ---------------------------------------------------------
        print("  5. Performing L1 probability normalization (PageRank initial vector)...")
        total_energy = domain_scores['raw_score'].sum()
        if total_energy <= 0:
            domain_scores['prior_probability'] = 1.0 / max(len(domain_scores), 1)
        else:
            domain_scores['prior_probability'] = domain_scores['raw_score'] / total_energy
        domain_scores = domain_scores.sort_values(by='prior_probability', ascending=False)

        # ---------------------------------------------------------
        # Step 6: Persist results
        # ---------------------------------------------------------
        out_file = os.path.join(self.output_dir, 'pagerank_prior_v.csv')
        domain_scores.to_csv(out_file, index=False)

        print("\n" + "=" * 60)
        print(f"  DONE: Prior vector saved to: {out_file}")
        print(f"    - Active domains analyzed: {len(domain_scores):,}")
        print(f"    - Energy sum verification: {domain_scores['prior_probability'].sum():.6f} (must be exactly 1.0)")

        return domain_scores


# ==========================================
# Standalone debug entry point
# ==========================================
if __name__ == "__main__":
    try:
        SRC_DIR = os.path.dirname(os.path.abspath(__file__))
        PROJECT_ROOT = os.path.dirname(SRC_DIR)

        TEST_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'pdns', 'test_day')
        TEST_OUT_DIR = os.path.join(PROJECT_ROOT, 'output', 'daily', 'test_day')

        print(f"  [Standalone Debug Mode] Prior Engine")
        print(f"    Data directory: {TEST_DATA_DIR}")
        print(f"    Output directory: {TEST_OUT_DIR}")

        if not os.path.exists(TEST_DATA_DIR):
            os.makedirs(TEST_DATA_DIR, exist_ok=True)
            print(f"\n  ERROR: Directory {TEST_DATA_DIR} is empty.")
            print(f"  Please place test PDNS CSV files there and re-run.")
            sys.exit(0)

        engine = TrusListPriorEngine(data_dir=TEST_DATA_DIR, output_dir=TEST_OUT_DIR)
        final_pairs_df, days_count = engine.extract_spatiotemporal_features()
        final_v_vector = engine.calculate_prior_vector(final_pairs_df, days_count)

    except KeyboardInterrupt:
        print("\n  User interrupted.")
    except Exception as e:
        print(f"\n  ERROR: Fatal error during execution: {e}")
