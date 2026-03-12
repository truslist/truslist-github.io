
"""
User behavior scoring module.
Implements the SecRank-inspired method from the paper to compute
domain popularity scores based on passive DNS user behavior.
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
import tldextract

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserBehaviorScorer:
    """
    User behavior scorer.
    Implements the user behavior scoring module described in the paper, including:
    1. IP-domain preference modeling
    2. IP weight computation
    3. Global scoring via improved Borda count
    """

    def __init__(self, config: dict):
        """
        Initialize the user behavior scorer.

        Args:
            config: Configuration dictionary containing various parameters.
        """
        self.config = config
        self.time_slot_minutes = config.get('time_slot_minutes', 10)
        self.slots_per_day = config.get('slots_per_day', 144)
        self.top_n_domains = config.get('top_n_domains', 100)
        self.log_smoothing_base = config.get('log_smoothing_base', 1)

        logger.info(f"UserBehaviorScorer initialized: time_slot={self.time_slot_minutes}min, "
                    f"slots_per_day={self.slots_per_day}, Top-N={self.top_n_domains}")

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess raw input data.

        Args:
            df: Raw DataFrame containing timestamp, src_ip, domain columns.

        Returns:
            Preprocessed DataFrame.
        """
        logger.info("Starting data preprocessing...")

        processed_df = df.copy()
        required_columns = ['timestamp', 'src_ip']
        missing_columns = [col for col in required_columns if col not in processed_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        domain_column = None
        if 'domain' in processed_df.columns:
            domain_column = 'domain'
        elif 'dns.rrname' in processed_df.columns:
            domain_column = 'dns.rrname'
            processed_df = processed_df.rename(columns={'dns.rrname': 'domain'})
            logger.info("Renamed dns.rrname column to domain.")
        elif 'SLD' in processed_df.columns:
            domain_column = 'SLD'
            processed_df['domain'] = processed_df['SLD']
            logger.info("Created domain column from SLD.")
        else:
            raise ValueError("Missing domain column; expected one of: domain, dns.rrname, SLD.")

        processed_df['timestamp'] = pd.to_datetime(processed_df['timestamp'], errors='coerce')
        invalid_count = processed_df['timestamp'].isna().sum()
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} records with invalid timestamps; filtered out.")
            processed_df = processed_df[processed_df['timestamp'].notna()]

        processed_df['date'] = processed_df['timestamp'].dt.date
        processed_df['SLD'] = processed_df['domain'].apply(self._extract_sld)
        processed_df = processed_df[processed_df['SLD'].notna()]

        logger.info(f"Preprocessing complete. Valid records: {len(processed_df)}")
        logger.info(f"Columns: {processed_df.columns.tolist()}")
        return processed_df

    def _extract_sld(self, domain: str) -> Optional[str]:
        """
        Extract the second-level domain (SLD) using the tldextract package.
        """
        if pd.isna(domain) or not isinstance(domain, str):
            return None
        try:
            extracted = tldextract.extract(domain)
            if not extracted.domain or not extracted.suffix:
                return None
            sld = f"{extracted.domain}.{extracted.suffix}"
            if len(sld) < 3 or sld.startswith('.') or sld.endswith('.'):
                return None
            return sld
        except Exception as e:
            logger.warning(f"SLD extraction failed for domain: {domain}, error: {str(e)}")
            return None

    def compute_ip_domain_preferences(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute IP-domain preference scores.
        """
        logger.info("Computing IP-domain preference scores...")
        all_preferences = []
        for date in tqdm(df['date'].unique(), desc="Computing daily preference scores"):
            date_df = df[df['date'] == date]
            query_counts = date_df.groupby(['src_ip', 'SLD']).size().reset_index(name='query_count')
            access_persistence = self._calculate_access_persistence(date_df)
            merged = pd.merge(query_counts, access_persistence, on=['src_ip', 'SLD'], how='outer').fillna(0)
            merged['gamma_smooth'] = np.log(1 + merged['query_count'])
            merged['alpha_smooth'] = np.log(1 + merged['access_slots'])   # Paper: minmax(log(1+α))
            merged = self._normalize_by_ip(merged, 'gamma_smooth', 'gamma_norm')
            merged = self._normalize_by_ip(merged, 'alpha_smooth', 'alpha_norm')
            merged['preference_score'] = np.sqrt(merged['gamma_norm'] * merged['alpha_norm'])
            merged['date'] = date
            all_preferences.append(merged)
        preferences_df = pd.concat(all_preferences, ignore_index=True)
        logger.info(f"Preference score computation complete. Total records: {len(preferences_df)}")
        return preferences_df

    def _calculate_access_persistence(self, date_df: pd.DataFrame) -> pd.DataFrame:
        date_df = date_df.copy()
        date_df['time_slot'] = date_df['timestamp'].dt.hour * 6 + date_df['timestamp'].dt.minute // 10
        persistence = date_df.groupby(['src_ip', 'SLD'])['time_slot'].nunique().reset_index(name='access_slots')
        return persistence

    def _normalize_by_ip(self, df: pd.DataFrame, source_col: str, target_col: str) -> pd.DataFrame:
        df[target_col] = df.groupby('src_ip')[source_col].transform(
            lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() > x.min() else 0
        )
        return df

    def compute_ip_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Computing IP weights (with EWMA KL-divergence penalty)...")

        alpha_ewma = self.config.get('ewma_alpha', 0.1)   # Paper: α=0.1
        beta_penalty = self.config.get('beta_penalty', 1.0)  # Paper: β=1.0

        # Sort dates to ensure correct EWMA temporal ordering
        sorted_dates = sorted(df['date'].unique())

        # Build global reference distribution Q_ref: query share per SLD across all records
        total_counts = df.groupby('SLD').size()
        q_ref = (total_counts / total_counts.sum()).to_dict()

        # EWMA state: {src_ip: bar_delta}
        ewma_state: dict = {}

        all_weights = []
        for date in tqdm(sorted_dates, desc="Computing daily IP weights"):
            date_df = df[df['date'] == date]

            # W_i^0: base weight (geometric mean)
            domain_diversity = date_df.groupby('src_ip')['SLD'].nunique().reset_index(name='unique_slds')
            total_queries = date_df.groupby('src_ip').size().reset_index(name='total_queries')
            merged = pd.merge(domain_diversity, total_queries, on='src_ip')
            merged['diversity_smooth'] = np.log(1 + merged['unique_slds'])
            merged['queries_smooth'] = np.log(1 + merged['total_queries'])
            merged = self._normalize_by_date(merged, 'diversity_smooth', 'diversity_norm')
            merged = self._normalize_by_date(merged, 'queries_smooth', 'queries_norm')
            merged['W0'] = np.sqrt(merged['diversity_norm'] * merged['queries_norm'])

            # KL divergence: per-IP query distribution Q_i vs Q_ref
            ip_kl = {}
            for src_ip, ip_df in date_df.groupby('src_ip'):
                q_i_counts = ip_df.groupby('SLD').size()
                q_i = (q_i_counts / q_i_counts.sum()).to_dict()
                kl = 0.0
                for domain, p in q_i.items():
                    q = q_ref.get(domain, 1e-10)
                    kl += p * np.log(p / q)
                ip_kl[src_ip] = max(kl, 0.0)  # numerical stability

            # Δ_init = 75th percentile (used as prior for first-seen IPs)
            kl_values = np.array(list(ip_kl.values()))
            delta_init = float(np.percentile(kl_values, 75)) if len(kl_values) > 0 else 0.0

            # EWMA update: bar_Δ_i^(t) = α·Δ_i^KL,(t) + (1-α)·bar_Δ_i^(t-1)
            new_ewma_state = {}
            for src_ip, kl_t in ip_kl.items():
                prev = ewma_state.get(src_ip, delta_init)  # unseen IPs use delta_init
                new_ewma_state[src_ip] = alpha_ewma * kl_t + (1 - alpha_ewma) * prev
            ewma_state.update(new_ewma_state)

            # W_i = W_i^0 · exp(-β · bar_Δ_i^(t))
            merged['bar_delta'] = merged['src_ip'].map(lambda ip: ewma_state.get(ip, delta_init))
            merged['weight'] = merged['W0'] * np.exp(-beta_penalty * merged['bar_delta'])
            merged['date'] = date
            all_weights.append(merged)

        weights_df = pd.concat(all_weights, ignore_index=True)
        logger.info(f"IP weight computation complete. Total records: {len(weights_df)}")
        return weights_df

    def _normalize_by_date(self, df: pd.DataFrame, source_col: str, target_col: str) -> pd.DataFrame:
        df[target_col] = (df[source_col] - df[source_col].min()) / (df[source_col].max() - df[source_col].min())
        return df

    def compute_global_scores(self, preferences_df: pd.DataFrame, weights_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Computing global scores...")
        all_scores = []
        for date in tqdm(preferences_df['date'].unique(), desc="Computing daily global scores"):
            date_prefs = preferences_df[preferences_df['date'] == date]
            date_weights = weights_df[weights_df['date'] == date]
            ip_scores = []
            for _, ip_row in date_weights.iterrows():
                src_ip = ip_row['src_ip']
                weight = ip_row['weight']
                ip_prefs = date_prefs[date_prefs['src_ip'] == src_ip].sort_values('preference_score', ascending=False)
                for rank, (_, domain_row) in enumerate(ip_prefs.head(self.top_n_domains).iterrows()):
                    ip_scores.append({
                        'date': date,
                        'src_ip': src_ip,
                        'SLD': domain_row['SLD'],
                        'borda_score': self.top_n_domains - rank,
                        'weight': weight,
                        'weighted_score': (self.top_n_domains - rank) * weight
                    })
            ip_scores_df = pd.DataFrame(ip_scores)
            global_scores = ip_scores_df.groupby('SLD')['weighted_score'].sum().reset_index(name='global_score')
            global_scores['date'] = date
            all_scores.append(global_scores)
        scores_df = pd.concat(all_scores, ignore_index=True)
        scores_df['rank'] = scores_df.groupby('date')['global_score'].rank(ascending=False, method='min')
        logger.info(f"Global score computation complete. Total records: {len(scores_df)}")
        return scores_df.sort_values(['date', 'rank'])

    def save_results(self, df, path, fmt='csv'):
        """
        Save results sorted by Sd_Score in descending order.

        Args:
            df: Output DataFrame from run(), containing Domain and Sd_Score columns.
            path: Output file path.
            fmt: 'csv' or 'xlsx'.
        """
        save_df = df.sort_values(by="Sd_Score", ascending=False).reset_index(drop=True)

        if fmt == 'csv':
            save_df.to_csv(path, index=False, encoding='utf-8-sig')
        elif fmt == 'xlsx':
            save_df.to_excel(path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {fmt}")

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the full user behavior scoring pipeline and return the final
        simplified DataFrame with normalized Sd_Score.
        """
        start_time = time.time()
        try:
            logger.info("Starting user behavior score computation...")

            processed_df = self.preprocess_data(df)
            preferences_df = self.compute_ip_domain_preferences(processed_df)
            self.preferences_df = preferences_df
            weights_df = self.compute_ip_weights(processed_df)
            self.weights_df = weights_df
            final_scores = self.compute_global_scores(preferences_df, weights_df)

            # Build final output keeping only Domain and Sd_Score
            result_df = final_scores[['SLD', 'global_score']].rename(
                columns={'SLD': 'Domain', 'global_score': 'Sd_Score'}
            )

            # Normalize Sd_Score to [0, 1]
            min_score = result_df['Sd_Score'].min()
            max_score = result_df['Sd_Score'].max()
            if max_score > min_score:
                result_df['Sd_Score'] = (result_df['Sd_Score'] - min_score) / (max_score - min_score)
            else:
                result_df['Sd_Score'] = 0.0  # When all scores are identical, normalize to 0

            elapsed_time = time.time() - start_time
            logger.info(f"User behavior score computation complete. Elapsed: {elapsed_time:.2f}s")
            return result_df

        except Exception as e:
            logger.error(f"User behavior score computation failed: {str(e)}")
            raise


# 使用示例
if __name__ == "__main__":
    config = {
        'time_slot_minutes': 10,
        'slots_per_day': 144,
        'top_n_domains': 100,
        'log_smoothing_base': 1
    }

    scorer = UserBehaviorScorer(config)
    # df = pd.read_csv('your_data.csv')  # 加载实际数据
    # results = scorer.run(df)
    # print(results.head())
