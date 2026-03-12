# -*- coding: utf-8 -*-
"""
Registrar-Based Domain Importance Scoring
Combines user behavior scores, malicious domain statistics, and registrar
compliance to compute the final registrar score Sr_Score for each domain.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RegistrarScorer:
    def __init__(self, config: Dict):
        """
        Args:
            config: Configuration dictionary.
                - bayesian_smoothing_k: Bayesian smoothing parameter k.
                - compliance_scores: Score per compliance tier {'high':1.0,'medium':0.7,'low':0.3}.
        """
        self.k = config.get('bayesian_smoothing_k', 10)
        self.compliance_scores = config.get('compliance_scores', {'high': 1.0, 'medium': 0.7, 'low': 0.3})
        logger.info(f"RegistrarScorer initialized. Bayesian smoothing k={self.k}")

    def run(self, registrar_df: pd.DataFrame, user_scores: pd.DataFrame,
            phish_data: pd.DataFrame, registrar_accredited: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the registrar score Sr_Score for each domain.

        Args:
            registrar_df: DataFrame mapping domain -> registrar.
            user_scores: DataFrame mapping domain -> Sd_Score.
            phish_data: DataFrame of malicious domains with columns: domain, timestamp, registrar.
            registrar_accredited: DataFrame with registrar country information.

        Returns:
            DataFrame with columns: Domain, Sr_Score.
        """
        logger.info("Starting registrar score computation...")

        # ---- Step 0: Merge domain list with user behavior scores ----
        df = pd.merge(registrar_df, user_scores, left_on='domain', right_on='Domain', how='left')
        df['Sd_Score'] = df['Sd_Score'].fillna(0)

        # ---- Step 1: Compute S_P (Scale / Popularity) ----
        df['Sd_log'] = np.log1p(df['Sd_Score'])
        sp_df = df.groupby('registrar')['Sd_log'].sum().reset_index(name='P_R')
        sp_min = sp_df['P_R'].min()
        sp_max = sp_df['P_R'].max()
        sp_df['S_P'] = (sp_df['P_R'] - sp_min) / (sp_max - sp_min + 1e-12)
        logger.info("S_P computation complete.")

        # ---- Step 2: Compute S_S (Smoothed Malicious Rate) ----
        phish_data['timestamp'] = pd.to_datetime(phish_data['timestamp'], errors='coerce')
        latest_ts = phish_data['timestamp'].max()
        recent_phish = phish_data[phish_data['timestamp'] >= (latest_ts - pd.Timedelta(days=7))]

        phish_counts = recent_phish.groupby('registrar')['domain'].nunique().reset_index(name='M_R')
        registrar_counts = df.groupby('registrar')['domain'].nunique().reset_index(name='N_R')
        ss_df = pd.merge(registrar_counts, phish_counts, on='registrar', how='left')
        ss_df['M_R'] = ss_df['M_R'].fillna(0)

        r0 = ss_df['M_R'].sum() / ss_df['N_R'].sum() if ss_df['N_R'].sum() > 0 else 0
        ss_df['r_hat'] = (ss_df['M_R'] + self.k * r0) / (ss_df['N_R'] + self.k)
        ss_df['S_S'] = 1 - ss_df['r_hat']
        logger.info("S_S computation complete.")

        # ---- Step 3: Compute S_C (Compliance Score) ----
        acc_df = registrar_accredited[['Registrar Name', 'Country/Territory']].rename(
            columns={'Registrar Name': 'registrar', 'Country/Territory': 'country'})
        sc_df = pd.merge(registrar_counts[['registrar']], acc_df, on='registrar', how='left')

        def compliance_score(country):
            if pd.isna(country):
                return self.compliance_scores.get('medium', 0.7)
            return self.compliance_scores.get('high', 1.0) if country.lower() == 'china' else self.compliance_scores.get('medium', 0.7)

        sc_df['S_C'] = sc_df['country'].apply(compliance_score)
        logger.info("S_C computation complete.")

        # ---- Step 4: Combine the three scores ----
        combined_df = pd.merge(sp_df[['registrar', 'S_P']], ss_df[['registrar', 'S_S']], on='registrar')
        combined_df = pd.merge(combined_df, sc_df[['registrar', 'S_C']], on='registrar')

        combined_df['Sr_Score'] = (combined_df['S_P'] * combined_df['S_S'] * combined_df['S_C']) ** (1 / 3)

        # ---- Step 5: Map scores back to individual domains ----
        result_df = pd.merge(df[['domain', 'registrar']], combined_df[['registrar', 'Sr_Score']], on='registrar',
                             how='left')
        result_df = result_df[['domain', 'Sr_Score']].rename(columns={'domain': 'Domain'})

        # ---- Step 6: Align with user_scores and fill missing values ----
        final_df = pd.merge(user_scores[['Domain']], result_df, on='Domain', how='left')
        final_df['Sr_Score'] = final_df['Sr_Score'].fillna(0)

        logger.info("Registrar score computation complete. Output aligned with user_scores.")
        return final_df


# =================== Usage example ===================
if __name__ == "__main__":
    REGISTRAR_CONFIG = {
        'bayesian_smoothing_k': 10,
        'compliance_scores': {'high': 1.0, 'medium': 0.7, 'low': 0.3}
    }
    scorer = RegistrarScorer(REGISTRAR_CONFIG)

    # Example data loading:
    # registrar_df = pd.read_csv('registrar_df.csv')
    # user_scores = pd.read_csv('user_scores.csv')
    # phish_data = pd.read_csv('phish_data.csv')
    # registrar_accredited = pd.read_csv('registrar_accredited.csv')

    # result = scorer.run(registrar_df, user_scores, phish_data, registrar_accredited)
    # print(result.head())
