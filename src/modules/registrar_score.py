# -*- coding: utf-8 -*-
"""
Registrar-Based Domain Importance Scoring
根据论文算法，结合用户行为分数、恶意域名统计和注册商合规性
计算每个域名的最终注册商评分 Sr_Score
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
            config: 配置字典
                - bayesian_smoothing_k: 贝叶斯平滑参数 k
                - compliance_scores: 各合规等级对应的分数 {'high':1.0,'medium':0.7,'low':0.3}
        """
        self.k = config.get('bayesian_smoothing_k', 10)
        self.compliance_scores = config.get('compliance_scores', {'high': 1.0, 'medium': 0.7, 'low': 0.3})
        logger.info(f"RegistrarScorer 初始化完成，贝叶斯平滑k={self.k}")

    def run(self, registrar_df: pd.DataFrame, user_scores: pd.DataFrame,
            phish_data: pd.DataFrame, registrar_accredited: pd.DataFrame) -> pd.DataFrame:
        """
        计算每个域名的注册商评分 Sr_Score

        Args:
            registrar_df: DataFrame，domain -> registrar
            user_scores: DataFrame，domain -> Sd_Score
            phish_data: DataFrame，malicious domains，包含 domain, timestamp, registrar
            registrar_accredited: DataFrame，包含注册商归属国家

        Returns:
            DataFrame: Domain, Sr_Score
        """
        logger.info("开始计算注册商评分...")

        # ---- Step 0: 合并域名与用户行为得分 ----
        df = pd.merge(registrar_df, user_scores, left_on='domain', right_on='Domain', how='left')
        df['Sd_Score'] = df['Sd_Score'].fillna(0)

        # ---- Step 1: 计算 S_P（Scale/Popularity） ----
        df['Sd_log'] = np.log1p(df['Sd_Score'])
        sp_df = df.groupby('registrar')['Sd_log'].sum().reset_index(name='P_R')
        sp_min = sp_df['P_R'].min()
        sp_max = sp_df['P_R'].max()
        sp_df['S_P'] = (sp_df['P_R'] - sp_min) / (sp_max - sp_min + 1e-12)
        logger.info("S_P 计算完成")

        # ---- Step 2: 计算 S_S（Smoothed Malicious Rate） ----
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
        logger.info("S_S 计算完成")

        # ---- Step 3: 计算 S_C（Compliance Score） ----
        acc_df = registrar_accredited[['Registrar Name', 'Country/Territory']].rename(
            columns={'Registrar Name': 'registrar', 'Country/Territory': 'country'})
        sc_df = pd.merge(registrar_counts[['registrar']], acc_df, on='registrar', how='left')

        def compliance_score(country):
            if pd.isna(country):
                return self.compliance_scores.get('medium', 0.7)
            return self.compliance_scores.get('high', 1.0) if country.lower() == 'china' else self.compliance_scores.get('medium', 0.7)

        sc_df['S_C'] = sc_df['country'].apply(compliance_score)
        logger.info("S_C 计算完成")

        # ---- Step 4: 合并三个分数 ----
        combined_df = pd.merge(sp_df[['registrar', 'S_P']], ss_df[['registrar', 'S_S']], on='registrar')
        combined_df = pd.merge(combined_df, sc_df[['registrar', 'S_C']], on='registrar')

        combined_df['Sr_Score'] = (combined_df['S_P'] * combined_df['S_S'] * combined_df['S_C']) ** (1 / 3)

        # ---- Step 5: 回填到每个域名 ----
        result_df = pd.merge(df[['domain', 'registrar']], combined_df[['registrar', 'Sr_Score']], on='registrar',
                             how='left')
        result_df = result_df[['domain', 'Sr_Score']].rename(columns={'domain': 'Domain'})

        # ---- Step 6: 与 user_scores 对齐，并填充默认值 ----
        final_df = pd.merge(user_scores[['Domain']], result_df, on='Domain', how='left')
        final_df['Sr_Score'] = final_df['Sr_Score'].fillna(0)

        logger.info("注册商评分计算完成，输出与 user_scores 对齐的 DataFrame")
        return final_df


# =================== 使用示例 ===================
if __name__ == "__main__":
    REGISTRAR_CONFIG = {
        'bayesian_smoothing_k': 10,
        'compliance_scores': {'high': 1.0, 'medium': 0.7, 'low': 0.3}
    }
    scorer = RegistrarScorer(REGISTRAR_CONFIG)

    # 示例数据加载
    # registrar_df = pd.read_csv('registrar_df.csv')
    # user_scores = pd.read_csv('user_scores.csv')
    # phish_data = pd.read_csv('phish_data.csv')
    # registrar_accredited = pd.read_csv('registrar_accredited.csv')

    # result = scorer.run(registrar_df, user_scores, phish_data, registrar_accredited)
    # print(result.head())
