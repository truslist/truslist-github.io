
"""
用户行为评分模块
基于论文中的SecRank启发方法，计算域名的用户行为评分
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
import tldextract

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserBehaviorScorer:
    """
    用户行为评分器
    实现论文中的用户行为评分模块，包括：
    1. IP-域偏好建模
    2. IP权重计算
    3. 改进的Borda计数法全局评分
    """

    def __init__(self, config: dict):
        """
        初始化用户行为评分器

        Args:
            config: 配置字典，包含各种参数
        """
        self.config = config
        self.time_slot_minutes = config.get('time_slot_minutes', 10)
        self.slots_per_day = config.get('slots_per_day', 144)
        self.top_n_domains = config.get('top_n_domains', 100)
        self.log_smoothing_base = config.get('log_smoothing_base', 1)

        logger.info(f"初始化用户行为评分器: 时间槽={self.time_slot_minutes}分钟, "
                    f"每日槽数={self.slots_per_day}, Top-N={self.top_n_domains}")

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据预处理

        Args:
            df: 原始数据DataFrame，包含timestamp, src_ip, domain等列

        Returns:
            预处理后的DataFrame
        """
        logger.info("开始数据预处理...")

        processed_df = df.copy()
        required_columns = ['timestamp', 'src_ip']
        missing_columns = [col for col in required_columns if col not in processed_df.columns]
        if missing_columns:
            raise ValueError(f"数据缺少必需列: {missing_columns}")

        domain_column = None
        if 'domain' in processed_df.columns:
            domain_column = 'domain'
        elif 'dns.rrname' in processed_df.columns:
            domain_column = 'dns.rrname'
            processed_df = processed_df.rename(columns={'dns.rrname': 'domain'})
            logger.info("将dns.rrname列重命名为domain列")
        elif 'SLD' in processed_df.columns:
            domain_column = 'SLD'
            processed_df['domain'] = processed_df['SLD']
            logger.info("从SLD列创建domain列")
        else:
            raise ValueError("数据中缺少域名列，需要包含domain、dns.rrname或SLD列之一")

        processed_df['timestamp'] = pd.to_datetime(processed_df['timestamp'], errors='coerce')
        invalid_count = processed_df['timestamp'].isna().sum()
        if invalid_count > 0:
            logger.warning(f"发现 {invalid_count} 条无效时间戳记录，已过滤")
            processed_df = processed_df[processed_df['timestamp'].notna()]

        processed_df['date'] = processed_df['timestamp'].dt.date
        processed_df['SLD'] = processed_df['domain'].apply(self._extract_sld)
        processed_df = processed_df[processed_df['SLD'].notna()]

        logger.info(f"预处理完成，有效记录数: {len(processed_df)}")
        logger.info(f"列名: {processed_df.columns.tolist()}")
        return processed_df

    def _extract_sld(self, domain: str) -> Optional[str]:
        """
        从域名中提取二级域名，使用tldextract包确保准确性
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
            logger.warning(f"SLD提取失败，域名: {domain}, 错误: {str(e)}")
            return None

    def compute_ip_domain_preferences(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算IP-域偏好分数
        """
        logger.info("计算IP-域偏好分数...")
        all_preferences = []
        for date in tqdm(df['date'].unique(), desc="计算每日偏好分数"):
            date_df = df[df['date'] == date]
            query_counts = date_df.groupby(['src_ip', 'SLD']).size().reset_index(name='query_count')
            access_persistence = self._calculate_access_persistence(date_df)
            merged = pd.merge(query_counts, access_persistence, on=['src_ip', 'SLD'], how='outer').fillna(0)
            merged['gamma_smooth'] = np.log(1 + merged['query_count'])
            merged['alpha_smooth'] = np.log(1 + merged['access_slots'])   # 论文: minmax(log(1+α))
            merged = self._normalize_by_ip(merged, 'gamma_smooth', 'gamma_norm')
            merged = self._normalize_by_ip(merged, 'alpha_smooth', 'alpha_norm')
            merged['preference_score'] = np.sqrt(merged['gamma_norm'] * merged['alpha_norm'])
            merged['date'] = date
            all_preferences.append(merged)
        preferences_df = pd.concat(all_preferences, ignore_index=True)
        logger.info(f"偏好分数计算完成，总记录数: {len(preferences_df)}")
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
        logger.info("计算IP权重（含 EWMA KL 散度惩罚）...")

        alpha_ewma = self.config.get('ewma_alpha', 0.1)   # 论文: α=0.1
        beta_penalty = self.config.get('beta_penalty', 1.0)  # 论文: β=1.0

        # 按日期排序，保证 EWMA 时序正确
        sorted_dates = sorted(df['date'].unique())

        # 构建全局参考分布 Q_ref：所有记录中各 SLD 的查询占比
        total_counts = df.groupby('SLD').size()
        q_ref = (total_counts / total_counts.sum()).to_dict()

        # EWMA 状态: {src_ip: bar_delta}
        ewma_state: dict = {}

        all_weights = []
        for date in tqdm(sorted_dates, desc="计算每日IP权重"):
            date_df = df[df['date'] == date]

            # W_i^0：基础权重（几何均值）
            domain_diversity = date_df.groupby('src_ip')['SLD'].nunique().reset_index(name='unique_slds')
            total_queries = date_df.groupby('src_ip').size().reset_index(name='total_queries')
            merged = pd.merge(domain_diversity, total_queries, on='src_ip')
            merged['diversity_smooth'] = np.log(1 + merged['unique_slds'])
            merged['queries_smooth'] = np.log(1 + merged['total_queries'])
            merged = self._normalize_by_date(merged, 'diversity_smooth', 'diversity_norm')
            merged = self._normalize_by_date(merged, 'queries_smooth', 'queries_norm')
            merged['W0'] = np.sqrt(merged['diversity_norm'] * merged['queries_norm'])

            # KL 散度：每个 IP 的查询分布 Q_i vs Q_ref
            ip_kl = {}
            for src_ip, ip_df in date_df.groupby('src_ip'):
                q_i_counts = ip_df.groupby('SLD').size()
                q_i = (q_i_counts / q_i_counts.sum()).to_dict()
                kl = 0.0
                for domain, p in q_i.items():
                    q = q_ref.get(domain, 1e-10)
                    kl += p * np.log(p / q)
                ip_kl[src_ip] = max(kl, 0.0)  # 数值稳定

            # Δ_init = 75th 百分位（首次出现的 IP 使用全局先验）
            kl_values = np.array(list(ip_kl.values()))
            delta_init = float(np.percentile(kl_values, 75)) if len(kl_values) > 0 else 0.0

            # EWMA 更新: bar_Δ_i^(t) = α·Δ_i^KL,(t) + (1-α)·bar_Δ_i^(t-1)
            new_ewma_state = {}
            for src_ip, kl_t in ip_kl.items():
                prev = ewma_state.get(src_ip, delta_init)  # 未见过的 IP 用 delta_init
                new_ewma_state[src_ip] = alpha_ewma * kl_t + (1 - alpha_ewma) * prev
            ewma_state.update(new_ewma_state)

            # W_i = W_i^0 · exp(-β · bar_Δ_i^(t))
            merged['bar_delta'] = merged['src_ip'].map(lambda ip: ewma_state.get(ip, delta_init))
            merged['weight'] = merged['W0'] * np.exp(-beta_penalty * merged['bar_delta'])
            merged['date'] = date
            all_weights.append(merged)

        weights_df = pd.concat(all_weights, ignore_index=True)
        logger.info(f"IP权重计算完成，总记录数: {len(weights_df)}")
        return weights_df

    def _normalize_by_date(self, df: pd.DataFrame, source_col: str, target_col: str) -> pd.DataFrame:
        df[target_col] = (df[source_col] - df[source_col].min()) / (df[source_col].max() - df[source_col].min())
        return df

    def compute_global_scores(self, preferences_df: pd.DataFrame, weights_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("计算全局评分...")
        all_scores = []
        for date in tqdm(preferences_df['date'].unique(), desc="计算每日全局评分"):
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
        logger.info(f"全局评分计算完成，总记录数: {len(scores_df)}")
        return scores_df.sort_values(['date', 'rank'])

    def save_results(self, df, path, fmt='csv'):
        """
        保存结果，按 Sd_Score 降序排序

        Args:
            df: run() 的输出 DataFrame，包含 Domain 和 Sd_Score
            path: 保存路径
            fmt: 'csv' 或 'xlsx'
        """
        save_df = df.sort_values(by="Sd_Score", ascending=False).reset_index(drop=True)

        if fmt == 'csv':
            save_df.to_csv(path, index=False, encoding='utf-8-sig')
        elif fmt == 'xlsx':
            save_df.to_excel(path, index=False)
        else:
            raise ValueError(f"不支持的输出格式: {fmt}")

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        执行完整的用户行为评分流程，并返回最终简化DF，归一化Sd_Score
        """
        start_time = time.time()
        try:
            logger.info("开始用户行为评分计算...")

            processed_df = self.preprocess_data(df)
            preferences_df = self.compute_ip_domain_preferences(processed_df)
            self.preferences_df = preferences_df
            weights_df = self.compute_ip_weights(processed_df)
            self.weights_df = weights_df
            final_scores = self.compute_global_scores(preferences_df, weights_df)

            # 构建最终输出，只保留 Domain 和 Sd_Score
            result_df = final_scores[['SLD', 'global_score']].rename(
                columns={'SLD': 'Domain', 'global_score': 'Sd_Score'}
            )

            # 对 Sd_Score 做归一化
            min_score = result_df['Sd_Score'].min()
            max_score = result_df['Sd_Score'].max()
            if max_score > min_score:
                result_df['Sd_Score'] = (result_df['Sd_Score'] - min_score) / (max_score - min_score)
            else:
                result_df['Sd_Score'] = 0.0  # 当所有分数相同，归一化为0

            elapsed_time = time.time() - start_time
            logger.info(f"用户行为评分计算完成，耗时: {elapsed_time:.2f}秒")
            return result_df

        except Exception as e:
            logger.error(f"用户行为评分计算失败: {str(e)}")
            raise


        except Exception as e:
            logger.error(f"用户行为评分计算失败: {str(e)}")
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
