"""
主入口文件
负责加载配置，调用各个评分模块，并整合结果输出
"""

import os
import logging
import pandas as pd
from config.setting import (
    DATA_SOURCES, USER_BEHAVIOR_CONFIG, REGISTRAR_CONFIG, TLD_CONFIG,
    LINK_STRUCTURE_CONFIG, AHP_WEIGHTS, OUTPUT_CONFIG, PERFORMANCE_CONFIG
)

# ===== 模块导入 =====
from modules.user_behavior import UserBehaviorScorer
from modules.registrar_score import RegistrarScorer
from modules.tld_score import TLDScorer
from modules.link_structure import LinkStructureScorer
from modules.ahp import AHPIntegrator
from modules.umbrella import UmbrellaRanker

def setup_logging():
    """初始化日志"""
    log_level = getattr(logging, OUTPUT_CONFIG['log_level'].upper(), logging.INFO)
    log_file = OUTPUT_CONFIG.get('log_file', None)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file) if log_file else logging.StreamHandler()]
    )
    logging.info("日志系统初始化完成")


def ensure_output_dir():
    """确保结果目录存在"""
    os.makedirs(OUTPUT_CONFIG['results_dir'], exist_ok=True)


def choose_file():
    """交互式选择 PDNS 文件"""
    pdns_dir = DATA_SOURCES['pdns_data_path']
    files = [f for f in os.listdir(pdns_dir) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"目录中未找到任何 PDNS 文件: {pdns_dir}")

    # 按名称排序（假设文件名里有日期）
    files.sort(reverse=True)

    print("可用的 PDNS 文件:")
    for i, f in enumerate(files):
        print(f"[{i}] {f}")

    while True:
        try:
            choice = int(input("请输入要选择的文件序号: "))
            if 0 <= choice < len(files):
                return os.path.join(pdns_dir, files[choice]), files[choice]
            else:
                print("输入无效，请输入正确的序号。")
        except ValueError:
            print("请输入数字。")


def load(file_path):
    """加载指定 PDNS 文件，自动跳过异常行"""
    df = pd.read_csv(
        file_path,
        sep=',',
        dtype=str,
        quotechar='"',
        engine='python',
        on_bad_lines='skip',
        encoding='utf-8-sig'
    )
    logging.info(f"PDNS 数据加载完成: {df.shape[0]} 条记录")
    return df


def main():
    # 初始化
    setup_logging()
    ensure_output_dir()
    logging.info("配置加载完成，开始运行流程...")

    # ===== Step 0: 交互式选择 PDNS 文件 =====
    file_path, file_name = choose_file()
    logging.info(f"选择的 PDNS 文件: {file_name}")
    user_behavior_df = load(file_path)

    # ===== Step 1: 读取其它原始数据 =====
    try:
        registrar_df = pd.read_csv(DATA_SOURCES['registrar_data_path'], dtype=str, engine='python', on_bad_lines='skip')
        tld_df = pd.read_csv(DATA_SOURCES['tld_data_path'], dtype=str, engine='python', on_bad_lines='skip')
        link_df = pd.read_csv(DATA_SOURCES['link_data_path'], dtype=str, engine='python', on_bad_lines='skip')
        phish_data = pd.read_csv(DATA_SOURCES['phish_tank_path'], dtype=str, engine='python', on_bad_lines='skip')
        registrar_accredited = pd.read_csv(DATA_SOURCES['registrar_accredited'], dtype=str, engine='python', on_bad_lines='skip')
        SecRank = pd.read_csv(
            DATA_SOURCES['secrank_data_path'],
            sep=None,  # 自动检测分隔符
            engine='python',
            dtype=str,
            header=None,  # 没有表头
            names=['Domain', 'Score', 'Rank'],
            on_bad_lines='skip'
        )

        Tranco = pd.read_csv(
            DATA_SOURCES['tranco_data_path'],
            names=['rank', 'domain'],
            dtype=str,
            engine='python',
            on_bad_lines='skip',
            header=0
        )
        logging.info("其它原始数据加载完成")
    except Exception as e:
        logging.error(f"数据加载失败: {e}")
        return

    # ===== Step 2: 用户行为评分 =====
    user_scorer = UserBehaviorScorer(USER_BEHAVIOR_CONFIG)
    user_scores = user_scorer.run(user_behavior_df)
    logging.info("用户行为评分完成")

    # ===== Step 3: 注册商评分 =====
    registrar_scorer = RegistrarScorer(REGISTRAR_CONFIG)
    registrar_scores = registrar_scorer.run(registrar_df, user_scores, phish_data, registrar_accredited)
    logging.info("注册商评分完成")

    # ===== Step 4: TLD 评分 =====
    tld_scorer = TLDScorer(TLD_CONFIG)
    tld_scores = tld_scorer.run(user_scores, tld_df)
    logging.info("TLD 评分完成")

    # ===== Step 5: 链接结构评分 =====
    link_scorer = LinkStructureScorer(LINK_STRUCTURE_CONFIG)
    link_scores = link_scorer.run(user_scores, link_df, Tranco, SecRank)
    logging.info("链接结构评分完成")

    # ===== Step 6: AHP 权重融合 =====
    ahp = AHPIntegrator(AHP_WEIGHTS)
    final_scores = ahp.integrate(user_scores, registrar_scores, tld_scores, link_scores)
    logging.info("AHP 权重融合完成")

    # ===== Step 7: 输出结果 =====
    # 结果文件名与 PDNS 文件日期保持一致
    date_tag = os.path.splitext(file_name)[0]   # 去掉 .csv
    output_path = os.path.join(
        OUTPUT_CONFIG['TopList_dir'],
        f"TopList{date_tag}.{OUTPUT_CONFIG['output_format']}"
    )
    ahp.save_results(final_scores, output_path, OUTPUT_CONFIG['output_format'])
    logging.info(f"结果已保存到: {output_path}")

    # ===== Umbrella排名评分 =====
    umbrella = UmbrellaRanker()
    umbrella_Rank = umbrella.run(user_behavior_df)
    output_path = os.path.join(
        OUTPUT_CONFIG['Umbrella_dir'],
        f"umbrella{date_tag}.{OUTPUT_CONFIG['output_format']}"
    )
    ahp.save_results(umbrella_Rank, output_path, OUTPUT_CONFIG['output_format'])
    logging.info(f"结果已保存到: {output_path}")
    logging.info("Umbrella已保存")

    # ===== SecRank排名评分 =====
    output_path = os.path.join(
        OUTPUT_CONFIG['SecRank_dir'],
        f"SecRank{date_tag}.{OUTPUT_CONFIG['output_format']}"
    )
    user_scorer.save_results(user_scores, output_path, OUTPUT_CONFIG['output_format'])
    logging.info(f"结果已保存到: {output_path}")
    logging.info("SecRank已保存")

    #
if __name__ == "__main__":
    main()
