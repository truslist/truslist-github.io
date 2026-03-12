"""
Main entry point.
Loads configuration, invokes each scoring module, and aggregates the results.
"""

import os
import logging
import pandas as pd
from config.setting import (
    DATA_SOURCES, USER_BEHAVIOR_CONFIG, REGISTRAR_CONFIG, TLD_CONFIG,
    LINK_STRUCTURE_CONFIG, AHP_WEIGHTS, OUTPUT_CONFIG, PERFORMANCE_CONFIG
)

# ===== Module imports =====
from modules.user_behavior import UserBehaviorScorer
from modules.registrar_score import RegistrarScorer
from modules.tld_score import TLDScorer
from modules.link_structure import LinkStructureScorer
from modules.ahp import AHPIntegrator
from modules.umbrella import UmbrellaRanker

def setup_logging():
    """Initialize logging."""
    log_level = getattr(logging, OUTPUT_CONFIG['log_level'].upper(), logging.INFO)
    log_file = OUTPUT_CONFIG.get('log_file', None)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file) if log_file else logging.StreamHandler()]
    )
    logging.info("Logging initialized.")


def ensure_output_dir():
    """Ensure the output directory exists."""
    os.makedirs(OUTPUT_CONFIG['results_dir'], exist_ok=True)


def choose_file():
    """Interactively select a PDNS file."""
    pdns_dir = DATA_SOURCES['pdns_data_path']
    files = [f for f in os.listdir(pdns_dir) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No PDNS files found in directory: {pdns_dir}")

    # Sort by name (assumes filenames contain dates)
    files.sort(reverse=True)

    print("Available PDNS files:")
    for i, f in enumerate(files):
        print(f"[{i}] {f}")

    while True:
        try:
            choice = int(input("Enter the index of the file to use: "))
            if 0 <= choice < len(files):
                return os.path.join(pdns_dir, files[choice]), files[choice]
            else:
                print("Invalid input, please enter a valid index.")
        except ValueError:
            print("Please enter a number.")


def load(file_path):
    """Load the specified PDNS file, skipping malformed lines."""
    df = pd.read_csv(
        file_path,
        sep=',',
        dtype=str,
        quotechar='"',
        engine='python',
        on_bad_lines='skip',
        encoding='utf-8-sig'
    )
    logging.info(f"PDNS data loaded: {df.shape[0]} records")
    return df


def main():
    # Initialization
    setup_logging()
    ensure_output_dir()
    logging.info("Configuration loaded, starting pipeline...")

    # ===== Step 0: Interactively select PDNS file =====
    file_path, file_name = choose_file()
    logging.info(f"Selected PDNS file: {file_name}")
    user_behavior_df = load(file_path)

    # ===== Step 1: Load other raw data =====
    try:
        registrar_df = pd.read_csv(DATA_SOURCES['registrar_data_path'], dtype=str, engine='python', on_bad_lines='skip')
        tld_df = pd.read_csv(DATA_SOURCES['tld_data_path'], dtype=str, engine='python', on_bad_lines='skip')
        link_df = pd.read_csv(DATA_SOURCES['link_data_path'], dtype=str, engine='python', on_bad_lines='skip')
        phish_data = pd.read_csv(DATA_SOURCES['phish_tank_path'], dtype=str, engine='python', on_bad_lines='skip')
        registrar_accredited = pd.read_csv(DATA_SOURCES['registrar_accredited'], dtype=str, engine='python', on_bad_lines='skip')
        SecRank = pd.read_csv(
            DATA_SOURCES['secrank_data_path'],
            sep=None,  # Auto-detect separator
            engine='python',
            dtype=str,
            header=None,  # No header row
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
        logging.info("All raw data loaded successfully.")
    except Exception as e:
        logging.error(f"Data loading failed: {e}")
        return

    # ===== Step 2: User behavior scoring =====
    user_scorer = UserBehaviorScorer(USER_BEHAVIOR_CONFIG)
    user_scores = user_scorer.run(user_behavior_df)
    logging.info("User behavior scoring complete.")

    # ===== Step 3: Registrar scoring =====
    registrar_scorer = RegistrarScorer(REGISTRAR_CONFIG)
    registrar_scores = registrar_scorer.run(registrar_df, user_scores, phish_data, registrar_accredited)
    logging.info("Registrar scoring complete.")

    # ===== Step 4: TLD scoring =====
    tld_scorer = TLDScorer(TLD_CONFIG)
    tld_scores = tld_scorer.run(user_scores, tld_df)
    logging.info("TLD scoring complete.")

    # ===== Step 5: Link structure scoring =====
    link_scorer = LinkStructureScorer(LINK_STRUCTURE_CONFIG)
    link_scores = link_scorer.run(user_scores, link_df, Tranco, SecRank)
    logging.info("Link structure scoring complete.")

    # ===== Step 6: AHP weight fusion =====
    ahp = AHPIntegrator(AHP_WEIGHTS)
    final_scores = ahp.integrate(user_scores, registrar_scores, tld_scores, link_scores)
    logging.info("AHP weight fusion complete.")

    # ===== Step 7: Save results =====
    # Output filename matches the PDNS file date
    date_tag = os.path.splitext(file_name)[0]   # strip .csv
    output_path = os.path.join(
        OUTPUT_CONFIG['TopList_dir'],
        f"TopList{date_tag}.{OUTPUT_CONFIG['output_format']}"
    )
    ahp.save_results(final_scores, output_path, OUTPUT_CONFIG['output_format'])
    logging.info(f"Results saved to: {output_path}")

    # ===== Umbrella ranking =====
    umbrella = UmbrellaRanker()
    umbrella_Rank = umbrella.run(user_behavior_df)
    output_path = os.path.join(
        OUTPUT_CONFIG['Umbrella_dir'],
        f"umbrella{date_tag}.{OUTPUT_CONFIG['output_format']}"
    )
    ahp.save_results(umbrella_Rank, output_path, OUTPUT_CONFIG['output_format'])
    logging.info(f"Umbrella results saved to: {output_path}")

    # ===== SecRank ranking =====
    output_path = os.path.join(
        OUTPUT_CONFIG['SecRank_dir'],
        f"SecRank{date_tag}.{OUTPUT_CONFIG['output_format']}"
    )
    user_scorer.save_results(user_scores, output_path, OUTPUT_CONFIG['output_format'])
    logging.info(f"SecRank results saved to: {output_path}")
if __name__ == "__main__":
    main()
