"""
Global configuration file.
Contains all parameters and settings required to run the model.
"""
import os

from win32con import MERGEPAINT

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_SOURCES = {
    'pdns_data_path': os.path.join(BASE_DIR, 'data', 'filtered'),
    'registrar_data_path': os.path.join(BASE_DIR, 'data', 'raw','registrar_info.csv'),
    'tld_data_path': os.path.join(BASE_DIR, 'data', 'raw','import_domain.csv'),
    'link_data_path': os.path.join(BASE_DIR, 'data', 'raw','Link_data.csv'),
    'phish_tank_path': os.path.join(BASE_DIR, 'data', 'raw','threat_intelligence.csv'),
    'registrar_accredited': os.path.join(BASE_DIR, 'data', 'raw','Accredited-Registrars.csv'),
    'secrank_data_path': os.path.join(BASE_DIR, 'data', 'rank','SecRank','2025-08-15'),
    'tranco_data_path': os.path.join(BASE_DIR, 'data', 'rank','Tranco','tranco_08-15.csv'),
    'result':os.path.join(BASE_DIR,'data','result')
}

MERGE_SOURCES = {
    'secrank_data_path':os.path.join(BASE_DIR, 'data', 'rank','SecRank'),
    'tranco_data_path':os.path.join(BASE_DIR, 'data', 'rank','SecRank'),
    'TopList':os.path.join(BASE_DIR, 'data', 'result', 'TopList'),
    'SecRank':os.path.join(BASE_DIR, 'data', 'result', 'SecRank'),
    'Umbrella':os.path.join(BASE_DIR,'data', 'result', 'Umbrella'),
    'MergeTopList':os.path.join(BASE_DIR, 'data', 'MergeRank', 'TopList'),
    'MergeSecRank':os.path.join(BASE_DIR, 'data', 'MergeRank', 'SecRank'),
    'MergeUmbrella':os.path.join(BASE_DIR,'data', 'MergeRank', 'Umbrella'),
}

# ── User behavior scoring module configuration ──
USER_BEHAVIOR_CONFIG = {
    'time_slot_minutes': 10,  # Time slot length (minutes)
    'slots_per_day': 144,     # Number of time slots per day
    'top_n_domains': 100,     # Top-N domains each IP votes for
    'log_smoothing_base': 1,  # Logarithmic smoothing base
    'ewma_alpha': 0.1,        # Paper Eq.6: α=0.1
    'beta_penalty': 1.0       # Paper Eq.weight: β=1.0
}

# ── Registrar scoring module configuration ──
REGISTRAR_CONFIG = {
    'bayesian_smoothing_k': 10,  # Bayesian smoothing parameter
    'compliance_scores': {
        'high': 1.0,      # High-compliance regions
        'medium': 0.7,    # Medium-compliance regions
        'low': 0.3        # Low-compliance regions
    }
}

# ── TLD scoring configuration ──
TLD_CONFIG = {
    'strict_tlds': {
        '.gov.cn': 0.6,   # Chinese government agencies
        '.edu.cn': 0.6,   # Chinese higher education institutions
        '.mil.cn': 0.5,   # Chinese military units
        '.gov': 0.4,      # US government entities
        '.edu': 0.4,      # US higher education institutions
        '.ac.cn': 0.4,    # Chinese research institutes
        '.org.cn': 0.4,   # Chinese non-profit organizations
    },
    'default_score': 0
}

# ── Link structure scoring configuration ──
LINK_STRUCTURE_CONFIG = {
    'reference_sources': ['tranco', 'secrank', 'pdns'],
    'max_reference_domains': 10000,  # Paper: Top-10000 per source; ~25,000 in union
    'ranking_data': {
        'secrank_format': 'space_separated',
        'tranco_format': 'csv_ranked',
        'top_n_domains': 10000          # Paper: Top-10000
    }
}

# ── AHP weight configuration ──
AHP_WEIGHTS = {
    'Sd': 0.479,    # User behavior weight
    'St': 0.338,    # TLD credibility weight
    'Sr': 0.112,    # Registrar weight
    'Sp': 0.071     # Link structure weight
}

# ── Output configuration ──
OUTPUT_CONFIG = {
    'userbehavior':'data/results/user_behavior',
    'registrar_score':'data/results/registrar_score',
    'tld_score':'data/results/tld_score',
    'Umbrella_dir': os.path.join(BASE_DIR,'data','result','Umbrella'),
    'TopList_dir': os.path.join(BASE_DIR,'data','result','TopList'),
    'SecRank_dir': os.path.join(BASE_DIR,'data','result','SecRank'),
    'results_dir' : os.path.join(BASE_DIR,'data','result'),
    'log_level': 'INFO',
    'save_intermediate': True,
    'output_format': 'csv'  # csv, json, parquet
}

# ── Performance configuration ──
PERFORMANCE_CONFIG = {
    'batch_size': 10000,      # Batch size
    'max_workers': 4,         # Maximum number of worker processes
    'chunk_size': 100000,     # Data chunk size
    'memory_limit_gb': 8      # Memory limit (GB)
}