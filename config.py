# -*- coding: utf-8 -*-
"""配置管理模块"""

import json
import os

# 默认配置
DEFAULT_CONFIG = {
    "download_interval": 15,  # 默认下载间隔（秒）
    "min_interval": 5,        # 最小间隔
    "max_interval": 120,      # 最大间隔
    "max_retries": 3,         # 最大重试次数
    "timeout": 30,            # 下载超时（秒）
    "excel_start_row": 2,     # Excel数据起始行
    "name_column": "E",       # 姓名列
    "intro_column": "F",      # 简介列
    "url_column": "G",        # 图片链接列
    "parallel_workers": 3,    # 并行下载线程数
    "min_parallel": 1,        # 最小并发数
    "max_parallel": 10,       # 最大并发数
    "last_data_dir": "",      # 上次选择数据文件的目录
    "last_save_dir": "",      # 上次选择的保存目录
    "asset_db_backend": "sqlite",  # 资产库后端: sqlite/postgres
    "asset_db_url": "",       # PostgreSQL DSN（当 backend=postgres 时使用）
}


class Config:
    """配置管理类"""
    
    def __init__(self, config_file=None):
        self.config = DEFAULT_CONFIG.copy()
        self.config_file = config_file
        if config_file and os.path.exists(config_file):
            self.load(config_file)
    
    def get(self, key, default=None):
        return self.config.get(key, default)
    
    def set(self, key, value):
        self.config[key] = value
    
    def load(self, filepath):
        """从文件加载配置"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                self.config.update(loaded)
        except Exception:
            pass
    
    def save(self, filepath):
        """保存配置到文件"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
