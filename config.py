"""
配置文件
Configuration file for fund analysis system
"""

import os
import sys
from pathlib import Path


def get_resource_path(relative_path):
    """
    获取资源文件的绝对路径，兼容开发环境和打包后的环境
    
    PyInstaller 会将文件解压到 _MEIPASS 临时目录
    """
    if getattr(sys, 'frozen', False):
        # 打包后的环境
        base_path = Path(sys._MEIPASS)
    else:
        # 开发环境
        base_path = Path(__file__).parent
    
    return base_path / relative_path


def get_data_path():
    """
    获取数据目录路径
    
    优先级：
    1. exe 同目录下的 data 文件夹（用户数据）
    2. 打包到程序内的 data 文件夹
    3. 开发环境的 data 文件夹
    """
    if getattr(sys, 'frozen', False):
        # 打包后的环境：优先使用 exe 同目录的 data
        exe_dir = Path(os.path.dirname(sys.executable))
        external_data = exe_dir / "data"
        
        if external_data.exists():
            return external_data
        else:
            # 如果外部没有，使用打包进去的
            return get_resource_path("data")
    else:
        # 开发环境
        return Path(__file__).parent / "data"


# 项目根目录
PROJECT_ROOT = Path(__file__).parent if not getattr(sys, 'frozen', False) else Path(os.path.dirname(sys.executable))

# 数据目录
DATA_DIR = get_data_path()

# 数据库路径
DB_PATH = DATA_DIR / "aifm.db"
COMPRESSED_DB_PATH = DATA_DIR / "aifm.db.gz"

# 如果解压后的数据库不存在，默认使用压缩版本
if not DB_PATH.exists() and COMPRESSED_DB_PATH.exists():
    DB_PATH = COMPRESSED_DB_PATH




