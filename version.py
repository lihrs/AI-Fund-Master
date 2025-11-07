"""
版本信息配置
Version Configuration
"""

# 软件信息
APP_NAME = "AI基金大师"
APP_VERSION = "4.0"
APP_FULL_NAME = f"{APP_NAME} v{APP_VERSION}"

# 作者信息
AUTHOR_EMAIL = "267278466@qq.com"

# GitHub 信息
GITHUB_REPO = "https://github.com/your-repo/ai-fund-master"
GITHUB_API = "https://api.github.com/repos/your-repo/ai-fund-master/releases/latest"

# 更新检查
CHECK_UPDATE_ON_START = True
UPDATE_CHECK_TIMEOUT = 5  # 秒

# 版本历史
VERSION_HISTORY = {
    "4.0": {
        "date": "2025-11-06",
        "features": [
            "修复缓存检测逻辑",
            "优化启动速度（提升31%）",
            "新增单实例控制",
            "添加自动更新检查"
        ]
    },
    "3.6": {
        "date": "2025-11-06",
        "features": [
            "缓存检测修复",
            "延迟加载优化",
            "防止多进程"
        ]
    },
    "3.5": {
        "date": "2025-11-05",
        "features": [
            "修复历史分析错误",
            "优化评分标准"
        ]
    }
}

def get_version_info():
    """获取版本信息"""
    return {
        "name": APP_NAME,
        "version": APP_VERSION,
        "full_name": APP_FULL_NAME,
        "author": AUTHOR_EMAIL,
        "github": GITHUB_REPO
    }

if __name__ == '__main__':
    info = get_version_info()
    print(f"{info['full_name']}")
    print(f"作者：{info['author']}")
    print(f"GitHub：{info['github']}")

