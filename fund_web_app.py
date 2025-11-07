"""
基金分析 Web 应用
Fund Analysis Web Application

使用 Flask 提供 Web 界面，带系统托盘功能
"""

from flask import Flask, render_template, request, jsonify
import json
import numpy as np
import pandas as pd
from fund_analyzer import FundAnalyzer
import threading
import webbrowser
import sys
import os
import tkinter as tk
from tkinter import ttk
import pystray
from PIL import Image
from pystray import MenuItem as item
from version import APP_NAME, APP_VERSION, APP_FULL_NAME
import time
from datetime import datetime, timedelta

app = Flask(__name__)
analyzer = FundAnalyzer()

# 配置
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 16800
CONFIG_FILE = 'fund_app_settings.json'

# 🔥 自动退出配置
IDLE_TIMEOUT_MINUTES = 20  # 20分钟无活动自动退出
last_activity_time = time.time()  # 最后活动时间戳
idle_check_lock = threading.Lock()  # 线程锁

# 股票代码名称对照表（延迟加载）
_STOCK_NAMES = None

def get_stock_names():
    """延迟加载股票名称字典"""
    global _STOCK_NAMES
    if _STOCK_NAMES is None:
        from stockname_data import STOCK_NAME_DATA
        _STOCK_NAMES = STOCK_NAME_DATA.get('stocks', {})
    return _STOCK_NAMES


def clean_data_for_json(data):
    """清理数据中的 NaN 和 Infinity，使其可以序列化为 JSON"""
    if isinstance(data, dict):
        return {k: clean_data_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_data_for_json(item) for item in data]
    elif isinstance(data, float):
        if np.isnan(data) or np.isinf(data):
            return None
        return data
    elif isinstance(data, pd.DataFrame):
        return clean_data_for_json(data.to_dict('records'))
    else:
        return data


def update_activity_time():
    """更新最后活动时间"""
    global last_activity_time
    with idle_check_lock:
        last_activity_time = time.time()


@app.before_request
def before_request_callback():
    """在每个请求前更新活动时间"""
    update_activity_time()


@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


@app.route('/api/version', methods=['GET'])
def get_version():
    """获取当前版本信息"""
    return jsonify({
        "success": True,
        "data": {
            "name": APP_NAME,
            "version": APP_VERSION,
            "full_name": APP_FULL_NAME
        }
    })


@app.route('/api/check_update', methods=['GET'])
def check_update_api():
    """检查更新"""
    try:
        from updater import SoftwareUpdater, get_current_version
        
        # 获取当前版本
        current_version = get_current_version()
        
        # 版本文件URL（GitHub）
        version_url = "https://github.com/hengruiyun/AI-Fund-Master/raw/refs/heads/main/version.ini"
        
        # 创建更新器
        updater = SoftwareUpdater(current_version, version_url)
        
        # 读取远程版本信息
        version_info = updater.read_version_file()
        
        if not version_info:
            return jsonify({
                "success": True,
                "has_update": False,
                "message": "无法获取版本信息"
            })
        
        # 比较版本
        comparison = updater.compare_versions(current_version, version_info['version'])
        
        if comparison < 0:
            # 有新版本
            return jsonify({
                "success": True,
                "has_update": True,
                "data": {
                    "current_version": current_version,
                    "latest_version": version_info['version'],
                    "download_url": "https://github.com/hengruiyun/AI-Fund-Master/releases",
                    "exe_url": version_info.get('exe', ''),
                    "gz_url": version_info.get('gz', '')
                }
            })
        else:
            return jsonify({
                "success": True,
                "has_update": False,
                "message": "当前已是最新版本"
            })
            
    except Exception as e:
        return jsonify({
            "success": False,
            "has_update": False,
            "error": str(e)
        })


@app.route('/api/search', methods=['GET'])
def search_funds():
    """搜索基金"""
    keyword = request.args.get('keyword', '')
    
    if not keyword:
        return jsonify({"error": "请输入搜索关键词"}), 400
    
    try:
        df = analyzer.search_funds(keyword, limit=20)
        return jsonify({
            "success": True,
            "data": df.to_dict('records')
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>', methods=['GET'])
def get_fund_detail(ts_code):
    """获取基金详情"""
    try:
        report = analyzer.generate_report(ts_code)
        # 清理 NaN 值
        cleaned_report = clean_data_for_json(report)
        return jsonify({
            "success": True,
            "data": cleaned_report
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/returns', methods=['GET'])
def get_fund_returns(ts_code):
    """获取基金收益"""
    try:
        returns = analyzer.calculate_returns(ts_code)
        return jsonify({
            "success": True,
            "data": returns
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/risk', methods=['GET'])
def get_fund_risk(ts_code):
    """获取基金风险"""
    try:
        risk = analyzer.calculate_risk_metrics(ts_code)
        return jsonify({
            "success": True,
            "data": risk
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/score', methods=['GET'])
def get_fund_score(ts_code):
    """获取基金评分"""
    try:
        score = analyzer.calculate_fund_score(ts_code)
        return jsonify({
            "success": True,
            "data": score
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/holdings', methods=['GET'])
def get_fund_holdings(ts_code):
    """获取基金持仓（含股票名称）"""
    try:
        holdings = analyzer.get_top_holdings(ts_code, limit=10)
        holdings_list = holdings.to_dict('records')
        
        # 添加股票名称和行业
        for holding in holdings_list:
            symbol = holding.get('symbol', '')
            if symbol:
                # 提取纯数字代码（去掉.SH/.SZ/.HK等后缀）
                clean_code = symbol.split('.')[0]
                
                # 港股代码处理：如果是4位数，前面补0变成5位
                if len(clean_code) == 4 and clean_code.isdigit():
                    clean_code = '0' + clean_code
                
                stock_names = get_stock_names()
                stock_info = stock_names.get(clean_code, {})
                holding['stock_name'] = stock_info.get('name', '--')
                holding['industry'] = stock_info.get('industry', '--')
        
        # 清理 NaN 值
        cleaned_holdings = clean_data_for_json(holdings_list)
        return jsonify({
            "success": True,
            "data": cleaned_holdings
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/fund_flow', methods=['GET'])
def get_fund_flow(ts_code):
    """获取基金份额变化（资金流向）"""
    try:
        flow_data = analyzer.get_fund_flow(ts_code)
        cleaned_data = clean_data_for_json(flow_data)
        return jsonify({
            "success": True,
            "data": cleaned_data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/check_cache_status', methods=['GET'])
def check_cache_status():
    """检查预计算缓存状态"""
    try:
        status = analyzer.check_cache_status()
        return jsonify({
            "success": True,
            "data": status
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/batch_year_returns', methods=['POST'])
def batch_year_returns():
    """批量计算年度收益（优化性能）+ 同时返回评分"""
    try:
        data = request.get_json()
        ts_codes = data.get('ts_codes', [])
        years = data.get('years', ['2025', '2024', '2023'])
        use_cache = data.get('use_cache', True)  # 默认使用缓存
        include_score = data.get('include_score', True)  # 是否包含评分
        
        if not ts_codes:
            return jsonify({"error": "ts_codes不能为空"}), 400
        
        # 批量计算收益（支持缓存）
        if use_cache:
            results = analyzer.batch_get_cached_returns(ts_codes, years, fallback_to_realtime=False)
        else:
            results = analyzer.batch_calculate_year_returns(ts_codes, years)
        
        # 🔥 同时从缓存获取评分
        if include_score:
            scores = analyzer.batch_calculate_scores(ts_codes, results)
            # 将评分添加到结果中
            for ts_code in ts_codes:
                if ts_code in results and isinstance(results[ts_code], dict):
                    results[ts_code]['score'] = scores.get(ts_code)
        
        return jsonify({
            "success": True,
            "data": results,
            "from_cache": use_cache
        })
    except Exception as e:
        print(f"[ERROR] batch_year_returns失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/check_gold_rating', methods=['POST'])
def check_gold_rating_api():
    """批量检查红星评级"""
    try:
        data = request.get_json()
        funds = data.get('funds', [])  # [{"ts_code": "xxx", "rating": 4}, ...]
        
        if not funds:
            return jsonify({"error": "funds不能为空"}), 400
        
        results = {}
        for fund in funds:
            ts_code = fund.get('ts_code')
            rating = fund.get('rating', 0)
            
            if ts_code and rating >= 4:
                try:
                    is_gold = analyzer.check_gold_rating(ts_code, rating)
                    # 🔥 转换为Python原生bool类型，避免numpy.bool_序列化错误
                    results[ts_code] = bool(is_gold)
                except Exception as e:
                    print(f"[ERROR] check_gold_rating失败 {ts_code}: {e}")
                    import traceback
                    traceback.print_exc()
                    results[ts_code] = False
            else:
                results[ts_code] = False
        
        return jsonify({
            "success": True,
            "data": results
        })
    except Exception as e:
        print(f"[ERROR] check_gold_rating_api异常: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/batch_scores', methods=['POST'])
def batch_scores():
    """批量计算基金评分（优化性能）"""
    try:
        data = request.get_json()
        ts_codes = data.get('ts_codes', [])
        
        if not ts_codes:
            return jsonify({"error": "ts_codes不能为空"}), 400
        
        # 🔥 使用批量评分方法（优化：自动复用年度收益数据）
        year_returns = data.get('year_returns', None)  # 前端可传递已获取的收益数据
        results = analyzer.batch_calculate_scores(ts_codes, year_returns)
        
        return jsonify({
            "success": True,
            "data": results
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/year_end_nav', methods=['GET'])
def get_year_end_nav(ts_code):
    """获取基金年末净值数据（每年12月）"""
    try:
        nav_data = analyzer.get_year_end_nav(ts_code)
        return jsonify({
            "success": True,
            "data": nav_data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/index/000300/data', methods=['GET'])
def get_hs300_data():
    """获取沪深300指数数据用于对照"""
    try:
        from lj_read import StockDataReaderV2
        from config import DATA_DIR
        import os
        
        # 读取A股数据库（使用 config 中的路径）
        db_path = DATA_DIR / 'astock.db.gz'
        if not db_path.exists():
            return jsonify({"error": "A股数据库不存在"}), 404
        
        reader = StockDataReaderV2(str(db_path))
        
        # 获取沪深300指数数据（代码：000300，CN市场，index类型）
        df = reader.get_stock_data('000300', market='CN')
        
        if df.empty:
            return jsonify({"error": "未找到沪深300数据"}), 404
        
        # 转换为字典格式 {date: close}
        data = {}
        for _, row in df.iterrows():
            data[row['date']] = float(row['close'])
        
        return jsonify({
            "success": True,
            "data": data
        })
    except Exception as e:
        import traceback
        print(f"获取沪深300数据失败: {e}")
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/compare', methods=['GET'])
def compare_funds(ts_code):
    """同类对比"""
    try:
        comparison = analyzer.compare_with_peers(ts_code, top_n=10)
        return jsonify({
            "success": True,
            "data": comparison.to_dict('records')
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/top_performers', methods=['GET'])
def get_top_performers():
    """获取年度收益最高的前20名基金"""
    try:
        year = request.args.get('year', '2025')
        top_n = int(request.args.get('top_n', 20))
        
        # 使用 analyzer 的新方法
        results = analyzer.get_top_performers_by_year(year=year, top_n=top_n)
        
        # 清理 NaN 值
        cleaned_results = clean_data_for_json(results)
        
        return jsonify({
            "success": True,
            "data": cleaned_results
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/filter_options', methods=['GET'])
def get_filter_options():
    """获取筛选选项（基金公司、类型等）"""
    try:
        options = analyzer.get_filter_options()
        return jsonify({
            "success": True,
            "data": options
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/filter_funds', methods=['GET'])
def filter_funds():
    """根据条件筛选基金"""
    try:
        filters = {
            'search': request.args.get('search', ''),
            'company': request.args.get('company', ''),
            'fund_type': request.args.get('fund_type', ''),
            'invest_type': request.args.get('invest_type', ''),
            'risk_level': request.args.get('risk_level', ''),
            'status': request.args.get('status', 'L')
        }
        
        results = analyzer.filter_funds(filters)
        
        return jsonify({
            "success": True,
            "data": results.to_dict('records') if not results.empty else []
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/year_return', methods=['GET'])
def get_year_return(ts_code):
    """获取指定年度收益"""
    try:
        year = request.args.get('year', '2025')
        result = analyzer.calculate_year_return(ts_code, year)
        
        return jsonify({
            "success": True,
            "data": {"return": result} if result is not None else None
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/fund/<ts_code>/period_return', methods=['GET'])
def get_period_return(ts_code):
    """获取指定期间收益"""
    try:
        days = int(request.args.get('days', 365))
        result = analyzer.calculate_period_return(ts_code, days)
        
        return jsonify({
            "success": True,
            "data": {"return": result} if result is not None else None
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/notify', methods=['POST'])
def show_notification():
    """显示系统托盘气泡通知"""
    try:
        data = request.get_json()
        title = data.get('title', '基金分析系统')
        message = data.get('message', '')
        
        # 触发托盘通知
        if hasattr(app, 'tray_icon') and app.tray_icon:
            app.tray_icon.notify(title, message)
        
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


class Settings:
    """应用设置管理"""
    def __init__(self):
        self.auto_open_browser = True
        self.load()
    
    def load(self):
        """从文件加载设置"""
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.auto_open_browser = data.get('auto_open_browser', True)
        except Exception as e:
            print(f"加载设置失败: {e}")
    
    def save(self):
        """保存设置到文件"""
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'auto_open_browser': self.auto_open_browser
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存设置失败: {e}")


class SettingsDialog:
    """设置窗口 - 使用 tkinter"""
    def __init__(self, settings):
        self.settings = settings
        self.window = None
    
    def show(self):
        """显示设置窗口"""
        if self.window is not None:
            try:
                self.window.lift()
                self.window.focus_force()
                return
            except:
                pass
        
        self.window = tk.Tk()
        self.window.title('设置')
        self.window.geometry('400x200')
        self.window.resizable(False, False)
        
        # 设置图标
        icon_path = os.path.join(os.path.dirname(__file__), 'mrcai.ico')
        if os.path.exists(icon_path):
            try:
                self.window.iconbitmap(icon_path)
            except:
                pass
        
        # 居中窗口
        self.center_window()
        
        # 主框架
        main_frame = tk.Frame(self.window, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 标题
        title_label = tk.Label(main_frame, text='应用设置', font=('微软雅黑', 12, 'bold'))
        title_label.pack(pady=(0, 20))
        
        # 中间框架（用于垂直居中复选框）
        center_frame = tk.Frame(main_frame)
        center_frame.pack(expand=True)
        
        # 复选框
        self.auto_open_var = tk.BooleanVar(value=not self.settings.auto_open_browser)
        checkbox = tk.Checkbutton(
            center_frame, 
            text='禁止启动时自动打开浏览器',
            variable=self.auto_open_var,
            font=('微软雅黑', 10)
        )
        checkbox.pack()
        
        # 按钮框架（右下角）
        button_frame = tk.Frame(main_frame)
        button_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(20, 0))
        
        # 右对齐的按钮容器
        right_buttons = tk.Frame(button_frame)
        right_buttons.pack(side=tk.RIGHT)
        
        # 保存按钮
        save_button = tk.Button(
            right_buttons,
            text='保存设置',
            command=self.save_settings,
            bg='#4CAF50',
            fg='white',
            font=('微软雅黑', 10),
            width=10,
            height=1,
            relief=tk.FLAT,
            cursor='hand2'
        )
        save_button.pack(side=tk.LEFT, padx=(0, 10))
        
        # 退出按钮
        exit_button = tk.Button(
            right_buttons,
            text='退出',
            command=self.close,
            bg='#f44336',
            fg='white',
            font=('微软雅黑', 10),
            width=10,
            height=1,
            relief=tk.FLAT,
            cursor='hand2'
        )
        exit_button.pack(side=tk.LEFT)
        
        # 处理窗口关闭事件
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        
        self.window.mainloop()
    
    def center_window(self):
        """居中窗口"""
        self.window.update_idletasks()
        width = self.window.winfo_width()
        height = self.window.winfo_height()
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f'{width}x{height}+{x}+{y}')
    
    def save_settings(self):
        """保存设置"""
        self.settings.auto_open_browser = not self.auto_open_var.get()
        self.settings.save()
        self.close()
    
    def close(self):
        """关闭窗口"""
        if self.window:
            self.window.destroy()
            self.window = None


class SystemTrayApp:
    """系统托盘应用 - 使用 pystray"""
    def __init__(self):
        self.settings = Settings()
        self.icon = None
        self.idle_checker_thread = None
        
        # 启动Flask服务器
        self.start_server()
        
        # 🔥 启动空闲检查线程
        self.start_idle_checker()
        
        # 自动打开浏览器
        if self.settings.auto_open_browser:
            # 延迟一下让服务器启动
            threading.Timer(1.0, self.open_browser).start()
    
    def create_icon(self):
        """创建托盘图标"""
        # 加载图标
        icon_path = os.path.join(os.path.dirname(__file__), 'mrcai.ico')
        try:
            image = Image.open(icon_path)
        except:
            # 如果加载失败，创建一个简单的图标
            image = Image.new('RGB', (64, 64), color='blue')
        
        # 创建菜单
        menu = pystray.Menu(
            item('打开', self.open_browser),
            item('设置', self.show_settings),
            pystray.Menu.SEPARATOR,
            item('退出', self.exit_app)
        )
        
        # 创建托盘图标（显示软件名称+版本号）
        self.icon = pystray.Icon(
            APP_NAME,
            image,
            APP_FULL_NAME,  # tooltip 显示 "AI基金大师 v4.0"
            menu
        )
        
        # 将托盘图标保存到 app 对象，供通知API使用
        app.tray_icon = self.icon
    
    def start_server(self):
        """在后台线程启动Flask服务器"""
        def run_server():
            app.run(host=SERVER_HOST, port=SERVER_PORT, debug=False, use_reloader=False)
        
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        print("=" * 60)
        print(f"{APP_FULL_NAME} 已启动")
        print(f"访问地址: http://localhost:{SERVER_PORT}")
        print(f"服务器监听: {SERVER_HOST}:{SERVER_PORT}")
        print(f"🕐 空闲超时: {IDLE_TIMEOUT_MINUTES} 分钟")
        print("=" * 60)
    
    def start_idle_checker(self):
        """启动空闲检查线程"""
        def check_idle():
            """检查空闲时间，超时则退出"""
            check_interval = 60  # 每60秒检查一次
            
            while True:
                time.sleep(check_interval)
                
                with idle_check_lock:
                    current_time = time.time()
                    idle_seconds = current_time - last_activity_time
                    idle_minutes = idle_seconds / 60
                    
                    if idle_minutes >= IDLE_TIMEOUT_MINUTES:
                        print(f"\n{'='*60}")
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
                        print(f"⏰ 检测到 {idle_minutes:.1f} 分钟无活动")
                        print(f"💤 达到空闲超时阈值 ({IDLE_TIMEOUT_MINUTES} 分钟)")
                        print(f"🚪 自动退出应用...")
                        print("="*60)
                        
                        # 触发退出
                        self.exit_app()
                        break
        
        self.idle_checker_thread = threading.Thread(target=check_idle, daemon=True)
        self.idle_checker_thread.start()
        print(f"✓ 空闲检查线程已启动")
    
    def open_browser(self, icon=None, item=None):
        """打开浏览器"""
        url = f'http://localhost:{SERVER_PORT}'
        webbrowser.open(url)
    
    def show_settings(self, icon=None, item=None):
        """显示设置窗口"""
        def run_dialog():
            dialog = SettingsDialog(self.settings)
            dialog.show()
        
        # 在新线程中运行 tkinter 对话框
        threading.Thread(target=run_dialog, daemon=True).start()
    
    def exit_app(self, icon=None, item=None):
        """退出应用"""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 正在退出应用...")
        if self.icon:
            self.icon.stop()
        sys.exit(0)
    
    def run(self):
        """运行应用"""
        self.create_icon()
        self.icon.run()


if __name__ == '__main__':
    # 单实例控制
    from single_instance import ensure_single_instance
    
    instance = ensure_single_instance(APP_NAME, show_dialog=True)
    
    if instance is None:
        # 已有实例在运行，退出
        sys.exit(0)
    
    try:
        tray_app = SystemTrayApp()
        tray_app.run()
    finally:
        # 确保释放锁
        instance.release()


