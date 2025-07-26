#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI基金大师GUI界面 - PyQt5版本
使用PyQt5重新实现的现代化界面
"""

import sys
import os
import json
import time
import tempfile
import webbrowser
import threading
from datetime import datetime, timedelta
from io import StringIO

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QTabWidget, QLabel, QPushButton, QLineEdit, QTextEdit, QComboBox,
    QCheckBox, QProgressBar, QGroupBox, QGridLayout, QScrollArea,
    QMessageBox, QFileDialog, QFrame, QSplitter, QDateEdit
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QDate
from PyQt5.QtGui import QFont, QIcon, QPalette, QColor


class OutputRedirector:
    """重定向stdout到GUI和终端"""
    def __init__(self, text_widget, original_stdout):
        self.text_widget = text_widget
        self.original_stdout = original_stdout
        
    def write(self, text):
        # 写入到原始终端
        if self.original_stdout:
            self.original_stdout.write(text)
            self.original_stdout.flush()
        
        # 写入到GUI控件
        if self.text_widget and hasattr(self.text_widget, 'append'):
            # 移除末尾的换行符，因为append会自动添加
            clean_text = text.rstrip('\n\r')
            if clean_text:  # 只有非空文本才添加
                self.text_widget.append(f"[DEBUG] {clean_text}")
    
    def flush(self):
        if self.original_stdout:
            self.original_stdout.flush()

# 导入原有的功能模块
from src.utils.ollama_utils import (
    is_ollama_installed, 
    is_ollama_server_running, 
    get_locally_available_models,
    start_ollama_server,
    ensure_ollama_and_model
)
from src.tools.api import set_api_interrupt, clear_api_interrupt
from src.utils.html_report import generate_html_report
from src.utils.display import format_trading_output

# 导入核心分析模块
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langgraph.graph import END, StateGraph
from src.agents.portfolio_manager import portfolio_management_agent
from src.graph.state import AgentState
from src.utils.analysts import get_analyst_nodes
from src.utils.unified_data_accessor import unified_data_accessor
from src.utils.progress import progress
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Load environment variables from .env file
load_dotenv()


def parse_hedge_fund_response(response):
    """Parses a JSON string and returns a dictionary."""
    try:
        return json.loads(response)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}\nResponse: {repr(response)}")
        return None
    except TypeError as e:
        print(f"Invalid response type (expected string, got {type(response).__name__}): {e}")
        return None
    except Exception as e:
        print(f"Unexpected error while parsing response: {e}\nResponse: {repr(response)}")
        return None


##### Run the Hedge Fund #####
def run_hedge_fund(
    tickers: list[str],
    start_date: str,
    end_date: str,
    portfolio: dict,
    show_reasoning: bool = False,
    selected_analysts: list[str] = [],
    model_name: str = "gpt-4.1",
    model_provider: str = "OpenAI",
):
    # 检查是否在GUI环境中运行
    is_gui = 'tkinter' in sys.modules or 'PyQt5' in sys.modules
    
    # 只在非GUI环境中启动rich进度显示
    if not is_gui:
        progress.start()
    
    try:
        # Create a new workflow if analysts are customized
        if selected_analysts:
            workflow = create_workflow(selected_analysts)
            agent = workflow.compile()
        else:
            # 如果没有指定分析师，使用所有分析师
            workflow = create_workflow()
            agent = workflow.compile()

        final_state = agent.invoke(
            {
                "messages": [
                    HumanMessage(
                        content="Make trading decisions based on the provided data.",
                    )
                ],
                "data": {
                    "tickers": tickers,
                    "portfolio": portfolio,
                    "start_date": start_date,
                    "end_date": end_date,
                    "analyst_signals": {},
                    "unified_data_accessor": unified_data_accessor,  # 传递实际的unified_data_accessor对象
                },
                "metadata": {
                    "show_reasoning": show_reasoning,
                    "model_name": model_name,
                    "model_provider": model_provider,
                },
            },
        )
        
        # Parse the final portfolio manager response
        portfolio_decisions = parse_hedge_fund_response(final_state["messages"][-1].content)
        
        # Safety check: ensure we have valid decisions
        if portfolio_decisions is None:
            print("Warning: Failed to parse portfolio manager response, creating default decisions")
            portfolio_decisions = {ticker: {"action": "hold", "quantity": 0, "confidence": 50.0, "reasoning": "Error parsing portfolio manager response"} for ticker in tickers}

        return {
            "decisions": portfolio_decisions,
            "analyst_signals": final_state["data"]["analyst_signals"],
        }
    finally:
        # 只在非GUI环境中停止rich进度显示
        if not is_gui:
            progress.stop()


def start(state: AgentState):
    """Initialize the workflow with the input message and prefetch all data."""
    from src.utils.data_prefetch import data_prefetcher
    
    # 获取分析参数
    data = state["data"]
    tickers = data["tickers"]
    end_date = data["end_date"]
    start_date = data["start_date"]
    
    # 预获取所有分析师需要的数据
    prefetched_data = data_prefetcher.prefetch_all_data(tickers, end_date, start_date)
    
    # 将预获取的数据和数据预取器存储到状态中
    state["data"]["prefetched_data"] = prefetched_data
    state["data"]["data_prefetcher"] = data_prefetcher
    
    return state


def create_workflow(selected_analysts=None):
    """Create the workflow with selected analysts."""
    workflow = StateGraph(AgentState)
    workflow.add_node("start_node", start)

    # Get analyst nodes from the configuration
    analyst_nodes = get_analyst_nodes()

    # Default to all analysts if none selected
    if selected_analysts is None:
        selected_analysts = list(analyst_nodes.keys())
    else:
        # 过滤掉已删除或不存在的分析师
        selected_analysts = [analyst for analyst in selected_analysts if analyst in analyst_nodes]
        if not selected_analysts:
            # 如果过滤后没有有效分析师，使用默认配置
            selected_analysts = list(analyst_nodes.keys())
    
    # Add selected analyst nodes
    for analyst_key in selected_analysts:
        node_name, node_func = analyst_nodes[analyst_key]
        workflow.add_node(node_name, node_func)
        workflow.add_edge("start_node", node_name)

    # Add portfolio management
    workflow.add_node("portfolio_manager", portfolio_management_agent)

    # Connect selected analysts directly to portfolio management
    for analyst_key in selected_analysts:
        node_name = analyst_nodes[analyst_key][0]
        workflow.add_edge(node_name, "portfolio_manager")

    workflow.add_edge("portfolio_manager", END)

    workflow.set_entry_point("start_node")
    return workflow


class AnalysisWorker(QThread):
    """分析工作线程"""
    progress_updated = pyqtSignal(str)
    analysis_completed = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        
    def run(self):
        try:
            print(f"=== 开始分析任务 ===")
            print(f"配置信息: {self.config}")
            
            # 确保选择的模型可用
            print(f"正在检查模型: {self.config['model']}")
            if not ensure_ollama_and_model(self.config['model']):
                error_msg = f"无法准备模型 {self.config['model']}，请检查模型是否正确安装"
                print(f"ERROR: {error_msg}")
                self.error_occurred.emit(error_msg)
                return
            
            print("SUCCESS: 模型准备完成")
            self.progress_updated.emit("模型准备完成，开始分析...")
            
            # 准备参数
            portfolio = {
                "cash": float(self.config['initial_cash']),
                "margin_requirement": float(self.config['margin']),
                "positions": {}
            }
            print(f"投资组合配置: {portfolio}")
            
            # 设置进度更新处理器
            from src.utils.progress import progress
            
            def progress_handler(agent_name, ticker, status, analysis, timestamp):
                """处理进度更新"""
                progress_text = f"[{timestamp}] {agent_name}: {status}"
                if ticker:
                    progress_text += f" [{ticker}]"
                if analysis:
                    progress_text += f" - {analysis[:100]}{'...' if len(analysis) > 100 else ''}"
                print(f"PROGRESS: {progress_text}")
                self.progress_updated.emit(progress_text)
            
            # 注册进度处理器
            print("注册进度处理器")
            progress.register_handler(progress_handler)
            
            try:
                print("=== 开始运行hedge fund分析 ===")
                print(f"股票代码: {self.config['tickers']}")
                print(f"开始日期: {self.config['start_date']}")
                print(f"结束日期: {self.config['end_date']}")
                print(f"选择的分析师: {self.config['selected_analysts']}")
                print(f"模型: {self.config['model']}")
                
                # 运行分析
                result = run_hedge_fund(
                    tickers=self.config['tickers'],
                    start_date=self.config['start_date'],
                    end_date=self.config['end_date'],
                    portfolio=portfolio,
                    show_reasoning=self.config['show_reasoning'],
                    selected_analysts=self.config['selected_analysts'],
                    model_name=self.config['model'],
                    model_provider="OLLAMA"
                )
                
                print(f"=== 分析完成 ===")
                print(f"结果类型: {type(result)}")
                if isinstance(result, dict):
                    print(f"结果键: {list(result.keys())}")
                    if 'decisions' in result:
                        print(f"决策数量: {len(result['decisions'])}")
                else:
                    print(f"WARNING: 结果不是字典类型: {result}")
                
                self.analysis_completed.emit(result)
                
            finally:
                # 取消注册进度处理器
                print("取消注册进度处理器")
                progress.unregister_handler(progress_handler)
                
        except Exception as e:
            error_msg = f"分析过程中发生错误: {str(e)}"
            print(f"ERROR: {error_msg}")
            import traceback
            print(f"错误堆栈: {traceback.format_exc()}")
            self.error_occurred.emit(error_msg)


class AIHedgeFundGUI(QMainWindow):
    """AI基金大师GUI主窗口"""
    
    def __init__(self):
        super().__init__()
        self.config_file = "gui_config.json"
        self.current_html_content = None
        self.current_result_data = None
        self.analysis_worker = None
        self.analysis_start_time = None
        self.total_analysts = 0
        self.completed_analysts = 0
        self.original_stdout = sys.stdout
        self.output_redirector = None
        
        # 分析师配置 - 更新为实际可用的分析师
        self.analyst_configs = {
            "warren_buffett": "沃伦·巴菲特 - 价值投资大师",
            "charlie_munger": "查理·芒格 - 理性投资者", 
            "peter_lynch": "彼得·林奇 - 成长股猎手",
            "phil_fisher": "菲利普·费雪 - 成长投资先驱",
            "ben_graham": "本杰明·格雷厄姆 - 价值投资之父",
            "bill_ackman": "比尔·阿克曼 - 激进投资者",
            "cathie_wood": "凯茜·伍德 - 创新投资女王",
            "michael_burry": "迈克尔·伯里 - 逆向投资专家",
            "stanley_druckenmiller": "斯坦利·德鲁肯米勒 - 宏观交易大师",
            "rakesh_jhunjhunwala": "拉凯什·琼琼瓦拉 - 印度巴菲特",
            "technical_analyst": "技术面分析师 - 图表分析专家",
            "aswath_damodaran": "阿斯沃斯·达摩达兰 - 估值教授"
        }
        
        self.init_ui()
        self.load_config()
        self.check_ollama_status()
        
    def init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle("AI基金大师投资分析系统 v2.0 - 267278466@qq.com")
        self.setGeometry(100, 100, 1000, 700)
        
        # 设置窗口图标
        try:
            self.setWindowIcon(QIcon("mrcai.ico"))
        except Exception as e:
            print(f"设置图标失败: {e}")
        
        # 设置应用样式
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f0f0f0;
            }
            QTabWidget::pane {
                border: 1px solid #c0c0c0;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #e0e0e0;
                padding: 8px 16px;
                margin-right: 2px;
                border: 1px solid #c0c0c0;
                border-bottom: none;
            }
            QTabBar::tab:selected {
                background-color: white;
                border-bottom: 1px solid white;
            }
            QGroupBox {
                font-weight: bold;
                border: 2px solid #c0c0c0;
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QPushButton {
                background-color: #e0e0e0;
                border: 1px solid #c0c0c0;
                padding: 6px 12px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #d0d0d0;
            }
            QPushButton:pressed {
                background-color: #c0c0c0;
            }
            QPushButton:disabled {
                background-color: #f0f0f0;
                color: #808080;
            }
        """)
        
        # 创建中央窗口部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 创建主布局
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # 创建标题
        title_label = QLabel(" AI基金大师投资分析系统")
        title_label.setAlignment(Qt.AlignCenter)
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title_label.setFont(title_font)
        main_layout.addWidget(title_label)
        
        # 创建标签页
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)
        
        # 创建各个标签页
        self.create_analysts_tab()
        self.create_config_tab()
        self.create_run_tab()
        self.create_results_tab()
        
        # 创建底部按钮
        self.create_bottom_buttons(main_layout)
        
    def create_analysts_tab(self):
        """创建分析师选择标签页"""
        tab = QWidget()
        self.tab_widget.addTab(tab, "🧠 分析师")
        
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # 标题和统计
        title_layout = QHBoxLayout()
        title_label = QLabel("选择AI分析师")
        title_font = QFont()
        title_font.setPointSize(12)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_layout.addWidget(title_label)
        
        title_layout.addStretch()
        
        self.analysts_count_label = QLabel("已选择: 0/15")
        title_layout.addWidget(self.analysts_count_label)
        
        layout.addLayout(title_layout)
        
        # 快捷操作按钮
        button_layout = QHBoxLayout()
        
        select_all_btn = QPushButton("✅ 全选")
        select_all_btn.clicked.connect(self.select_all_analysts)
        button_layout.addWidget(select_all_btn)
        
        deselect_all_btn = QPushButton("❌ 全不选")
        deselect_all_btn.clicked.connect(self.deselect_all_analysts)
        button_layout.addWidget(deselect_all_btn)
        
        recommended_btn = QPushButton("⭐ 推荐配置")
        recommended_btn.clicked.connect(self.set_recommended_analysts)
        button_layout.addWidget(recommended_btn)
        
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        # 分析师选择区域
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setMinimumHeight(400)
        
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        # 第一行分组
        row1_group = QGroupBox("💼 投资大师")
        row1_layout = QGridLayout(row1_group)
        
        # 技术分析师分组
        tech_group = QGroupBox("📊 专业分析师")
        tech_layout = QGridLayout(tech_group)
        
        # 创建分析师复选框
        self.analyst_checkboxes = {}
        
        # 投资大师（前10个）
        master_analysts = list(self.analyst_configs.items())[:10]
        for i, (key, name) in enumerate(master_analysts):
            checkbox = QCheckBox(name)
            checkbox.setChecked(True)  # 默认选中
            checkbox.stateChanged.connect(self.update_analysts_count)
            self.analyst_checkboxes[key] = checkbox
            row1_layout.addWidget(checkbox, i // 2, i % 2)
        
        # 专业分析师（后5个）
        tech_analysts = list(self.analyst_configs.items())[10:]
        for i, (key, name) in enumerate(tech_analysts):
            checkbox = QCheckBox(name)
            checkbox.setChecked(True)  # 默认选中
            checkbox.stateChanged.connect(self.update_analysts_count)
            self.analyst_checkboxes[key] = checkbox
            tech_layout.addWidget(checkbox, i // 2, i % 2)
        
        scroll_layout.addWidget(row1_group)
        scroll_layout.addWidget(tech_group)
        scroll_layout.addStretch()
        
        scroll_area.setWidget(scroll_widget)
        layout.addWidget(scroll_area)
        
    def create_config_tab(self):
        """创建配置标签页"""
        tab = QWidget()
        self.tab_widget.addTab(tab, "⚙️ 配置")
        
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # Ollama模型配置
        ollama_group = QGroupBox("🤖 Ollama模型配置")
        ollama_layout = QVBoxLayout(ollama_group)
        
        # Ollama状态
        self.ollama_status_label = QLabel("正在检查Ollama状态...")
        ollama_layout.addWidget(self.ollama_status_label)
        
        # Ollama按钮
        ollama_btn_layout = QHBoxLayout()
        
        check_status_btn = QPushButton("🔄 检查状态")
        check_status_btn.clicked.connect(self.check_ollama_status)
        ollama_btn_layout.addWidget(check_status_btn)
        
        start_ollama_btn = QPushButton("▶️ 启动Ollama")
        start_ollama_btn.clicked.connect(self.start_ollama)
        ollama_btn_layout.addWidget(start_ollama_btn)
        
        ollama_btn_layout.addStretch()
        ollama_layout.addLayout(ollama_btn_layout)
        
        # 模型选择
        model_layout = QHBoxLayout()
        model_layout.addWidget(QLabel("选择模型:"))
        
        self.model_combo = QComboBox()
        self.model_combo.setMinimumWidth(200)
        model_layout.addWidget(self.model_combo)
        model_layout.addStretch()
        
        ollama_layout.addLayout(model_layout)
        layout.addWidget(ollama_group)
        
        # 交易参数
        trading_group = QGroupBox("💰 交易参数")
        trading_layout = QGridLayout(trading_group)
        
        # 股票代码
        trading_layout.addWidget(QLabel("股票代码:"), 0, 0)
        self.tickers_edit = QLineEdit("AAPL,GOOGL,MSFT,TSLA,AMZN")
        self.tickers_edit.setPlaceholderText("输入股票代码，用逗号分隔")
        trading_layout.addWidget(self.tickers_edit, 0, 1, 1, 2)
        
        # 日期范围
        trading_layout.addWidget(QLabel("开始日期:"), 1, 0)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setDate(QDate.currentDate().addMonths(-3))
        self.start_date_edit.setCalendarPopup(True)
        trading_layout.addWidget(self.start_date_edit, 1, 1)
        
        trading_layout.addWidget(QLabel("结束日期:"), 1, 2)
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setDate(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)
        trading_layout.addWidget(self.end_date_edit, 1, 3)
        
        # 资金配置
        trading_layout.addWidget(QLabel("初始资金:"), 2, 0)
        self.initial_cash_edit = QLineEdit("100000.0")
        trading_layout.addWidget(self.initial_cash_edit, 2, 1)
        
        trading_layout.addWidget(QLabel("保证金要求:"), 2, 2)
        self.margin_edit = QLineEdit("0.0")
        trading_layout.addWidget(self.margin_edit, 2, 3)
        
        layout.addWidget(trading_group)
        
        # 分析选项
        options_group = QGroupBox("🔧 分析选项")
        options_layout = QVBoxLayout(options_group)
        
        self.show_reasoning_checkbox = QCheckBox("显示详细分析推理过程")
        self.show_reasoning_checkbox.setChecked(True)
        options_layout.addWidget(self.show_reasoning_checkbox)
        
        layout.addWidget(options_group)
        
        layout.addStretch()
        
    def create_run_tab(self):
        """创建运行标签页"""
        tab = QWidget()
        self.tab_widget.addTab(tab, "▶️ 运行")
        
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # 分析控制台
        control_group = QGroupBox("🎮 分析控制台")
        control_layout = QVBoxLayout(control_group)
        
        # 按钮区域
        button_layout = QHBoxLayout()
        
        self.run_button = QPushButton("▶️ 开始分析")
        self.run_button.clicked.connect(self.run_analysis)
        button_layout.addWidget(self.run_button)
        
        self.stop_button = QPushButton("⏹️ 停止分析")
        self.stop_button.clicked.connect(self.stop_analysis)
        self.stop_button.setEnabled(False)
        button_layout.addWidget(self.stop_button)
        
        button_layout.addStretch()
        
        # 状态信息
        status_layout = QHBoxLayout()
        status_layout.addWidget(QLabel("分析状态:"))
        
        self.status_label = QLabel("准备就绪")
        self.status_label.setStyleSheet("font-weight: bold;")
        status_layout.addWidget(self.status_label)
        status_layout.addStretch()
        
        button_layout.addLayout(status_layout)
        control_layout.addLayout(button_layout)
        
        # 进度条
        progress_layout = QHBoxLayout()
        progress_layout.addWidget(QLabel("进度:"))
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        progress_layout.addWidget(self.progress_bar)
        
        control_layout.addLayout(progress_layout)
        layout.addWidget(control_group)
        
        # 输出区域
        output_group = QGroupBox("📊 实时分析日志")
        output_layout = QVBoxLayout(output_group)
        
        self.output_text = QTextEdit()
        # 设置等宽字体，添加回退选项和错误处理
        try:
            output_font = QFont()
            output_font.setFamily("Consolas")
            output_font.setPointSize(9)
            # 如果Consolas不可用，Qt会自动选择系统默认等宽字体
            output_font.setStyleHint(QFont.Monospace)
            self.output_text.setFont(output_font)
        except Exception as e:
            print(f"设置输出文本字体失败: {e}")
            # 使用系统默认字体
            self.output_text.setFont(QFont())
        self.output_text.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #d4d4d4;
                border: 1px solid #c0c0c0;
            }
        """)
        output_layout.addWidget(self.output_text)
        
        # 设置stdout重定向，将print输出同时显示在GUI和终端
        self.output_redirector = OutputRedirector(self.output_text, self.original_stdout)
        sys.stdout = self.output_redirector
        
        # 添加调试信息
        print("=== AI基金大师调试模式已启用 ===")
        print(f"GUI初始化完成，时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("所有print输出将同时显示在终端和GUI中")
        
        layout.addWidget(output_group)
        
    def create_results_tab(self):
        """创建结果标签页"""
        tab = QWidget()
        self.tab_widget.addTab(tab, "📊 结果")
        
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # 结果控制区域
        control_layout = QHBoxLayout()
        
        browser_btn = QPushButton("🌐 浏览器查看")
        browser_btn.clicked.connect(self.open_html_in_browser)
        control_layout.addWidget(browser_btn)
        
        save_btn = QPushButton("💾 保存报告")
        save_btn.clicked.connect(self.save_results)
        control_layout.addWidget(save_btn)
        
        save_html_btn = QPushButton("📄 保存HTML")
        save_html_btn.clicked.connect(self.save_html_report)
        control_layout.addWidget(save_html_btn)
        
        clear_btn = QPushButton("🗑️ 清空")
        clear_btn.clicked.connect(self.clear_results)
        control_layout.addWidget(clear_btn)
        
        control_layout.addStretch()
        layout.addLayout(control_layout)
        
        # 结果显示区域
        results_tab_widget = QTabWidget()
        
        # HTML报告标签页
        html_tab = QWidget()
        results_tab_widget.addTab(html_tab, "📊 精美报告")
        
        html_layout = QVBoxLayout(html_tab)
        html_group = QGroupBox("分析报告预览")
        html_group_layout = QVBoxLayout(html_group)
        
        self.html_preview = QTextEdit()
        # 设置中文字体，添加回退选项和错误处理
        try:
            preview_font = QFont()
            preview_font.setFamily("Microsoft YaHei")
            preview_font.setPointSize(10)
            # 如果Microsoft YaHei不可用，Qt会自动选择系统默认字体
            preview_font.setStyleHint(QFont.SansSerif)
            self.html_preview.setFont(preview_font)
        except Exception as e:
            print(f"设置HTML预览字体失败: {e}")
            # 使用系统默认字体
            self.html_preview.setFont(QFont())
        self.html_preview.setStyleSheet("""
            QTextEdit {
                background-color: #f8f9fa;
                border: none;
            }
        """)
        html_group_layout.addWidget(self.html_preview)
        html_layout.addWidget(html_group)
        
        # 原始数据标签页
        raw_tab = QWidget()
        results_tab_widget.addTab(raw_tab, "📋 详细数据")
        
        raw_layout = QVBoxLayout(raw_tab)
        raw_group = QGroupBox("原始分析数据")
        raw_group_layout = QVBoxLayout(raw_group)
        
        self.results_text = QTextEdit()
        # 设置等宽字体，添加回退选项和错误处理
        try:
            results_font = QFont()
            results_font.setFamily("Consolas")
            results_font.setPointSize(9)
            # 如果Consolas不可用，Qt会自动选择系统默认等宽字体
            results_font.setStyleHint(QFont.Monospace)
            self.results_text.setFont(results_font)
        except Exception as e:
            print(f"设置结果文本字体失败: {e}")
            # 使用系统默认字体
            self.results_text.setFont(QFont())
        raw_group_layout.addWidget(self.results_text)
        raw_layout.addWidget(raw_group)
        
        layout.addWidget(results_tab_widget)
        
    def create_bottom_buttons(self, main_layout):
        """创建底部按钮区域"""
        bottom_layout = QHBoxLayout()
        
        # 左下角添加GitHub链接
        github_label = QLabel('<a href="https://github.com/hengruiyun" style="color: #0066cc; text-decoration: none;">HengruiYun</a>')
        github_label.setOpenExternalLinks(True)
        github_label.setStyleSheet("QLabel { font-size: 12px; color: #666; }")
        bottom_layout.addWidget(github_label)
        
        bottom_layout.addStretch()
        
        # 开始分析按钮
        self.bottom_run_button = QPushButton("开始分析")
        self.bottom_run_button.clicked.connect(self.run_analysis)
        bottom_layout.addWidget(self.bottom_run_button)
        
        # 退出按钮
        exit_button = QPushButton("退出")
        exit_button.clicked.connect(self.close)
        bottom_layout.addWidget(exit_button)
        
        main_layout.addLayout(bottom_layout)
        
    def check_ollama_status(self):
        """检查Ollama状态"""
        print("正在检查Ollama状态...")
        try:
            installed = is_ollama_installed()
            print(f"Ollama安装状态: {installed}")
            running = is_ollama_server_running() if installed else False
            print(f"Ollama运行状态: {running}")
            models = get_locally_available_models() if running else []
            print(f"可用模型数量: {len(models)}")
            
            self.update_ollama_status(installed, running, models)
        except Exception as e:
            print(f"检查Ollama状态时出错: {e}")
            self.update_ollama_status(False, False, [], str(e))
            
    def update_ollama_status(self, installed, running, models, error=None):
        """更新Ollama状态显示"""
        if error:
            status_text = f"Ollama状态检查失败: {error}"
        elif not installed:
            status_text = "Ollama未安装 - 请先安装Ollama"
        elif not running:
            status_text = "Ollama已安装但未运行"
        else:
            status_text = f"Ollama正在运行 - 可用模型: {len(models)}个"
            
        self.ollama_status_label.setText(status_text)
        
        # 更新模型选择框
        self.model_combo.clear()
        if models:
            self.model_combo.addItems(models)
            
    def start_ollama(self):
        """启动Ollama服务"""
        try:
            print("正在启动Ollama服务...")
            self.ollama_status_label.setText("正在启动Ollama服务...")
            success = start_ollama_server()
            if success:
                print("SUCCESS: Ollama服务启动成功")
                self.ollama_status_label.setText("Ollama服务启动成功")
                QTimer.singleShot(2000, self.check_ollama_status)
            else:
                print("ERROR: Ollama服务启动失败")
                self.ollama_status_label.setText("Ollama服务启动失败")
        except Exception as e:
            print(f"ERROR: 启动Ollama服务时出错: {e}")
            self.ollama_status_label.setText(f"启动失败: {str(e)}")
            
    def select_all_analysts(self):
        """全选分析师"""
        for checkbox in self.analyst_checkboxes.values():
            checkbox.setChecked(True)
        self.update_analysts_count()
        
    def deselect_all_analysts(self):
        """取消全选分析师"""
        for checkbox in self.analyst_checkboxes.values():
            checkbox.setChecked(False)
        self.update_analysts_count()
        
    def set_recommended_analysts(self):
        """设置推荐的分析师配置"""
        recommended = {
            "warren_buffett": True,
            "charlie_munger": True, 
            "peter_lynch": True,
            "michael_burry": True,
            "aswath_damodaran": True,
            "technical_analyst": True,
            "phil_fisher": False,
            "ben_graham": False,
            "bill_ackman": False,
            "cathie_wood": False,
            "stanley_druckenmiller": False,
            "rakesh_jhunjhunwala": False
        }
        
        for key, value in recommended.items():
            if key in self.analyst_checkboxes:
                self.analyst_checkboxes[key].setChecked(value)
        self.update_analysts_count()
        
    def update_analysts_count(self):
        """更新分析师选择计数"""
        selected = sum(1 for checkbox in self.analyst_checkboxes.values() if checkbox.isChecked())
        total = len(self.analyst_checkboxes)
        self.analysts_count_label.setText(f"已选择: {selected}/{total}")
        
    def get_selected_analysts(self):
        """获取选中的分析师"""
        return [key for key, checkbox in self.analyst_checkboxes.items() if checkbox.isChecked()]
        
    def run_analysis(self):
        """运行AI基金大师分析"""
        print("=== 用户点击开始分析 ===")
        
        # 验证输入
        model_name = self.model_combo.currentText()
        print(f"选择的模型: '{model_name}'")
        if not model_name:
            print("ERROR: 未选择模型")
            QMessageBox.critical(self, "错误", "请先选择一个大模型")
            return
            
        selected_analysts = self.get_selected_analysts()
        print(f"选中的分析师: {selected_analysts}")
        if not selected_analysts:
            print("ERROR: 未选择分析师")
            QMessageBox.critical(self, "错误", "请至少选择一个AI分析师")
            return
            
        # 解析股票代码
        import re
        tickers_input = self.tickers_edit.text()
        print(f"输入的股票代码: '{tickers_input}'")
        tickers = [t.strip().upper() for t in re.split(r'[,;\s\t\n]+', tickers_input) if t.strip()]
        print(f"解析后的股票代码: {tickers}")
        if not tickers:
            print("ERROR: 股票代码解析失败")
            QMessageBox.critical(self, "错误", "请输入至少一个股票代码")
            return
            
        # 检查股票数量限制
        if len(tickers) > 4:
            print(f"WARNING: 股票数量过多 ({len(tickers)} > 4)")
            QMessageBox.warning(self, "股票数量限制", f"股票数量过多，最多支持4支股票。\n当前输入了{len(tickers)}支股票，请减少股票数量。")
            return
            
        print("开始准备分析环境...")
        
        # 清空数据缓存和上次分析内容
        print("清空数据缓存和上次分析内容")
        self.clear_analysis_cache()
        
        # 切换到运行标签页
        print("切换到运行标签页")
        self.tab_widget.setCurrentIndex(2)  # 运行标签页是第3个（索引为2）
        
        # 禁用运行按钮，启用停止按钮
        print("更新UI状态")
        self.run_button.setEnabled(False)
        self.bottom_run_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.progress_bar.setValue(0)
        self.status_label.setText("正在运行...")
        
        # 清空输出
        self.output_text.clear()
        
        # 清除API中断标志
        print("清除API中断标志")
        clear_api_interrupt()
        
        # 重置进度计数器
        self.analysis_start_time = time.time()
        self.total_analysts = len(selected_analysts)
        self.completed_analysts = 0
        print(f"分析师总数: {self.total_analysts}")
        
        # 准备配置
        config = {
            'model': self.model_combo.currentText(),
            'tickers': tickers,
            'start_date': self.start_date_edit.date().toString("yyyy-MM-dd"),
            'end_date': self.end_date_edit.date().toString("yyyy-MM-dd"),
            'initial_cash': self.initial_cash_edit.text(),
            'margin': self.margin_edit.text(),
            'show_reasoning': self.show_reasoning_checkbox.isChecked(),
            'selected_analysts': selected_analysts
        }
        print(f"分析配置: {config}")
        
        # 启动分析工作线程
        print("创建并启动分析工作线程")
        self.analysis_worker = AnalysisWorker(config)
        self.analysis_worker.progress_updated.connect(self.update_progress)
        self.analysis_worker.analysis_completed.connect(self.show_results)
        self.analysis_worker.error_occurred.connect(self.show_error)
        self.analysis_worker.finished.connect(self.analysis_finished)
        self.analysis_worker.start()
        print("分析工作线程已启动")
        
    def stop_analysis(self):
        """停止分析"""
        set_api_interrupt()
        self.status_label.setText("正在停止...")
        
        if self.analysis_worker and self.analysis_worker.isRunning():
            self.analysis_worker.terminate()
            self.analysis_worker.wait(2000)  # 等待最多2秒
            
        self.analysis_finished()
        
    def analysis_finished(self):
        """分析完成后的清理工作"""
        self.run_button.setEnabled(True)
        self.bottom_run_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        
        # 设置进度条为100%
        self.progress_bar.setValue(100)
        
        # 显示最终完成状态
        if hasattr(self, 'analysis_start_time') and self.analysis_start_time:
            total_time = time.time() - self.analysis_start_time
            if total_time < 60:
                time_str = f"{total_time:.0f}秒"
            elif total_time < 3600:
                time_str = f"{total_time/60:.1f}分钟"
            else:
                time_str = f"{total_time/3600:.1f}小时"
            
            self.status_label.setText(f"分析完成 - 总耗时: {time_str}")
        else:
            self.status_label.setText("分析完成")
            
    def update_progress(self, message):
        """更新进度显示"""
        self.output_text.append(message)
        
        # 更新进度百分比
        if "Done" in message:
            self.completed_analysts += 1
            
        if self.total_analysts > 0:
            progress_percent = min(100, int((self.completed_analysts / self.total_analysts) * 100))
            self.progress_bar.setValue(progress_percent)
            
            if hasattr(self, 'analysis_start_time') and self.analysis_start_time:
                elapsed_time = time.time() - self.analysis_start_time
                if elapsed_time < 60:
                    time_str = f"{elapsed_time:.0f}秒"
                elif elapsed_time < 3600:
                    time_str = f"{elapsed_time/60:.1f}分钟"
                else:
                    time_str = f"{elapsed_time/3600:.1f}小时"
                
                self.status_label.setText(f"分析进行中... {progress_percent}% ({self.completed_analysts}/{self.total_analysts}) - {time_str}")
            
    def show_results(self, result):
        """显示分析结果"""
        self.tab_widget.setCurrentIndex(3)  # 切换到结果标签页
        
        # 添加类型检查和错误处理
        if not isinstance(result, dict):
            error_msg = f"接收到无效的结果类型: {type(result).__name__}，期望字典类型。结果内容: {str(result)[:200]}..."
            self.html_preview.setPlainText(f"❌ 结果类型错误: {error_msg}")
            self.results_text.setPlainText(f"❌ 结果类型错误: {error_msg}")
            print(f"ERROR: {error_msg}")
            return
        
        # 存储结果数据
        self.current_result_data = result
        
        # 生成HTML报告
        try:
            self.current_html_content = generate_html_report(result)
            # 显示HTML报告的文本预览版本
            html_preview_text = self.convert_html_to_preview_text(result)
            self.html_preview.setPlainText(html_preview_text)
        except Exception as e:
            self.html_preview.setPlainText(f"HTML报告生成失败: {str(e)}")
        
        # 格式化并显示原始结果数据
        try:
            formatted_result = format_trading_output(result)
            self.results_text.setPlainText(formatted_result)
        except Exception as e:
            self.results_text.setPlainText(f"结果格式化失败: {str(e)}")
            
        # 添加完成提示
        QMessageBox.information(
            self, "✅ 分析完成", 
            "🎉 投资分析已成功完成！\n\n" +
            "📊 请查看'分析结果'标签页获取详细报告\n" +
            "🌐 点击'浏览器查看'按钮可查看完整HTML报告\n" +
            "💾 可使用'保存结果'按钮保存分析报告"
        )
            
    def convert_html_to_preview_text(self, result):
        """将分析结果转换为可在Text控件中显示的预览文本"""
        if not result:
            return "❌ 没有可用的分析结果"
        
        preview_text = " AI基金大师投资分析报告\n"
        preview_text += "=" * 50 + "\n\n"
        
        # 生成时间
        current_time = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")
        preview_text += f" 作者:267278466@qq.com \n"
        preview_text += f"📅 生成时间: {current_time}\n\n"
        
        # 执行摘要
        decisions = result.get("decisions", {})
        if decisions:
            preview_text += "📋 执行摘要\n"
            preview_text += "-" * 30 + "\n"
            
            # 统计决策分布
            action_counts = {"buy": 0, "sell": 0, "hold": 0, "short": 0, "cover": 0}
            total_confidence = 0
            total_decisions = len(decisions)
            
            for decision in decisions.values():
                action = decision.get("action", "hold").lower()
                if action in action_counts:
                    action_counts[action] += 1
                confidence = decision.get("confidence", 0)
                total_confidence += confidence
            
            avg_confidence = total_confidence / total_decisions if total_decisions > 0 else 0
            
            preview_text += f"📊 分析股票数量: {total_decisions}\n"
            preview_text += f"📈 买入建议: {action_counts['buy']}\n"
            preview_text += f"📉 卖出建议: {action_counts['sell']}\n"
            preview_text += f"⏸️  持有建议: {action_counts['hold']}\n"
            preview_text += f"🎯 平均信心度: {avg_confidence:.1f}%\n\n"
        
        # 投资决策详情
        if decisions:
            preview_text += "💰 投资决策详情\n"
            preview_text += "-" * 30 + "\n"
            
            for ticker, decision in decisions.items():
                action = decision.get("action", "hold").lower()
                quantity = decision.get("quantity", 0)
                confidence = decision.get("confidence", 0)
                reasoning = decision.get("reasoning", "无详细说明")
                
                # 获取动作的中文描述
                action_map = {
                    "buy": "买入",
                    "sell": "卖出", 
                    "hold": "持有",
                    "short": "做空",
                    "cover": "平仓"
                }
                action_text = action_map.get(action, action)
                
                preview_text += f"\n📈 {ticker} - {action_text}\n"
                preview_text += f"   交易数量: {quantity:,} 股\n"
                preview_text += f"   信心度: {confidence:.1f}%\n"
                preview_text += f"   分析理由: {reasoning[:100]}{'...' if len(reasoning) > 100 else ''}\n"
        
        preview_text += "\n" + "=" * 50 + "\n"
        preview_text += "⚠️ 风险提示: 本报告为编程生成的模拟样本，不能作为真实使用，不构成投资建议。\n"
        preview_text += "投资有风险，决策需谨慎。请根据自身情况做出投资决定。\n"
        preview_text += "\n💡 完整的精美HTML报告请点击 '🌐 浏览器查看' 按钮。\n"
        
        return preview_text
        
    def show_error(self, error_msg):
        """显示错误信息"""
        QMessageBox.critical(self, "运行错误", f"分析过程中出现错误:\n\n{error_msg}")
        self.output_text.append(f"\n\n错误: {error_msg}\n")
        
    def save_results(self):
        """保存结果到文件"""
        if not self.results_text.toPlainText().strip():
            QMessageBox.warning(self, "警告", "没有结果可保存")
            return
            
        filename, _ = QFileDialog.getSaveFileName(
            self, "保存结果", "", "文本文件 (*.txt);;所有文件 (*.*)"
        )
        
        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(self.results_text.toPlainText())
                QMessageBox.information(self, "成功", f"结果已保存到: {filename}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"保存失败: {str(e)}")
                
    def clear_results(self):
        """清空结果"""
        self.results_text.clear()
        self.html_preview.clear()
        self.current_html_content = None
        self.current_result_data = None
        
    def clear_analysis_cache(self):
        """清空数据缓存和上次分析的内容"""
        # 清空结果显示
        self.clear_results()
        
        # 清空运行日志
        if hasattr(self, 'output_text'):
            self.output_text.clear()
        
        # 重置进度条和状态
        if hasattr(self, 'progress_bar'):
            self.progress_bar.setValue(0)
        if hasattr(self, 'status_label'):
            self.status_label.setText("准备开始分析...")
        
        # 清空数据预取缓存
        try:
            from src.utils.data_prefetch import data_prefetcher
            data_prefetcher.clear_cache()
        except Exception as e:
            print(f"清空数据预取缓存失败: {e}")
        
        # 清空API缓存
        try:
            from src.tools.api import clear_cache
            clear_cache()
        except Exception as e:
            print(f"清空API缓存失败: {e}")
        
        # 清空进度跟踪
        try:
            from src.utils.progress import progress
            progress.clear_all_status()
        except Exception as e:
            print(f"清空进度跟踪失败: {e}")
        
        # 重置分析计数器
        self.analysis_start_time = None
        self.total_analysts = 0
        self.completed_analysts = 0
        
        print("✅ 数据缓存和上次分析内容已清空")
        
    def open_html_in_browser(self):
        """在浏览器中打开HTML报告"""
        if not self.current_html_content:
            QMessageBox.warning(self, "警告", "没有可用的HTML报告")
            return
        
        try:
            # 创建临时HTML文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                f.write(self.current_html_content)
                temp_file = f.name
            
            # 在浏览器中打开
            webbrowser.open(f'file://{temp_file}')
            
            # 延迟删除临时文件
            def cleanup():
                try:
                    os.unlink(temp_file)
                except:
                    pass
            
            QTimer.singleShot(5000, cleanup)  # 5秒后删除临时文件
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法在浏览器中打开HTML报告: {str(e)}")
            
    def save_html_report(self):
        """保存HTML报告到文件"""
        if not self.current_html_content:
            QMessageBox.warning(self, "警告", "没有可用的HTML报告")
            return
        
        default_filename = f"AI基金大师分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        filename, _ = QFileDialog.getSaveFileName(
            self, "保存HTML报告", default_filename, "HTML文件 (*.html);;所有文件 (*.*)"
        )
        
        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(self.current_html_content)
                QMessageBox.information(self, "成功", f"HTML报告已保存到: {filename}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"保存失败: {str(e)}")
                
    def save_config(self):
        """保存当前配置到文件"""
        try:
            config = {
                "model": self.model_combo.currentText(),
                "selected_analysts": {key: checkbox.isChecked() for key, checkbox in self.analyst_checkboxes.items()},
                "tickers": self.tickers_edit.text(),
                "initial_cash": self.initial_cash_edit.text(),
                "margin": self.margin_edit.text(),
                "show_reasoning": self.show_reasoning_checkbox.isChecked(),
                "window_geometry": f"{self.width()}x{self.height()}+{self.x()}+{self.y()}"
            }
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"保存配置失败: {e}")
            
    def load_config(self):
        """从文件加载配置"""
        try:
            if not os.path.exists(self.config_file):
                return
                
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # 恢复窗口位置和大小
            if "window_geometry" in config:
                try:
                    geometry = config["window_geometry"]
                    if '+' in geometry:
                        parts = geometry.split('+')
                        if len(parts) >= 3:
                            x_pos = int(parts[1])
                            y_pos = int(parts[2])
                            self.move(x_pos, y_pos)
                except:
                    pass
            
            # 恢复分析师选择，但过滤掉已删除的分析师
            if "selected_analysts" in config:
                # 确保config["selected_analysts"]是字典格式
                selected_analysts = config["selected_analysts"]
                if isinstance(selected_analysts, list):
                    # 兼容旧格式：列表转换为字典
                    selected_analysts = {analyst: True for analyst in selected_analysts}
                elif not isinstance(selected_analysts, dict):
                    # 其他格式，重置为空
                    selected_analysts = {}
                
                # 只恢复仍然存在的分析师配置
                for key, value in selected_analysts.items():
                    if key in self.analyst_checkboxes:  # 只处理仍然存在的分析师
                        self.analyst_checkboxes[key].setChecked(value)
                self.update_analysts_count()
            
            # 恢复其他配置
            if "tickers" in config:
                self.tickers_edit.setText(config["tickers"])
            if "initial_cash" in config:
                self.initial_cash_edit.setText(config["initial_cash"])
            if "margin" in config:
                self.margin_edit.setText(config["margin"])
            if "show_reasoning" in config:
                self.show_reasoning_checkbox.setChecked(config["show_reasoning"])
                
        except Exception as e:
            print(f"加载配置失败: {e}")
            # 配置加载失败时，设置默认推荐配置
            self.set_recommended_analysts()
            
    def closeEvent(self, event):
        """关闭程序时的处理"""
        # 如果分析正在运行，询问用户
        if self.analysis_worker and self.analysis_worker.isRunning():
            reply = QMessageBox.question(
                self, '确认退出', 
                '分析正在进行中，确定要退出吗？\n\n退出将丢失当前分析进度。',
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.No:
                event.ignore()
                return
            
            # 停止分析
            set_api_interrupt()
            self.analysis_worker.terminate()
            self.analysis_worker.wait(3000)
        
        # 恢复原始stdout
        if self.output_redirector and self.original_stdout:
            sys.stdout = self.original_stdout
            print("GUI关闭，stdout已恢复")
        
        self.save_config()  # 保存配置
        event.accept()


def main():
    """主函数"""
    # 设置Qt应用属性，必须在QApplication创建之前设置
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    QApplication.setAttribute(Qt.AA_DisableWindowContextHelpButton, True)
    QApplication.setAttribute(Qt.AA_DontCreateNativeWidgetSiblings, True)
    
    app = QApplication(sys.argv)
    
    # 设置应用信息
    app.setApplicationName("AI基金大师投资分析系统")
    app.setApplicationVersion("2.0.0")
    
    # 设置默认字体
    try:
        default_font = QFont()
        default_font.setFamily("Microsoft YaHei")
        default_font.setPointSize(9)
        default_font.setStyleHint(QFont.SansSerif)
        app.setFont(default_font)
    except Exception as e:
        print(f"设置默认字体失败: {e}")
    
    try:
        window = AIHedgeFundGUI()
        window.show()
        sys.exit(app.exec_())
    except Exception as e:
        print(f"GUI启动失败: {e}")
        import traceback
        print(f"详细错误信息: {traceback.format_exc()}")
        try:
            QMessageBox.critical(None, "启动错误", f"GUI启动失败:\n{e}")
        except:
            print("无法显示错误对话框")


if __name__ == "__main__":
    main()