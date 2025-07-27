#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI基金大师GUI界面 - Tkinter版本
使用Tkinter重新实现的界面
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

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
from tkinter import font as tkFont
from tkcalendar import DateEntry


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
        if self.text_widget:
            # 移除末尾的换行符，因为insert会保持原格式
            clean_text = text.rstrip('\n\r')
            if clean_text:  # 只有非空文本才添加
                self.text_widget.insert(tk.END, f"[DEBUG] {clean_text}\n")
                self.text_widget.see(tk.END)
    
    def flush(self):
        if self.original_stdout:
            self.original_stdout.flush()

# 导入原有的功能模块
from src.utils.ollama_utils import (
    get_locally_available_models,
    ensure_ollama_and_model
)
from check_ollama_env import OllamaChecker
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


##### Run the AI Fund Master #####
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
    is_gui = 'tkinter' in sys.modules
    
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
            portfolio_decisions = {ticker: {"action": "hold", "quantity": 0, "confidence": 50.0, "reasoning": "投资组合管理器解析失败，采用默认持有策略"} for ticker in tickers}

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
    from src.utils.unified_data_accessor import unified_data_accessor
    
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
    
    # 添加统一数据访问器到状态中
    state["data"]["unified_data_accessor"] = unified_data_accessor
    
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


class AnalysisWorker(threading.Thread):
    """分析工作线程"""
    
    def __init__(self, config, progress_callback, completed_callback, error_callback):
        super().__init__()
        self.config = config
        self.progress_callback = progress_callback
        self.completed_callback = completed_callback
        self.error_callback = error_callback
        self.daemon = True
        
    def run(self):
        try:
            print(f"=== 开始分析任务 ===")
            print(f"配置信息: {self.config}")
            
            # 确保选择的模型可用
            print(f"正在检查模型: {self.config['model']}")
            #if not ensure_ollama_and_model(self.config['model']):
            #    error_msg = f"无法准备模型 {self.config['model']}，请检查模型是否正确安装"
            #    print(f"ERROR: {error_msg}")
            #    self.error_callback(error_msg)
            #    return
            
            #print("SUCCESS: 模型准备完成")
            #self.progress_callback("模型准备完成，开始分析...")
            
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
                self.progress_callback(progress_text)
            
            # 注册进度处理器
            print("注册进度处理器")
            progress.register_handler(progress_handler)
            
            try:
                print("=== 开始运行AI Fund Master分析 ===")
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
                
                self.completed_callback(result)
                
            finally:
                # 取消注册进度处理器
                print("取消注册进度处理器")
                progress.unregister_handler(progress_handler)
                
        except Exception as e:
            error_msg = f"分析过程中发生错误: {str(e)}"
            print(f"ERROR: {error_msg}")
            import traceback
            print(f"错误堆栈: {traceback.format_exc()}")
            self.error_callback(error_msg)


class AIHedgeFundGUI:
    """AI基金大师GUI主窗口"""
    
    def __init__(self):
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
        self.root = tk.Tk()
        self.root.title("AI基金大师 v2.0 - 267278466@qq.com")
        
        # 设置窗口大小和位置（居中显示）
        window_width = 800
        window_height = 540  # 增加窗口高度以容纳固定高度的notebook
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
        self.root.minsize(800, 540)  # 设置最小尺寸
        
        # 设置窗口图标
        try:
            self.root.iconbitmap("mrcai.ico")
        except Exception as e:
            print(f"设置图标失败: {e}")
        
        # 创建主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 创建标题
        title_label = ttk.Label(main_frame, text="AI基金大师", 
                               font=("Arial", 12, "bold"))
        title_label.pack(pady=(0, 5))
        
        # 创建标签页容器框架，设置固定高度
        notebook_container = ttk.Frame(main_frame)
        notebook_container.pack(fill=tk.BOTH, expand=True)
        
        # 创建标签页，设置固定高度500
        self.notebook = ttk.Notebook(notebook_container)
        self.notebook.pack(fill=tk.BOTH, expand=False, pady=(0, 10))  # 底部留出空间给按钮
        
        # 创建各个标签页
        self.create_analysts_tab()
        self.create_config_tab()
        self.create_run_tab()
        self.create_results_tab()
        
        # 创建底部按钮区域
        self.create_bottom_buttons(main_frame)
        
    def create_analysts_tab(self):
        """创建分析师选择标签页"""
        tab_frame = ttk.Frame(self.notebook)
        self.notebook.add(tab_frame, text="分析师")
        
        # 主容器 - 设置固定高度
        main_container = ttk.Frame(tab_frame, height=400)
        main_container.pack(fill=tk.BOTH, expand=False, padx=8, pady=8)
        main_container.pack_propagate(False)  # 防止子组件改变父组件大小
        
        # 标题和统计
        title_frame = ttk.Frame(main_container)
        title_frame.pack(fill=tk.X, pady=(0, 10))
        
        title_label = ttk.Label(title_frame, text="选择AI分析师", 
                               font=("Arial", 12, "bold"))
        title_label.pack(side=tk.LEFT)
        
        self.analysts_count_label = ttk.Label(title_frame, text="已选择: 0/15")
        self.analysts_count_label.pack(side=tk.RIGHT)
        
        # 快捷操作按钮
        button_frame = ttk.Frame(main_container)
        button_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(button_frame, text="全选", 
                  command=self.select_all_analysts).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="全不选", 
                  command=self.deselect_all_analysts).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="推荐配置", 
                  command=self.set_recommended_analysts).pack(side=tk.LEFT, padx=(0, 5))
        
        # 分析师选择区域（使用滚动框架）- 设置固定高度
        canvas_frame = ttk.Frame(main_container, height=400)  # 设置固定高度
        canvas_frame.pack(fill=tk.BOTH, expand=False)
        canvas_frame.pack_propagate(False)
        
        canvas = tk.Canvas(canvas_frame)
        scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # 投资大师分组
        masters_group = ttk.LabelFrame(scrollable_frame, text="投资大师")
        masters_group.pack(fill=tk.X, pady=(0, 10), padx=5)
        
        # 专业分析师分组
        analysts_group = ttk.LabelFrame(scrollable_frame, text="专业分析师")
        analysts_group.pack(fill=tk.X, pady=(0, 10), padx=5)
        
        # 创建分析师复选框
        self.analyst_checkboxes = {}
        
        # 投资大师（前10个）
        master_analysts = list(self.analyst_configs.items())[:10]
        for i, (key, name) in enumerate(master_analysts):
            var = tk.BooleanVar(value=True)  # 默认选中
            checkbox = ttk.Checkbutton(masters_group, text=name, variable=var,
                                     command=self.update_analysts_count)
            checkbox.grid(row=i//2, column=i%2, sticky="w", padx=5, pady=2)
            self.analyst_checkboxes[key] = var
        
        # 专业分析师（后5个）
        tech_analysts = list(self.analyst_configs.items())[10:]
        for i, (key, name) in enumerate(tech_analysts):
            var = tk.BooleanVar(value=True)  # 默认选中
            checkbox = ttk.Checkbutton(analysts_group, text=name, variable=var,
                                     command=self.update_analysts_count)
            checkbox.grid(row=i//2, column=i%2, sticky="w", padx=5, pady=2)
            self.analyst_checkboxes[key] = var
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # 更新初始计数
        self.update_analysts_count()
        
    def create_config_tab(self):
        """创建配置标签页"""
        tab_frame = ttk.Frame(self.notebook)
        self.notebook.add(tab_frame, text="配置")
        
        # 主容器 - 设置固定高度
        main_container = ttk.Frame(tab_frame, height=400)
        main_container.pack(fill=tk.BOTH, expand=False, padx=8, pady=8)
        main_container.pack_propagate(False)  # 防止子组件改变父组件大小
        
        # 创建滚动框架来容纳所有配置项
        canvas = tk.Canvas(main_container)
        scrollbar = ttk.Scrollbar(main_container, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Ollama模型配置
        ollama_group = ttk.LabelFrame(scrollable_frame, text="Ollama模型配置")
        ollama_group.pack(fill=tk.X, pady=(0, 10))
        
        # Ollama状态
        self.ollama_status_label = ttk.Label(ollama_group, text="正在检查Ollama状态...")
        self.ollama_status_label.pack(pady=5, anchor="w")
        
        # Ollama按钮
        ollama_btn_frame = ttk.Frame(ollama_group)
        ollama_btn_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(ollama_btn_frame, text="检查状态", 
                  command=self.check_ollama_status).pack(side=tk.LEFT, padx=(5, 5))
        ttk.Button(ollama_btn_frame, text="启动Ollama", 
                  command=self.start_ollama).pack(side=tk.LEFT, padx=(0, 5))
        
        # 模型选择
        model_frame = ttk.Frame(ollama_group)
        model_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(model_frame, text="选择模型:").pack(side=tk.LEFT, padx=(5, 5))
        self.model_combo = ttk.Combobox(model_frame, width=30)
        self.model_combo.pack(side=tk.LEFT, padx=(0, 5))
        
        # 交易参数
        trading_group = ttk.LabelFrame(scrollable_frame, text="交易参数")
        trading_group.pack(fill=tk.X, pady=(0, 10))
        
        # 股票代码
        ticker_frame = ttk.Frame(trading_group)
        ticker_frame.pack(fill=tk.X, pady=5)
        ttk.Label(ticker_frame, text="股票代码:", width=12).pack(side=tk.LEFT, padx=(5, 5))
        self.tickers_entry = ttk.Entry(ticker_frame)
        self.tickers_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.tickers_entry.insert(0, "AAPL,GOOGL,MSFT,TSLA,AMZN")
        
        # 日期范围
        date_frame = ttk.Frame(trading_group)
        date_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(date_frame, text="开始日期:", width=12).pack(side=tk.LEFT, padx=(5, 5))
        self.start_date_entry = DateEntry(date_frame, width=12, background='darkblue',
                                         foreground='white', borderwidth=2,
                                         date_pattern='yyyy-mm-dd')
        self.start_date_entry.pack(side=tk.LEFT, padx=(0, 10))
        self.start_date_entry.set_date(datetime.now() - timedelta(days=90))
        
        ttk.Label(date_frame, text="结束日期:", width=12).pack(side=tk.LEFT, padx=(5, 5))
        self.end_date_entry = DateEntry(date_frame, width=12, background='darkblue',
                                       foreground='white', borderwidth=2,
                                       date_pattern='yyyy-mm-dd')
        self.end_date_entry.pack(side=tk.LEFT, padx=(0, 5))
        self.end_date_entry.set_date(datetime.now())
        
        # 资金配置
        money_frame = ttk.Frame(trading_group)
        money_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(money_frame, text="初始资金:", width=12).pack(side=tk.LEFT, padx=(5, 5))
        self.initial_cash_entry = ttk.Entry(money_frame, width=15)
        self.initial_cash_entry.pack(side=tk.LEFT, padx=(0, 10))
        self.initial_cash_entry.insert(0, "100000.0")
        
        ttk.Label(money_frame, text="保证金要求:", width=12).pack(side=tk.LEFT, padx=(5, 5))
        self.margin_entry = ttk.Entry(money_frame, width=15)
        self.margin_entry.pack(side=tk.LEFT, padx=(0, 5))
        self.margin_entry.insert(0, "0.0")
        
        # 分析选项
        options_group = ttk.LabelFrame(scrollable_frame, text="分析选项")
        options_group.pack(fill=tk.X, pady=(0, 10))
        
        self.show_reasoning_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_group, text="显示详细分析推理过程", 
                       variable=self.show_reasoning_var).pack(pady=5, padx=5, anchor="w")
        
        # 配置滚动条
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
    def create_run_tab(self):
        """创建运行标签页"""
        tab_frame = ttk.Frame(self.notebook)
        self.notebook.add(tab_frame, text="运行")
        
        # 主容器 - 设置固定高度
        main_container = ttk.Frame(tab_frame, height=400)
        main_container.pack(fill=tk.BOTH, expand=False, padx=8, pady=8)
        main_container.pack_propagate(False)  # 防止子组件改变父组件大小
        
        # 分析控制台
        control_group = ttk.LabelFrame(main_container, text="分析控制台")
        control_group.pack(fill=tk.X, pady=(0, 10))
        
        # 按钮和状态区域
        control_frame = ttk.Frame(control_group)
        control_frame.pack(fill=tk.X, pady=5)
        
        # 按钮
        button_frame = ttk.Frame(control_frame)
        button_frame.pack(side=tk.LEFT)
        
        self.run_button = ttk.Button(button_frame, text="开始分析", 
                                    command=self.run_analysis)
        self.run_button.pack(side=tk.LEFT, padx=(5, 5))
        
        self.stop_button = ttk.Button(button_frame, text="停止分析", 
                                     command=self.stop_analysis, state="disabled")
        self.stop_button.pack(side=tk.LEFT, padx=(0, 5))
        
        # 状态信息
        status_frame = ttk.Frame(control_frame)
        status_frame.pack(side=tk.RIGHT)
        
        ttk.Label(status_frame, text="分析状态:").pack(side=tk.LEFT, padx=(5, 5))
        self.status_label = ttk.Label(status_frame, text="准备就绪", 
                                     font=("Arial", 9, "bold"))
        self.status_label.pack(side=tk.LEFT)
        
        # 进度条
        progress_frame = ttk.Frame(control_group)
        progress_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(progress_frame, text="进度:").pack(side=tk.LEFT, padx=(5, 5))
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        # 输出区域 - 设置固定高度
        output_group = ttk.LabelFrame(main_container, text="实时分析日志")
        output_group.pack(fill=tk.BOTH, expand=True)
        
        self.output_text = scrolledtext.ScrolledText(output_group, 
                                                    font=("Consolas", 9),
                                                    bg="#1e1e1e", fg="#d4d4d4",
                                                    insertbackground="white")
        self.output_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 设置stdout重定向，将print输出同时显示在GUI和终端
        self.output_redirector = OutputRedirector(self.output_text, self.original_stdout)
        sys.stdout = self.output_redirector
        
        # 添加调试信息
        print("=== AI基金大师调试模式已启用 ===")
        print(f"GUI初始化完成，时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("所有print输出将同时显示在终端和GUI中")
        
    def create_results_tab(self):
        """创建结果标签页"""
        tab_frame = ttk.Frame(self.notebook)
        self.notebook.add(tab_frame, text="结果")
        
        # 主容器 - 设置固定高度
        main_container = ttk.Frame(tab_frame, height=400)
        main_container.pack(fill=tk.BOTH, expand=False, padx=8, pady=8)
        main_container.pack_propagate(False)  # 防止子组件改变父组件大小
        
        # 结果控制区域
        control_frame = ttk.Frame(main_container)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(control_frame, text="浏览器查看", 
                  command=self.open_html_in_browser).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(control_frame, text="保存报告", 
                  command=self.save_results).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(control_frame, text="保存HTML", 
                  command=self.save_html_report).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(control_frame, text="清空", 
                  command=self.clear_results).pack(side=tk.LEFT, padx=(0, 5))
        
        # 结果显示区域 - 设置固定高度
        results_notebook = ttk.Notebook(main_container)
        results_notebook.pack(fill=tk.BOTH, expand=True)
        
        # HTML报告标签页
        html_frame = ttk.Frame(results_notebook)
        results_notebook.add(html_frame, text="精美报告")
        
        html_group = ttk.LabelFrame(html_frame, text="分析报告预览")
        html_group.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.html_preview = scrolledtext.ScrolledText(html_group, 
                                                     font=("Microsoft YaHei", 10),
                                                     bg="#f8f9fa")
        self.html_preview.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 原始数据标签页
        raw_frame = ttk.Frame(results_notebook)
        results_notebook.add(raw_frame, text="详细数据")
        
        raw_group = ttk.LabelFrame(raw_frame, text="原始分析数据")
        raw_group.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.results_text = scrolledtext.ScrolledText(raw_group, 
                                                     font=("Consolas", 9))
        self.results_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
    def create_bottom_buttons(self, parent):
        """创建底部按钮区域"""
        bottom_frame = ttk.Frame(parent)
        bottom_frame.pack(fill=tk.X, pady=(0, 5))  # 减少顶部间距，增加底部间距
        
        # 创建一个内部框架来容纳按钮，并设置固定高度
        button_container = ttk.Frame(bottom_frame, height=40)  # 设置固定高度
        button_container.pack(fill=tk.X)
        button_container.pack_propagate(False)  # 防止子组件改变父组件大小
        
        # 退出按钮 - 放在右侧
        ttk.Button(button_container, text="退出", 
                   command=self.root.quit).pack(side=tk.RIGHT, padx=(10, 5))
        
        # 开始分析按钮 - 放在退出按钮左边
        self.bottom_run_button = ttk.Button(button_container, text="开始分析", 
                                           command=self.run_analysis)
        self.bottom_run_button.pack(side=tk.RIGHT, padx=(5, 10))
        
    def update_analysts_count(self):
        """更新分析师选择计数"""
        selected_count = sum(1 for var in self.analyst_checkboxes.values() if var.get())
        total_count = len(self.analyst_checkboxes)
        self.analysts_count_label.config(text=f"已选择: {selected_count}/{total_count}")
        
    def select_all_analysts(self):
        """选择所有分析师"""
        for var in self.analyst_checkboxes.values():
            var.set(True)
        self.update_analysts_count()
        
    def deselect_all_analysts(self):
        """取消选择所有分析师"""
        for var in self.analyst_checkboxes.values():
            var.set(False)
        self.update_analysts_count()
        
    def set_recommended_analysts(self):
        """设置推荐的分析师配置"""
        recommended = [
            "warren_buffett", "charlie_munger", "peter_lynch", 
            "michael_burry", "aswath_damodaran", "technical_analyst"
        ]
        
        for key, var in self.analyst_checkboxes.items():
            var.set(key in recommended)
        self.update_analysts_count()
        
    def check_ollama_status(self):
        """检查Ollama状态"""
        def check_status():
            try:
                print("正在检查Ollama状态")
                
                # 使用OllamaChecker检查状态
                checker = OllamaChecker("qwen3:0.6b")
                
                # 检查安装状态
                installed = checker.find_ollama_exe()
                print(f"Ollama安装状态: {installed}")
                
                if not installed:
                    self.root.after(0, lambda: self.ollama_status_label.config(
                        text="Ollama未安装，请先安装Ollama", foreground="red"))
                    return
                
                # 检查运行状态
                process_running = checker.check_ollama_process()
                service_ready = checker.check_ollama_service()
                print(f"Ollama进程运行状态: {process_running}")
                print(f"Ollama服务就绪状态: {service_ready}")
                
                if not process_running:
                    self.root.after(0, lambda: self.ollama_status_label.config(
                        text="Ollama已安装但未运行，请启动服务", foreground="orange"))
                    return
                elif not service_ready:
                    self.root.after(0, lambda: self.ollama_status_label.config(
                        text="Ollama进程运行中，服务正在初始化...", foreground="orange"))
                    return
                
                # 获取可用模型
                models = get_locally_available_models()
                print(f"可用模型数量: {len(models)}")
                
                if models:
                    # models已经是字符串列表，直接使用
                    self.root.after(0, lambda: self.update_model_list(models))
                    self.root.after(0, lambda: self.ollama_status_label.config(
                        text=f"Ollama运行正常，发现{len(models)}个模型", foreground="green"))
                else:
                    self.root.after(0, lambda: self.ollama_status_label.config(
                        text="Ollama运行正常，但没有可用模型", foreground="orange"))
                    
            except Exception as e:
                error_msg = str(e)
                print(f"检查Ollama状态时出错: {error_msg}")
                self.root.after(0, lambda: self.ollama_status_label.config(
                    text=f"检查状态失败: {error_msg}", foreground="red"))
        
        # 在后台线程中检查状态
        threading.Thread(target=check_status, daemon=True).start()
        
    def update_model_list(self, models):
        """更新模型列表"""
        self.model_combo['values'] = models
        if models:
            self.model_combo.current(0)
            
    def start_ollama(self):
        """启动Ollama服务"""
        def start_service():
            try:
                print("正在启动Ollama服务...")
                self.root.after(0, lambda: self.ollama_status_label.config(
                    text="正在启动Ollama服务...", foreground="blue"))
                
                # 使用OllamaChecker启动服务
                checker = OllamaChecker("qwen3:0.6b")
                success = checker.start_ollama_serve()
                
                if success:
                    print("Ollama服务启动成功")
                    self.root.after(0, lambda: self.ollama_status_label.config(
                        text="Ollama服务启动成功", foreground="green"))
                    # 重新检查状态
                    self.root.after(2000, self.check_ollama_status)
                else:
                    print("Ollama服务启动失败")
                    self.root.after(0, lambda: self.ollama_status_label.config(
                        text="Ollama服务启动失败", foreground="red"))
                    
            except Exception as e:
                error_msg = str(e)
                print(f"启动Ollama服务时出错: {error_msg}")
                self.root.after(0, lambda: self.ollama_status_label.config(
                    text=f"启动失败: {error_msg}", foreground="red"))
        
        # 在后台线程中启动服务
        threading.Thread(target=start_service, daemon=True).start()
        
    def run_analysis(self):
        """运行分析"""
        try:
            # 清空缓存
            from src.data.cache import get_cache
            cache = get_cache()
            cache.clear_cache()
            
            # 切换到运行标签页
            self.notebook.select(2)  # 运行标签页是第3个（索引2）
            
            # 更新按钮状态
            self.run_button.config(state="disabled")
            self.bottom_run_button.config(state="disabled")
            self.stop_button.config(state="normal")
            
            # 清空输出
            self.output_text.delete(1.0, tk.END)
            
            # 重置进度
            self.progress_bar.config(mode='indeterminate')
            self.progress_bar.start()
            
            # 准备分析配置
            config = {
                'model': self.model_combo.get(),
                'tickers': [ticker.strip() for ticker in self.tickers_entry.get().split(',') if ticker.strip()],
                'start_date': self.start_date_entry.get(),
                'end_date': self.end_date_entry.get(),
                'initial_cash': self.initial_cash_entry.get(),
                'margin': self.margin_entry.get(),
                'show_reasoning': self.show_reasoning_var.get(),
                'selected_analysts': [key for key, var in self.analyst_checkboxes.items() if var.get()]
            }
            
            print(f"开始分析，配置: {config}")
            
            # 记录开始时间
            self.analysis_start_time = time.time()
            self.total_analysts = len(config['selected_analysts'])
            self.completed_analysts = 0
            
            # 更新状态
            self.status_label.config(text="正在分析...")
            
            # 创建并启动分析工作线程
            self.analysis_worker = AnalysisWorker(
                config=config,
                progress_callback=self.on_progress_update,
                completed_callback=self.on_analysis_completed,
                error_callback=self.on_analysis_error
            )
            self.analysis_worker.start()
            
        except Exception as e:
            error_msg = f"启动分析时发生错误: {str(e)}"
            print(f"ERROR: {error_msg}")
            messagebox.showerror("错误", error_msg)
            self.on_analysis_error(error_msg)
            
    def stop_analysis(self):
        """停止分析"""
        try:
            print("用户请求停止分析")
            
            # 设置API中断标志
            set_api_interrupt()
            
            # 更新状态
            self.status_label.config(text="正在停止...")
            
            # 如果工作线程存在，等待其结束
            if self.analysis_worker and self.analysis_worker.is_alive():
                print("等待分析线程结束...")
                # 给线程一些时间来响应中断
                self.analysis_worker.join(timeout=5.0)
                
                if self.analysis_worker.is_alive():
                    print("WARNING: 分析线程未能及时结束")
            
            # 清除中断标志
            clear_api_interrupt()
            
            # 恢复按钮状态
            self.run_button.config(state="normal")
            self.bottom_run_button.config(state="normal")
            self.stop_button.config(state="disabled")
            
            # 停止进度条
            self.progress_bar.stop()
            
            # 更新状态
            self.status_label.config(text="已停止")
            
            print("分析已停止")
            
        except Exception as e:
            error_msg = f"停止分析时发生错误: {str(e)}"
            print(f"ERROR: {error_msg}")
            messagebox.showerror("错误", error_msg)
            
    def on_progress_update(self, message):
        """处理进度更新"""
        # 在主线程中更新UI
        self.root.after(0, lambda: self._update_progress_ui(message))
        
    def _update_progress_ui(self, message):
        """在主线程中更新进度UI"""
        # 这里可以添加更详细的进度处理逻辑
        pass
        
    def on_analysis_completed(self, result):
        """处理分析完成"""
        # 在主线程中处理结果
        self.root.after(0, lambda: self._handle_analysis_completed(result))
        
    def _handle_analysis_completed(self, result):
        """在主线程中处理分析完成"""
        try:
            # 停止进度条
            self.progress_bar.stop()
            
            # 恢复按钮状态
            self.run_button.config(state="normal")
            self.bottom_run_button.config(state="normal")
            self.stop_button.config(state="disabled")
            
            # 计算分析时间
            if self.analysis_start_time:
                elapsed_time = time.time() - self.analysis_start_time
                time_str = f"{elapsed_time:.1f}秒"
            else:
                time_str = "未知"
            
            # 更新状态
            self.status_label.config(text=f"分析完成 (耗时: {time_str})")
            
            print(f"=== 分析完成，耗时: {time_str} ===")
            
            # 保存结果数据
            self.current_result_data = result
            
            # 生成HTML报告
            try:
                html_content = generate_html_report(result)
                self.current_html_content = html_content
                
                # 显示HTML预览（提取文本内容而不是显示源码）
                self.html_preview.delete(1.0, tk.END)
                preview_text = self._extract_html_text(html_content)
                self.html_preview.insert(tk.END, preview_text)
                
            except Exception as e:
                print(f"生成HTML报告时出错: {e}")
                self.html_preview.delete(1.0, tk.END)
                self.html_preview.insert(tk.END, f"生成HTML报告时出错: {e}")
            
            # 格式化并显示原始结果
            try:
                formatted_result = format_trading_output(result)
                self.results_text.delete(1.0, tk.END)
                self.results_text.insert(tk.END, formatted_result)
                
            except Exception as e:
                print(f"格式化结果时出错: {e}")
                self.results_text.delete(1.0, tk.END)
                self.results_text.insert(tk.END, f"格式化结果时出错: {e}\n\n原始结果:\n{str(result)}")
            
            # 切换到结果标签页
            self.notebook.select(3)  # 结果标签页是第4个（索引3）
            
            print("结果显示完成")
            
        except Exception as e:
            error_msg = f"处理分析结果时发生错误: {str(e)}"
            print(f"ERROR: {error_msg}")
            messagebox.showerror("错误", error_msg)
            
    def on_analysis_error(self, error_message):
        """处理分析错误"""
        # 在主线程中处理错误
        self.root.after(0, lambda: self._handle_analysis_error(error_message))
        
    def _handle_analysis_error(self, error_message):
        """在主线程中处理分析错误"""
        # 停止进度条
        self.progress_bar.stop()
        
        # 恢复按钮状态
        self.run_button.config(state="normal")
        self.bottom_run_button.config(state="normal")
        self.stop_button.config(state="disabled")
        
        # 更新状态
        self.status_label.config(text="分析失败")
        
        # 显示错误消息
        messagebox.showerror("分析错误", error_message)
        
        print(f"分析失败: {error_message}")
        
    def open_html_in_browser(self):
        """在浏览器中打开HTML报告"""
        if not self.current_html_content:
            messagebox.showwarning("警告", "没有可用的HTML报告")
            return
            
        try:
            # 创建临时HTML文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                f.write(self.current_html_content)
                temp_file = f.name
            
            # 在浏览器中打开
            webbrowser.open(f'file://{temp_file}')
            print(f"HTML报告已在浏览器中打开: {temp_file}")
            
        except Exception as e:
            error_msg = f"打开HTML报告时发生错误: {str(e)}"
            print(f"ERROR: {error_msg}")
            messagebox.showerror("错误", error_msg)
            
    def save_results(self):
        """保存分析结果"""
        if not self.current_result_data:
            messagebox.showwarning("警告", "没有可用的分析结果")
            return
            
        try:
            # 选择保存文件
            filename = filedialog.asksaveasfilename(
                defaultextension=".json",
                filetypes=[("JSON文件", "*.json"), ("所有文件", "*.*")],
                title="保存分析结果"
            )
            
            if filename:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.current_result_data, f, ensure_ascii=False, indent=2)
                
                messagebox.showinfo("成功", f"分析结果已保存到: {filename}")
                print(f"分析结果已保存到: {filename}")
                
        except Exception as e:
            error_msg = f"保存分析结果时发生错误: {str(e)}"
            print(f"ERROR: {error_msg}")
            messagebox.showerror("错误", error_msg)
            
    def save_html_report(self):
        """保存HTML报告"""
        if not self.current_html_content:
            messagebox.showwarning("警告", "没有可用的HTML报告")
            return
            
        try:
            # 生成默认文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_filename = f"AI基金大师分析报告_{timestamp}.html"
            
            # 选择保存文件
            filename = filedialog.asksaveasfilename(
                defaultextension=".html",
                filetypes=[("HTML文件", "*.html"), ("所有文件", "*.*")],
                title="保存HTML报告",
                initialfile=default_filename
            )
            
            if filename:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(self.current_html_content)
                
                messagebox.showinfo("成功", f"HTML报告已保存到: {filename}")
                print(f"HTML报告已保存到: {filename}")
                
        except Exception as e:
            error_msg = f"保存HTML报告时发生错误: {str(e)}"
            print(f"ERROR: {error_msg}")
            messagebox.showerror("错误", error_msg)
            
    def _extract_html_text(self, html_content):
        """从HTML内容中提取文本用于预览显示"""
        try:
            import re
            from html import unescape
            
            # 如果没有HTML内容，返回提示信息
            if not html_content:
                return "没有可用的分析报告内容"
            
            # 移除CSS样式和脚本
            text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
            
            # 移除HTML注释
            text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
            
            # 将某些块级标签替换为换行
            text = re.sub(r'</(div|p|h[1-6]|section|article|header|footer|li)>', '\n', text, flags=re.IGNORECASE)
            text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
            text = re.sub(r'<hr\s*/?>', '\n' + '='*50 + '\n', text, flags=re.IGNORECASE)
            
            # 移除所有HTML标签
            text = re.sub(r'<[^>]+>', '', text)
            
            # 解码HTML实体
            text = unescape(text)
            
            # 清理空白字符
            # 移除行首行尾空白
            lines = [line.strip() for line in text.split('\n')]
            # 移除空行（保留一些空行用于格式化）
            cleaned_lines = []
            empty_line_count = 0
            for line in lines:
                if line.strip():
                    cleaned_lines.append(line)
                    empty_line_count = 0
                else:
                    empty_line_count += 1
                    if empty_line_count <= 2:  # 最多保留2个连续空行
                        cleaned_lines.append('')
            
            # 重新组合文本
            result_text = '\n'.join(cleaned_lines).strip()
            
            # 如果结果为空或太短，返回备用信息
            if not result_text or len(result_text) < 50:
                return "报告内容生成完成，请点击'浏览器查看'按钮查看完整的格式化报告"
            
            return result_text
            
        except Exception as e:
            print(f"提取HTML文本时出错: {e}")
            return f"HTML内容解析失败: {str(e)}\n\n请使用'浏览器查看'按钮查看完整报告"
    
    def clear_results(self):
        """清空结果"""
        self.html_preview.delete(1.0, tk.END)
        self.results_text.delete(1.0, tk.END)
        self.current_html_content = None
        self.current_result_data = None
        print("结果已清空")
        
    def load_config(self):
        """加载配置"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                # 恢复配置
                if 'tickers' in config:
                    self.tickers_entry.delete(0, tk.END)
                    self.tickers_entry.insert(0, config['tickers'])
                
                if 'initial_cash' in config:
                    self.initial_cash_entry.delete(0, tk.END)
                    self.initial_cash_entry.insert(0, config['initial_cash'])
                
                if 'margin' in config:
                    self.margin_entry.delete(0, tk.END)
                    self.margin_entry.insert(0, config['margin'])
                
                if 'show_reasoning' in config:
                    self.show_reasoning_var.set(config['show_reasoning'])
                
                if 'selected_analysts' in config:
                    # 只恢复仍然存在的分析师配置，过滤掉已删除的分析师
                    selected_analysts = config['selected_analysts']
                    if isinstance(selected_analysts, list):
                        # 列表格式：过滤掉不存在的分析师
                        valid_analysts = [analyst for analyst in selected_analysts if analyst in self.analyst_checkboxes]
                        for key, var in self.analyst_checkboxes.items():
                            var.set(key in valid_analysts)
                    elif isinstance(selected_analysts, dict):
                        # 字典格式：只处理仍然存在的分析师
                        for key, var in self.analyst_checkboxes.items():
                            if key in selected_analysts:
                                var.set(selected_analysts[key])
                            else:
                                var.set(False)
                    self.update_analysts_count()
                
                print("配置加载成功")
                
        except Exception as e:
            print(f"加载配置时出错: {e}")
            
    def save_config(self):
        """保存配置"""
        try:
            config = {
                'tickers': self.tickers_entry.get(),
                'initial_cash': self.initial_cash_entry.get(),
                'margin': self.margin_entry.get(),
                'show_reasoning': self.show_reasoning_var.get(),
                'selected_analysts': [key for key, var in self.analyst_checkboxes.items() if var.get()]
            }
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            
            print("配置保存成功")
            
        except Exception as e:
            print(f"保存配置时出错: {e}")
            
    def on_closing(self):
        """窗口关闭时的处理"""
        try:
            # 保存配置
            self.save_config()
            
            # 恢复stdout
            if self.original_stdout:
                sys.stdout = self.original_stdout
            
            # 如果有正在运行的分析，先停止
            if self.analysis_worker and self.analysis_worker.is_alive():
                print("正在停止分析线程...")
                set_api_interrupt()
                self.analysis_worker.join(timeout=3.0)
                clear_api_interrupt()
            
            print("GUI正在关闭...")
            
        except Exception as e:
            print(f"关闭时出错: {e}")
        finally:
            self.root.destroy()
            
    def run(self):
        """运行GUI"""
        # 设置关闭事件处理
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # 启动主循环
        self.root.mainloop()


def main():
    """主函数"""
    try:
        print("启动AI基金大师GUI...")
        app = AIHedgeFundGUI()
        app.run()
    except Exception as e:
        print(f"启动GUI时发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()