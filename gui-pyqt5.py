#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI基金大师GUI界面 - Tkinter安全多线程版本
使用线程安全的消息队列机制，提升分析速度同时防止崩溃
"""

import sys
import os
import json
import time
import tempfile
import webbrowser
import threading
import queue
from datetime import datetime, timedelta
from io import StringIO

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
from tkinter import font as tkFont
from tkcalendar import DateEntry

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

# 全局变量存储UI组件和状态
g_widgets = {}
g_state = {
    'config_file': "gui_config.json",
    'current_html_content': None,
    'current_result_data': None,
    'analysis_start_time': None,
    'total_analysts': 0,
    'completed_analysts': 0,
    'original_stdout': sys.stdout,
    'is_analyzing': False,
    'stop_requested': False,
    'analysis_thread': None
}

# 线程安全的消息队列
g_message_queue = queue.Queue()
g_thread_lock = threading.Lock()

# 消息类型常量
MSG_PROGRESS = "progress"
MSG_OUTPUT = "output"
MSG_RESULT = "result"
MSG_ERROR = "error"
MSG_FINISHED = "finished"

# 从统一配置文件获取分析师配置
from src.utils.analysts import ANALYST_CONFIG
g_analyst_configs = {
    key: f"{config['display_name']} - {config['description']}"
    for key, config in sorted(ANALYST_CONFIG.items(), key=lambda x: x[1]['order'])
}


# 线程安全的消息发送函数
def send_message(msg_type, data):
    """线程安全地发送消息到主线程"""
    try:
        g_message_queue.put((msg_type, data), timeout=1.0)
    except queue.Full:
        print(f"消息队列已满，丢弃消息: {msg_type}")


def process_messages():
    """处理来自工作线程的消息（在主线程中调用）"""
    try:
        while True:
            try:
                msg_type, data = g_message_queue.get_nowait()
                
                if msg_type == MSG_OUTPUT:
                    append_output_text_safe(data)
                elif msg_type == MSG_PROGRESS:
                    update_progress_safe(data)
                elif msg_type == MSG_RESULT:
                    show_analysis_results_safe(data)
                elif msg_type == MSG_ERROR:
                    show_error_safe(data)
                elif msg_type == MSG_FINISHED:
                    analysis_finished_safe()
                    
                g_message_queue.task_done()
                
            except queue.Empty:
                break
                
    except Exception as e:
        print(f"处理消息时出错: {e}")
    
    # 继续定期检查消息
    if g_state.get('is_analyzing', False):
        g_widgets['root'].after(100, process_messages)


def append_output_text_safe(text):
    """线程安全的输出文本添加"""
    try:
        if 'output_text' in g_widgets:
            g_widgets['output_text'].insert(tk.END, f"{text}\n")
            g_widgets['output_text'].see(tk.END)
    except Exception as e:
        print(f"输出文本追加失败: {e}")


def update_progress_safe(data):
    """线程安全的进度更新"""
    try:
        status_text = data.get('status', '')
        if 'progress_bar' in g_widgets:
            # 使用不确定模式的进度条
            if not g_widgets['progress_bar']['mode'] == 'indeterminate':
                g_widgets['progress_bar'].start()
        if 'status_label' in g_widgets:
            g_widgets['status_label'].config(text=status_text)
    except Exception as e:
        print(f"更新进度失败: {e}")


def show_error_safe(error_msg):
    """线程安全的错误显示"""
    try:
        append_output_text_safe(f"ERROR: {error_msg}")
        messagebox.showerror("分析错误", f"{error_msg}\n\n详细信息请查看运行日志。")
    except Exception as e:
        print(f"显示错误失败: {e}")


def analysis_finished_safe():
    """线程安全的分析完成处理"""
    try:
        with g_thread_lock:
            g_state['is_analyzing'] = False
            g_state['analysis_thread'] = None
            g_state['stop_requested'] = False
        
        # 恢复UI状态
        g_widgets['run_button'].config(state="normal")
        g_widgets['bottom_run_button'].config(state="normal")
        g_widgets['stop_button'].config(state="disabled")
        stop_progress()
        
        # 清理
        try:
            clear_api_interrupt()
        except:
            pass
            
    except Exception as e:
        print(f"分析完成处理失败: {e}")


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
    """运行AI基金分析"""
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

        # Fix analyst signal key mapping for HTML report compatibility
        analyst_signals = final_state["data"]["analyst_signals"].copy()
        if "technical_analyst_agent" in analyst_signals:
            analyst_signals["technical_analyst"] = analyst_signals.pop("technical_analyst_agent")

        return {
            "decisions": portfolio_decisions,
            "analyst_signals": analyst_signals,
        }
    except Exception as e:
        print(f"分析过程中发生错误: {e}")
        import traceback
        print(f"错误堆栈: {traceback.format_exc()}")
        raise


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


# 安全的工作线程类
class AnalysisWorker(threading.Thread):
    """安全的分析工作线程"""
    
    def __init__(self, config):
        super().__init__(daemon=True)
        self.config = config
        self.stop_requested = False
    
    def request_stop(self):
        """请求停止线程"""
        self.stop_requested = True
        set_api_interrupt()
    
    def run(self):
        """线程主执行函数"""
        try:
            send_message(MSG_OUTPUT, "=== 开始分析验证 ===")
            
            # 清空缓存
            try:
                from src.data.cache import get_cache
                cache = get_cache()
                cache.clear_cache()
                send_message(MSG_OUTPUT, "数据缓存已清理")
            except Exception as e:
                send_message(MSG_OUTPUT, f"清理缓存失败（忽略）: {e}")
            
            if self.stop_requested:
                return
            
            # 清除API中断标志
            clear_api_interrupt()
            send_message(MSG_OUTPUT, "开始AI基金大师分析...")
            
            # 准备配置
            portfolio = {
                "cash": float(self.config['initial_cash']),
                "margin_requirement": float(self.config['margin']),
                "positions": {}
            }
            
            send_message(MSG_OUTPUT, f"分析配置: {self.config}")
            send_message(MSG_PROGRESS, {'status': "配置完成，开始分析..."})
            
            # 设置环境变量
            if self.config.get('provider') == 'DeepSeek' and self.config.get('api_key'):
                os.environ['DEEPSEEK_API_KEY'] = self.config['api_key']
                if self.config.get('base_url'):
                    os.environ['DEEPSEEK_BASE_URL'] = self.config['base_url']
            
            if self.stop_requested:
                return
            
            # 设置进度更新处理器
            def progress_handler(agent_name, ticker, status, analysis, timestamp):
                """处理进度更新"""
                if self.stop_requested:
                    return
                
                try:
                    progress_text = f"[{timestamp}] {agent_name}: {status}"
                    if ticker:
                        progress_text += f" [{ticker}]"
                    if analysis:
                        progress_text += f" - {analysis[:100]}{'...' if len(analysis) > 100 else ''}"
                    
                    send_message(MSG_OUTPUT, f"PROGRESS: {progress_text}")
                    
                    # 更新进度计数
                    if "Done" in status:
                        with g_thread_lock:
                            g_state['completed_analysts'] += 1
                    
                    if g_state['total_analysts'] > 0:
                        elapsed_time = time.time() - g_state['analysis_start_time']
                        if elapsed_time < 60:
                            time_str = f"{elapsed_time:.0f}秒"
                        else:
                            time_str = f"{elapsed_time/60:.1f}分钟"
                        
                        progress_text = f"分析进行中... ({g_state['completed_analysts']}/{g_state['total_analysts']}) - {time_str}"
                        send_message(MSG_PROGRESS, {'status': progress_text})
                    
                except Exception as e:
                    print(f"进度更新错误（忽略）: {e}")
            
            # 注册进度处理器
            progress.register_handler(progress_handler)
            
            try:
                send_message(MSG_PROGRESS, {'status': "开始运行AI Fund Master分析..."})
                
                if self.stop_requested:
                    return
                
                # 运行分析 - 这是主要的分析过程
                result = run_hedge_fund(
                    tickers=self.config['tickers'],
                    start_date=self.config['start_date'],
                    end_date=self.config['end_date'],
                    portfolio=portfolio,
                    show_reasoning=self.config['show_reasoning'],
                    selected_analysts=self.config['selected_analysts'],
                    model_name=self.config['model'],
                    model_provider=self.config.get('provider', 'DeepSeek')
                )
                
                if self.stop_requested:
                    return
                
                send_message(MSG_PROGRESS, {'status': "分析完成，生成报告..."})
                send_message(MSG_OUTPUT, "=== 分析完成 ===")
                
                # 发送结果
                send_message(MSG_RESULT, result)
                
                # 计算总时间
                total_time = time.time() - g_state['analysis_start_time']
                if total_time < 60:
                    time_str = f"{total_time:.0f}秒"
                else:
                    time_str = f"{total_time/60:.1f}分钟"
                
                send_message(MSG_PROGRESS, {'status': f"分析完成 - 总耗时: {time_str}"})
                
                # 显示完成提示
                messagebox.showinfo(
                    "✅ 分析完成", 
                    "🎉 投资分析已成功完成！\n\n" +
                    "📊 请查看'分析结果'标签页获取详细报告\n" +
                    "🌐 点击'浏览器查看'按钮可查看完整HTML报告\n" +
                    "💾 可使用'保存结果'按钮保存分析报告"
                )
                
            finally:
                # 取消注册进度处理器
                try:
                    progress.unregister_handler(progress_handler)
                except Exception as e:
                    print(f"取消注册进度处理器失败（忽略）: {e}")
            
        except Exception as e:
            if not self.stop_requested:
                error_msg = f"分析过程中发生错误: {str(e)}"
                import traceback
                full_traceback = traceback.format_exc()
                send_message(MSG_OUTPUT, f"错误堆栈: {full_traceback}")
                send_message(MSG_ERROR, error_msg)
        
        finally:
            # 通知分析完成
            send_message(MSG_FINISHED, None)


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


def show_analysis_results_safe(result):
    """线程安全的结果显示"""
    try:
        # 切换到结果标签页
        g_widgets['notebook'].select(3)
        
        # 验证结果
        if not isinstance(result, dict):
            error_msg = f"接收到无效的结果类型: {type(result).__name__}，期望字典类型"
            g_widgets['html_preview'].delete(1.0, tk.END)
            g_widgets['html_preview'].insert(tk.END, f"❌ 结果类型错误: {error_msg}")
            g_widgets['results_text'].delete(1.0, tk.END)
            g_widgets['results_text'].insert(tk.END, f"❌ 结果类型错误: {error_msg}")
            return
        
        # 存储结果数据
        with g_thread_lock:
            g_state['current_result_data'] = result
        
        # 生成HTML报告
        try:
            g_state['current_html_content'] = generate_html_report(result)
            # 显示HTML报告的文本预览版本
            html_preview_text = extract_html_text(g_state['current_html_content'])
            g_widgets['html_preview'].delete(1.0, tk.END)
            g_widgets['html_preview'].insert(tk.END, html_preview_text)
        except Exception as e:
            g_widgets['html_preview'].delete(1.0, tk.END)
            g_widgets['html_preview'].insert(tk.END, f"HTML报告生成失败: {str(e)}")
        
        # 格式化并显示原始结果数据
        try:
            formatted_result = format_trading_output(result)
            g_widgets['results_text'].delete(1.0, tk.END)
            g_widgets['results_text'].insert(tk.END, formatted_result)
        except Exception as e:
            g_widgets['results_text'].delete(1.0, tk.END)
            g_widgets['results_text'].insert(tk.END, f"结果格式化失败: {str(e)}")
            
    except Exception as e:
        error_msg = f"显示分析结果时发生错误: {str(e)}"
        print(f"ERROR: {error_msg}")


def append_output_text(text):
    """添加输出文本"""
    try:
        if 'output_text' in g_widgets:
            g_widgets['output_text'].insert(tk.END, f"{text}\n")
            g_widgets['output_text'].see(tk.END)
    except Exception as e:
        print(f"输出文本追加失败: {e}")


def update_progress(status_text):
    """更新进度"""
    try:
        if 'progress_bar' in g_widgets:
            # 使用不确定模式的进度条
            g_widgets['progress_bar'].start()
        if 'status_label' in g_widgets:
            g_widgets['status_label'].config(text=status_text)
        
        # 强制更新UI
        g_widgets['root'].update()
    except Exception as e:
        print(f"更新进度失败: {e}")


def stop_progress():
    """停止进度条"""
    try:
        if 'progress_bar' in g_widgets:
            g_widgets['progress_bar'].stop()
    except Exception as e:
        print(f"停止进度条失败: {e}")


def run_analysis_async():
    """异步运行分析（多线程）"""
    with g_thread_lock:
        if g_state['is_analyzing']:
            messagebox.showwarning("警告", "分析正在进行中，请等待完成")
            return
    
    try:
        print("=== 开始分析验证 ===")
        
        # 验证输入
        model_name = g_widgets['model_combo'].get()
        if not model_name:
            messagebox.showerror("错误", "请先选择一个大模型")
            return
            
        selected_analysts = get_selected_analysts()
        if not selected_analysts:
            messagebox.showerror("错误", "请至少选择一个AI分析师")
            return
            
        # 验证DeepSeek的API key
        provider = g_widgets['provider_combo'].get()
        if provider == "DeepSeek":
            api_key = g_widgets['apikey_entry'].get().strip()
            if not api_key:
                messagebox.showerror("错误", "DeepSeek供应商需要提供API Key！\n\n请在API Key字段中填写您的DeepSeek API密钥。")
                return
            
        # 解析股票代码
        import re
        tickers_input = g_widgets['tickers_entry'].get()
        tickers = [t.strip().upper() for t in re.split(r'[,;\s\t\n]+', tickers_input) if t.strip()]
        if not tickers:
            messagebox.showerror("错误", "请输入至少一个股票代码")
            return
        
        # 设置分析状态
        g_state['is_analyzing'] = True
        g_state['analysis_start_time'] = time.time()
        g_state['total_analysts'] = len(selected_analysts)
        g_state['completed_analysts'] = 0
        
        # 切换到运行标签页
        g_widgets['notebook'].select(2)
        
        # 禁用按钮
        g_widgets['run_button'].config(state="disabled")
        g_widgets['bottom_run_button'].config(state="disabled")
        g_widgets['stop_button'].config(state="normal")
        
        # 清空输出
        g_widgets['output_text'].delete(1.0, tk.END)
        update_progress("正在初始化...")
        
        # 清空缓存
        try:
            from src.data.cache import get_cache
            cache = get_cache()
            cache.clear_cache()
            append_output_text("数据缓存已清理")
        except Exception as e:
            append_output_text(f"清理缓存失败（忽略）: {e}")
        
        # 清除API中断标志
        clear_api_interrupt()
        append_output_text("开始AI基金大师分析...")
        
        # 获取实际的模型名称
        display_name = g_widgets['model_combo'].get()
        provider = g_widgets['provider_combo'].get()
        actual_model_name = get_actual_model_name(display_name, provider)
        
        # 准备配置
        try:
            portfolio = {
                "cash": float(g_widgets['initial_cash_entry'].get()),
                "margin_requirement": float(g_widgets['margin_entry'].get()),
                "positions": {}
            }
        except ValueError as e:
            messagebox.showerror("配置错误", f"资金配置错误: {e}")
            return
        
        config = {
            'provider': provider,
            'model': actual_model_name or display_name,
            'base_url': g_widgets['baseurl_entry'].get(),
            'api_key': g_widgets['apikey_entry'].get(),
            'tickers': tickers,
            'start_date': g_widgets['start_date_entry'].get(),
            'end_date': g_widgets['end_date_entry'].get(),
            'initial_cash': g_widgets['initial_cash_entry'].get(),
            'margin': g_widgets['margin_entry'].get(),
            'show_reasoning': g_widgets['show_reasoning_var'].get(),
            'selected_analysts': selected_analysts
        }
        
        append_output_text(f"分析配置: {config}")
        update_progress("配置完成，开始分析...")
        
        # 设置环境变量
        if config.get('provider') == 'DeepSeek' and config.get('api_key'):
            os.environ['DEEPSEEK_API_KEY'] = config['api_key']
            if config.get('base_url'):
                os.environ['DEEPSEEK_BASE_URL'] = config['base_url']
        
        # 初始化状态
        with g_thread_lock:
            g_state['is_analyzing'] = True
            g_state['analysis_start_time'] = time.time()
            g_state['total_analysts'] = len(selected_analysts)
            g_state['completed_analysts'] = 0
            g_state['stop_requested'] = False
        
        # 创建并启动工作线程
        worker = AnalysisWorker(config)
        with g_thread_lock:
            g_state['analysis_thread'] = worker
        
        worker.start()
        
        # 启动消息处理循环
        process_messages()
        
        append_output_text("=== AI基金大师多线程版本已启动 ===")
        append_output_text(f"分析线程已启动，时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        error_msg = f"启动分析时发生错误: {str(e)}"
        append_output_text(f"ERROR: {error_msg}")
        import traceback
        full_traceback = traceback.format_exc()
        append_output_text(f"错误堆栈: {full_traceback}")
        
        messagebox.showerror("启动错误", f"{error_msg}\n\n详细信息请查看运行日志。")
        
        # 恢复UI状态
        analysis_finished_safe()


def stop_analysis():
    """安全地停止分析"""
    with g_thread_lock:
        if not g_state['is_analyzing']:
            return
        
        g_state['stop_requested'] = True
    
    print("用户请求停止分析")
    try:
        # 设置API中断标志
        set_api_interrupt()
        
        # 请求工作线程停止
        with g_thread_lock:
            if g_state['analysis_thread'] and g_state['analysis_thread'].is_alive():
                g_state['analysis_thread'].request_stop()
                append_output_text("正在请求分析线程停止...")
        
        update_progress_safe({'status': "正在停止..."})
        
    except Exception as e:
        print(f"停止分析时出错: {e}")
        append_output_text(f"停止分析时出错: {e}")


def show_analysis_results(result):
    """显示分析结果"""
    try:
        # 切换到结果标签页
        g_widgets['notebook'].select(3)
        
        # 验证结果
        if not isinstance(result, dict):
            error_msg = f"接收到无效的结果类型: {type(result).__name__}，期望字典类型"
            g_widgets['html_preview'].delete(1.0, tk.END)
            g_widgets['html_preview'].insert(tk.END, f"❌ 结果类型错误: {error_msg}")
            g_widgets['results_text'].delete(1.0, tk.END)
            g_widgets['results_text'].insert(tk.END, f"❌ 结果类型错误: {error_msg}")
            return
        
        # 存储结果数据
        g_state['current_result_data'] = result
        
        # 生成HTML报告
        try:
            g_state['current_html_content'] = generate_html_report(result)
            # 显示HTML报告的文本预览版本
            html_preview_text = extract_html_text(g_state['current_html_content'])
            g_widgets['html_preview'].delete(1.0, tk.END)
            g_widgets['html_preview'].insert(tk.END, html_preview_text)
        except Exception as e:
            g_widgets['html_preview'].delete(1.0, tk.END)
            g_widgets['html_preview'].insert(tk.END, f"HTML报告生成失败: {str(e)}")
        
        # 格式化并显示原始结果数据
        try:
            formatted_result = format_trading_output(result)
            g_widgets['results_text'].delete(1.0, tk.END)
            g_widgets['results_text'].insert(tk.END, formatted_result)
        except Exception as e:
            g_widgets['results_text'].delete(1.0, tk.END)
            g_widgets['results_text'].insert(tk.END, f"结果格式化失败: {str(e)}")
            
    except Exception as e:
        error_msg = f"显示分析结果时发生错误: {str(e)}"
        print(f"ERROR: {error_msg}")
        messagebox.showerror("错误", error_msg)


def extract_html_text(html_content):
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


def get_selected_analysts():
    """获取选中的分析师"""
    selected = []
    for key, var in g_widgets.get('analyst_checkboxes', {}).items():
        if var.get():
            selected.append(key)
    return selected


def select_all_analysts():
    """选择所有分析师"""
    for var in g_widgets.get('analyst_checkboxes', {}).values():
        var.set(True)
    update_analysts_count()


def deselect_all_analysts():
    """取消选择所有分析师"""
    for var in g_widgets.get('analyst_checkboxes', {}).values():
        var.set(False)
    update_analysts_count()


def set_recommended_analysts():
    """设置推荐的分析师配置"""
    recommended = [
        "warren_buffett", "charlie_munger", "peter_lynch"
    ]
    
    for key, var in g_widgets.get('analyst_checkboxes', {}).items():
        var.set(key in recommended)
    update_analysts_count()


def update_analysts_count():
    """更新分析师选择计数"""
    if 'analyst_checkboxes' in g_widgets and 'analysts_count_label' in g_widgets:
        selected_count = sum(1 for var in g_widgets['analyst_checkboxes'].values() if var.get())
        total_count = len(g_widgets['analyst_checkboxes'])
        g_widgets['analysts_count_label'].config(text=f"已选择: {selected_count}/{total_count}")


def check_ollama_status():
    """检查Ollama状态（同步版本）"""
    try:
        print("正在检查Ollama状态")
        
        # 使用OllamaChecker检查状态
        checker = OllamaChecker("qwen3:0.6b")
        
        # 检查安装状态
        installed = checker.find_ollama_exe()
        print(f"Ollama安装状态: {installed}")
        
        if not installed:
            g_widgets['ollama_status_label'].config(
                text="Ollama未安装，请先安装Ollama", foreground="red")
            return
        
        # 检查运行状态
        process_running = checker.check_ollama_process()
        service_ready = checker.check_ollama_service()
        print(f"Ollama进程运行状态: {process_running}")
        print(f"Ollama服务就绪状态: {service_ready}")
        
        if not process_running:
            g_widgets['ollama_status_label'].config(
                text="Ollama已安装但未运行，请启动服务", foreground="orange")
            return
        elif not service_ready:
            g_widgets['ollama_status_label'].config(
                text="Ollama进程运行中，服务正在初始化...", foreground="orange")
            return
        
        # 获取可用模型
        models = get_locally_available_models()
        print(f"可用模型数量: {len(models)}")
        
        if models:
            # models已经是字符串列表，直接使用
            update_model_list(models)
            g_widgets['ollama_status_label'].config(
                text=f"Ollama运行正常，发现{len(models)}个模型", foreground="green")
        else:
            g_widgets['ollama_status_label'].config(
                text="Ollama运行正常，但没有可用模型", foreground="orange")
            
    except Exception as e:
        error_msg = str(e)
        print(f"检查Ollama状态时出错: {error_msg}")
        g_widgets['ollama_status_label'].config(
            text=f"检查状态失败: {error_msg}", foreground="red")


def on_provider_changed():
    """当供应商改变时的处理"""
    provider = g_widgets['provider_combo'].get()
    
    if provider == "Ollama":
        # 隐藏BaseUrl和API Key（Ollama使用本地服务）
        g_widgets['baseurl_frame'].pack_forget()
        g_widgets['apikey_frame'].pack_forget()
        # 显示Ollama相关控件
        g_widgets['ollama_frame'].pack(fill=tk.X, pady=(10, 0))
        # 检查Ollama状态
        check_ollama_status()
    else:
        # 隐藏Ollama相关控件
        g_widgets['ollama_frame'].pack_forget()
        # 显示BaseUrl和API Key
        g_widgets['baseurl_frame'].pack(fill=tk.X, pady=5)
        g_widgets['apikey_frame'].pack(fill=tk.X, pady=5)
        # 加载API模型列表
        load_api_models()
    
    # 设置默认配置
    set_provider_defaults()


def set_provider_defaults():
    """设置供应商默认配置"""
    provider = g_widgets['provider_combo'].get()
    
    if provider == "DeepSeek":
        # 只在没有已保存配置时设置默认值
        if not g_widgets['baseurl_entry'].get():
            g_widgets['baseurl_entry'].delete(0, tk.END)
            g_widgets['baseurl_entry'].insert(0, "https://api.deepseek.com")
    elif provider == "Ollama":
        # Ollama使用本地服务，不需要设置BaseUrl和API Key
        pass


def load_api_models():
    """加载API模型列表"""
    try:
        from src.llm.models import load_models_from_json
        import os
        
        # 读取API模型配置
        api_models_file = os.path.join("src", "llm", "api_models.json")
        if os.path.exists(api_models_file):
            models = load_models_from_json(api_models_file)
            provider = g_widgets['provider_combo'].get()
            
            # 过滤当前供应商的模型
            filtered_models = [model.display_name for model in models if model.provider == provider]
            
            g_widgets['model_combo']['values'] = filtered_models
            if filtered_models:
                g_widgets['model_combo'].current(0)
            else:
                g_widgets['model_combo']['values'] = []
        else:
            print(f"API模型配置文件不存在: {api_models_file}")
            
    except Exception as e:
        print(f"加载API模型列表时出错: {e}")
        g_widgets['model_combo']['values'] = []


def get_actual_model_name(display_name: str, provider: str) -> str:
    """根据display_name和provider获取实际的model_name"""
    try:
        from src.llm.models import load_models_from_json
        import os
        
        # 读取API模型配置
        api_models_file = os.path.join("src", "llm", "api_models.json")
        if os.path.exists(api_models_file):
            models = load_models_from_json(api_models_file)
            
            # 查找匹配的模型
            for model in models:
                if model.display_name == display_name and model.provider == provider:
                    return model.model_name
                    
        return display_name  # 如果找不到，返回原始名称
        
    except Exception as e:
        print(f"获取实际模型名称时出错: {e}")
        return display_name


def update_model_list(models):
    """更新模型列表（用于Ollama）"""
    # 保存当前选择的模型
    current_model = g_widgets['model_combo'].get()
    
    g_widgets['model_combo']['values'] = models
    if models:
        # 如果当前模型在新列表中，保持选择
        if current_model and current_model in models:
            g_widgets['model_combo'].set(current_model)
        else:
            # 只有在没有当前选择或当前选择不在列表中时才选择第一个
            g_widgets['model_combo'].current(0)


def start_ollama():
    """启动Ollama服务（同步版本）"""
    try:
        print("正在启动Ollama服务...")
        g_widgets['ollama_status_label'].config(
            text="正在启动Ollama服务...", foreground="blue")
        
        # 强制更新UI
        g_widgets['root'].update()
        
        # 使用OllamaChecker启动服务
        checker = OllamaChecker("qwen3:0.6b")
        success = checker.start_ollama_serve()
        
        if success:
            print("Ollama服务启动成功")
            g_widgets['ollama_status_label'].config(
                text="Ollama服务启动成功", foreground="green")
            # 延迟检查状态
            g_widgets['root'].after(2000, check_ollama_status)
        else:
            print("Ollama服务启动失败")
            g_widgets['ollama_status_label'].config(
                text="Ollama服务启动失败", foreground="red")
            
    except Exception as e:
        error_msg = str(e)
        print(f"启动Ollama服务时出错: {error_msg}")
        g_widgets['ollama_status_label'].config(
            text=f"启动失败: {error_msg}", foreground="red")


def open_html_in_browser():
    """在浏览器中打开HTML报告"""
    if not g_state['current_html_content']:
        messagebox.showwarning("警告", "没有可用的HTML报告")
        return
        
    try:
        # 创建临时HTML文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
            f.write(g_state['current_html_content'])
            temp_file = f.name
        
        # 在浏览器中打开
        webbrowser.open(f'file://{temp_file}')
        print(f"HTML报告已在浏览器中打开: {temp_file}")
        
    except Exception as e:
        error_msg = f"打开HTML报告时发生错误: {str(e)}"
        print(f"ERROR: {error_msg}")
        messagebox.showerror("错误", error_msg)


def save_results():
    """保存分析结果"""
    if not g_state['current_result_data']:
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
                json.dump(g_state['current_result_data'], f, ensure_ascii=False, indent=2)
            
            messagebox.showinfo("成功", f"分析结果已保存到: {filename}")
            print(f"分析结果已保存到: {filename}")
            
    except Exception as e:
        error_msg = f"保存分析结果时发生错误: {str(e)}"
        print(f"ERROR: {error_msg}")
        messagebox.showerror("错误", error_msg)


def save_html_report():
    """保存HTML报告"""
    if not g_state['current_html_content']:
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
                f.write(g_state['current_html_content'])
            
            messagebox.showinfo("成功", f"HTML报告已保存到: {filename}")
            print(f"HTML报告已保存到: {filename}")
            
    except Exception as e:
        error_msg = f"保存HTML报告时发生错误: {str(e)}"
        print(f"ERROR: {error_msg}")
        messagebox.showerror("错误", error_msg)


def clear_results():
    """清空结果"""
    g_widgets['html_preview'].delete(1.0, tk.END)
    g_widgets['results_text'].delete(1.0, tk.END)
    g_state['current_html_content'] = None
    g_state['current_result_data'] = None
    print("结果已清空")


def load_config():
    """加载配置"""
    try:
        if os.path.exists(g_state['config_file']):
            with open(g_state['config_file'], 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # 恢复供应商配置
            if 'provider' in config:
                provider = config['provider']
                if provider in ["DeepSeek", "Ollama"]:
                    g_widgets['provider_combo'].set(provider)
                    print(f"恢复供应商配置: {provider}")
            
            # 恢复DeepSeek的模型名称和API key
            if 'model' in config:
                model_name = config.get('model', '')
                # 延迟设置模型，等供应商切换完成后
                g_widgets['root'].after(100, lambda: restore_model_config(model_name))
                print(f"准备恢复模型配置: {model_name}")
            
            if 'base_url' in config:
                base_url = config['base_url']
                g_widgets['baseurl_entry'].delete(0, tk.END)
                g_widgets['baseurl_entry'].insert(0, base_url)
                print(f"恢复Base URL配置: {base_url}")
            
            if 'api_key' in config:
                api_key = config['api_key']
                g_widgets['apikey_entry'].delete(0, tk.END)
                g_widgets['apikey_entry'].insert(0, api_key)
                print(f"恢复API Key配置: {'*' * min(len(api_key), 8) if api_key else '空'}")
            
            # 恢复其他配置
            if 'tickers' in config:
                g_widgets['tickers_entry'].delete(0, tk.END)
                g_widgets['tickers_entry'].insert(0, config['tickers'])
            
            if 'initial_cash' in config:
                g_widgets['initial_cash_entry'].delete(0, tk.END)
                g_widgets['initial_cash_entry'].insert(0, config['initial_cash'])
            
            if 'margin' in config:
                g_widgets['margin_entry'].delete(0, tk.END)
                g_widgets['margin_entry'].insert(0, config['margin'])
            
            if 'show_reasoning' in config:
                g_widgets['show_reasoning_var'].set(config['show_reasoning'])
            
            if 'selected_analysts' in config:
                # 只恢复仍然存在的分析师配置，过滤掉已删除的分析师
                selected_analysts = config['selected_analysts']
                if isinstance(selected_analysts, list):
                    # 列表格式：过滤掉不存在的分析师
                    valid_analysts = [analyst for analyst in selected_analysts if analyst in g_widgets['analyst_checkboxes']]
                    for key, var in g_widgets['analyst_checkboxes'].items():
                        var.set(key in valid_analysts)
                elif isinstance(selected_analysts, dict):
                    # 字典格式：只处理仍然存在的分析师
                    for key, var in g_widgets['analyst_checkboxes'].items():
                        if key in selected_analysts:
                            var.set(selected_analysts[key])
                        else:
                            var.set(False)
                update_analysts_count()
            
            print("配置加载成功")
            
    except Exception as e:
        print(f"加载配置时出错: {e}")


def restore_model_config(model_name):
    """恢复模型配置"""
    try:
        if model_name:
            # 检查模型是否在当前列表中
            current_models = g_widgets['model_combo']['values']
            if current_models and model_name in current_models:
                g_widgets['model_combo'].set(model_name)
                print(f"成功恢复模型配置: {model_name}")
            else:
                print(f"模型 '{model_name}' 不在当前可用列表中: {current_models}")
                # 如果模型不在列表中，仍然设置它（可能是用户自定义的模型名）
                g_widgets['model_combo'].set(model_name)
                print(f"强制设置模型配置: {model_name}")
    except Exception as e:
        print(f"恢复模型配置时出错: {e}")


def save_config():
    """保存配置"""
    try:
        # 检查GUI组件是否还存在并可访问
        if not g_widgets:
            print("GUI组件不可用，跳过配置保存")
            return
        
        # 安全地获取组件值
        try:
            provider = g_widgets['provider_combo'].get() if 'provider_combo' in g_widgets else "DeepSeek"
        except:
            provider = "DeepSeek"
            
        try:
            model = g_widgets['model_combo'].get() if 'model_combo' in g_widgets else ""
        except:
            model = ""
            
        try:
            base_url = g_widgets['baseurl_entry'].get() if 'baseurl_entry' in g_widgets else "https://api.deepseek.com"
        except:
            base_url = "https://api.deepseek.com"
            
        try:
            api_key = g_widgets['apikey_entry'].get() if 'apikey_entry' in g_widgets else ""
        except:
            api_key = ""
        
        try:
            tickers = g_widgets['tickers_entry'].get() if 'tickers_entry' in g_widgets else "AAPL,GOOGL,MSFT,TSLA,AMZN"
        except:
            tickers = "AAPL,GOOGL,MSFT,TSLA,AMZN"
            
        try:
            initial_cash = g_widgets['initial_cash_entry'].get() if 'initial_cash_entry' in g_widgets else "100000.0"
        except:
            initial_cash = "100000.0"
            
        try:
            margin = g_widgets['margin_entry'].get() if 'margin_entry' in g_widgets else "0.0"
        except:
            margin = "0.0"
            
        try:
            show_reasoning = g_widgets['show_reasoning_var'].get() if 'show_reasoning_var' in g_widgets else True
        except:
            show_reasoning = True
            
        try:
            selected_analysts = [key for key, var in g_widgets['analyst_checkboxes'].items() if var.get()] if 'analyst_checkboxes' in g_widgets else []
        except:
            selected_analysts = []
        
        config = {
            'provider': provider,
            'model': model,
            'base_url': base_url,
            'api_key': api_key,
            'tickers': tickers,
            'initial_cash': initial_cash,
            'margin': margin,
            'show_reasoning': show_reasoning,
            'selected_analysts': selected_analysts
        }
        
        with open(g_state['config_file'], 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        print(f"配置保存成功 - 供应商: {provider}, 模型: {model}")
        if provider == "DeepSeek":
            print(f"DeepSeek配置已保存 - Base URL: {base_url}, API Key: {'已设置' if api_key else '未设置'}")
        
    except Exception as e:
        print(f"保存配置时出错: {e}")


def create_analysts_tab():
    """创建分析师选择标签页"""
    tab_frame = ttk.Frame(g_widgets['notebook'])
    g_widgets['notebook'].add(tab_frame, text="分析师")
    
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
    
    g_widgets['analysts_count_label'] = ttk.Label(title_frame, text="已选择: 0/12")
    g_widgets['analysts_count_label'].pack(side=tk.RIGHT)
    
    # 快捷操作按钮
    button_frame = ttk.Frame(main_container)
    button_frame.pack(fill=tk.X, pady=(0, 10))
    
    ttk.Button(button_frame, text="全选", 
              command=select_all_analysts).pack(side=tk.LEFT, padx=(0, 5))
    ttk.Button(button_frame, text="全不选", 
              command=deselect_all_analysts).pack(side=tk.LEFT, padx=(0, 5))
    ttk.Button(button_frame, text="推荐配置", 
              command=set_recommended_analysts).pack(side=tk.LEFT, padx=(0, 5))
    
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
    
    # 第一行分组
    row1_group = ttk.LabelFrame(scrollable_frame, text="投资大师")
    row1_group.pack(fill=tk.X, pady=(0, 10), padx=5)
    
    # 技术分析师分组
    tech_group = ttk.LabelFrame(scrollable_frame, text="专业分析师")
    tech_group.pack(fill=tk.X, pady=(0, 10), padx=5)
    
    # 创建分析师复选框
    g_widgets['analyst_checkboxes'] = {}
    
    # 投资大师（前11个）
    master_analysts = list(g_analyst_configs.items())[:11]
    for i, (key, name) in enumerate(master_analysts):
        var = tk.BooleanVar(value=True)  # 默认选中
        checkbox = ttk.Checkbutton(row1_group, text=name, variable=var,
                                 command=update_analysts_count)
        checkbox.grid(row=i//2, column=i%2, sticky="w", padx=5, pady=2)
        g_widgets['analyst_checkboxes'][key] = var
    
    # 专业分析师（技术分析师）
    tech_analysts = list(g_analyst_configs.items())[11:]
    for i, (key, name) in enumerate(tech_analysts):
        var = tk.BooleanVar(value=True)  # 默认选中
        checkbox = ttk.Checkbutton(tech_group, text=name, variable=var,
                                 command=update_analysts_count)
        checkbox.grid(row=i//2, column=i%2, sticky="w", padx=5, pady=2)
        g_widgets['analyst_checkboxes'][key] = var
    
    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")
    
    # 更新初始计数
    update_analysts_count()


def create_config_tab():
    """创建配置标签页"""
    tab_frame = ttk.Frame(g_widgets['notebook'])
    g_widgets['notebook'].add(tab_frame, text="配置")
    
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
    
    # LLM模型配置
    llm_group = ttk.LabelFrame(scrollable_frame, text="LLM模型配置")
    llm_group.pack(fill=tk.X, pady=(0, 10))
    
    # 供应商选择
    provider_frame = ttk.Frame(llm_group)
    provider_frame.pack(fill=tk.X, pady=5)
    
    ttk.Label(provider_frame, text="供应商:").pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['provider_combo'] = ttk.Combobox(provider_frame, values=["DeepSeek", "Ollama"], width=15, state="readonly")
    g_widgets['provider_combo'].pack(side=tk.LEFT, padx=(0, 10))
    g_widgets['provider_combo'].bind("<<ComboboxSelected>>", lambda e: on_provider_changed())
    g_widgets['provider_combo'].current(0)  # 默认选择DeepSeek
    
    # 模型选择
    ttk.Label(provider_frame, text="模型:").pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['model_combo'] = ttk.Combobox(provider_frame, width=30)
    g_widgets['model_combo'].pack(side=tk.LEFT, padx=(0, 5))
    
    # BaseUrl配置
    g_widgets['baseurl_frame'] = ttk.Frame(llm_group)
    g_widgets['baseurl_frame'].pack(fill=tk.X, pady=5)
    ttk.Label(g_widgets['baseurl_frame'], text="Base URL:").pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['baseurl_entry'] = ttk.Entry(g_widgets['baseurl_frame'], width=50)
    g_widgets['baseurl_entry'].pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
    
    # API Key配置
    g_widgets['apikey_frame'] = ttk.Frame(llm_group)
    g_widgets['apikey_frame'].pack(fill=tk.X, pady=5)
    ttk.Label(g_widgets['apikey_frame'], text="API Key:").pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['apikey_entry'] = ttk.Entry(g_widgets['apikey_frame'], width=50, show="*")
    g_widgets['apikey_entry'].pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
    
    # Ollama相关控件（仅当选择Ollama时显示）
    g_widgets['ollama_frame'] = ttk.Frame(llm_group)
    
    # Ollama状态
    g_widgets['ollama_status_label'] = ttk.Label(g_widgets['ollama_frame'], text="正在检查Ollama状态...")
    g_widgets['ollama_status_label'].pack(pady=5, anchor="w")
    
    # Ollama按钮
    ollama_btn_frame = ttk.Frame(g_widgets['ollama_frame'])
    ollama_btn_frame.pack(fill=tk.X, pady=5)
    
    ttk.Button(ollama_btn_frame, text="检查状态", 
              command=check_ollama_status).pack(side=tk.LEFT, padx=(5, 5))
    ttk.Button(ollama_btn_frame, text="启动Ollama", 
              command=start_ollama).pack(side=tk.LEFT, padx=(0, 5))
    
    # 交易参数
    trading_group = ttk.LabelFrame(scrollable_frame, text="交易参数")
    trading_group.pack(fill=tk.X, pady=(0, 10))
    
    # 股票代码
    ticker_frame = ttk.Frame(trading_group)
    ticker_frame.pack(fill=tk.X, pady=5)
    ttk.Label(ticker_frame, text="股票代码:", width=12).pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['tickers_entry'] = ttk.Entry(ticker_frame)
    g_widgets['tickers_entry'].pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
    g_widgets['tickers_entry'].insert(0, "AAPL,GOOGL,MSFT,TSLA,AMZN")
    
    # 日期范围
    date_frame = ttk.Frame(trading_group)
    date_frame.pack(fill=tk.X, pady=5)
    
    ttk.Label(date_frame, text="开始日期:", width=12).pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['start_date_entry'] = DateEntry(date_frame, width=12, background='darkblue',
                                     foreground='white', borderwidth=2,
                                     date_pattern='yyyy-mm-dd')
    g_widgets['start_date_entry'].pack(side=tk.LEFT, padx=(0, 10))
    g_widgets['start_date_entry'].set_date(datetime.now() - timedelta(days=90))
    
    ttk.Label(date_frame, text="结束日期:", width=12).pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['end_date_entry'] = DateEntry(date_frame, width=12, background='darkblue',
                                   foreground='white', borderwidth=2,
                                   date_pattern='yyyy-mm-dd')
    g_widgets['end_date_entry'].pack(side=tk.LEFT, padx=(0, 5))
    g_widgets['end_date_entry'].set_date(datetime.now())
    
    # 资金配置
    money_frame = ttk.Frame(trading_group)
    money_frame.pack(fill=tk.X, pady=5)
    
    ttk.Label(money_frame, text="初始资金:", width=12).pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['initial_cash_entry'] = ttk.Entry(money_frame, width=15)
    g_widgets['initial_cash_entry'].pack(side=tk.LEFT, padx=(0, 10))
    g_widgets['initial_cash_entry'].insert(0, "100000.0")
    
    ttk.Label(money_frame, text="保证金要求:", width=12).pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['margin_entry'] = ttk.Entry(money_frame, width=15)
    g_widgets['margin_entry'].pack(side=tk.LEFT, padx=(0, 5))
    g_widgets['margin_entry'].insert(0, "0.0")
    
    # 分析选项
    options_group = ttk.LabelFrame(scrollable_frame, text="分析选项")
    options_group.pack(fill=tk.X, pady=(0, 10))
    
    g_widgets['show_reasoning_var'] = tk.BooleanVar(value=True)
    ttk.Checkbutton(options_group, text="显示详细分析推理过程", 
                   variable=g_widgets['show_reasoning_var']).pack(pady=5, padx=5, anchor="w")
    
    # 配置滚动条
    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")


def create_run_tab():
    """创建运行标签页"""
    tab_frame = ttk.Frame(g_widgets['notebook'])
    g_widgets['notebook'].add(tab_frame, text="运行")
    
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
    
    g_widgets['run_button'] = ttk.Button(button_frame, text="开始分析", 
                                command=run_analysis_async)
    g_widgets['run_button'].pack(side=tk.LEFT, padx=(5, 5))
    
    g_widgets['stop_button'] = ttk.Button(button_frame, text="停止分析", 
                                 command=stop_analysis, state="disabled")
    g_widgets['stop_button'].pack(side=tk.LEFT, padx=(0, 5))
    
    # 状态信息
    status_frame = ttk.Frame(control_frame)
    status_frame.pack(side=tk.RIGHT)
    
    ttk.Label(status_frame, text="分析状态:").pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['status_label'] = ttk.Label(status_frame, text="准备就绪", 
                                 font=("Arial", 9, "bold"))
    g_widgets['status_label'].pack(side=tk.LEFT)
    
    # 进度条
    progress_frame = ttk.Frame(control_group)
    progress_frame.pack(fill=tk.X, pady=5)
    
    ttk.Label(progress_frame, text="进度:").pack(side=tk.LEFT, padx=(5, 5))
    g_widgets['progress_bar'] = ttk.Progressbar(progress_frame, mode='indeterminate')
    g_widgets['progress_bar'].pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
    
    # 输出区域 - 设置固定高度
    output_group = ttk.LabelFrame(main_container, text="实时分析日志")
    output_group.pack(fill=tk.BOTH, expand=True)
    
    g_widgets['output_text'] = scrolledtext.ScrolledText(output_group, 
                                                font=("Consolas", 9),
                                                bg="#1e1e1e", fg="#d4d4d4",
                                                insertbackground="white")
    g_widgets['output_text'].pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
    
    # 添加调试信息
    append_output_text("=== AI基金大师安全多线程版本已启用 ===")
    append_output_text(f"GUI初始化完成，时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    append_output_text("使用线程安全的消息队列机制，提升分析速度同时防止崩溃")


def create_results_tab():
    """创建结果标签页"""
    tab_frame = ttk.Frame(g_widgets['notebook'])
    g_widgets['notebook'].add(tab_frame, text="结果")
    
    # 主容器 - 设置固定高度
    main_container = ttk.Frame(tab_frame, height=400)
    main_container.pack(fill=tk.BOTH, expand=False, padx=8, pady=8)
    main_container.pack_propagate(False)  # 防止子组件改变父组件大小
    
    # 结果控制区域
    control_frame = ttk.Frame(main_container)
    control_frame.pack(fill=tk.X, pady=(0, 10))
    
    ttk.Button(control_frame, text="浏览器查看", 
              command=open_html_in_browser).pack(side=tk.LEFT, padx=(0, 5))
    ttk.Button(control_frame, text="保存报告", 
              command=save_results).pack(side=tk.LEFT, padx=(0, 5))
    ttk.Button(control_frame, text="保存HTML", 
              command=save_html_report).pack(side=tk.LEFT, padx=(0, 5))
    ttk.Button(control_frame, text="清空", 
              command=clear_results).pack(side=tk.LEFT, padx=(0, 5))
    
    # 结果显示区域 - 设置固定高度
    results_notebook = ttk.Notebook(main_container)
    results_notebook.pack(fill=tk.BOTH, expand=True)
    
    # HTML报告标签页
    html_frame = ttk.Frame(results_notebook)
    results_notebook.add(html_frame, text="精美报告")
    
    html_group = ttk.LabelFrame(html_frame, text="分析报告预览")
    html_group.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
    
    g_widgets['html_preview'] = scrolledtext.ScrolledText(html_group, 
                                                 font=("Microsoft YaHei", 10),
                                                 bg="#f8f9fa")
    g_widgets['html_preview'].pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
    
    # 原始数据标签页
    raw_frame = ttk.Frame(results_notebook)
    results_notebook.add(raw_frame, text="详细数据")
    
    raw_group = ttk.LabelFrame(raw_frame, text="原始分析数据")
    raw_group.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
    
    g_widgets['results_text'] = scrolledtext.ScrolledText(raw_group, 
                                                 font=("Consolas", 9))
    g_widgets['results_text'].pack(fill=tk.BOTH, expand=True, padx=5, pady=5)


def create_bottom_buttons(parent):
    """创建底部按钮区域"""
    bottom_frame = ttk.Frame(parent)
    bottom_frame.pack(fill=tk.X, pady=(0, 5))  # 减少顶部间距，增加底部间距
    
    # 创建一个内部框架来容纳按钮，并设置固定高度
    button_container = ttk.Frame(bottom_frame, height=40)  # 设置固定高度
    button_container.pack(fill=tk.X)
    button_container.pack_propagate(False)  # 防止子组件改变父组件大小
    
    # 退出按钮 - 放在右侧
    ttk.Button(button_container, text="退出", 
               command=on_closing).pack(side=tk.RIGHT, padx=(10, 5))
    
    # 开始分析按钮 - 放在退出按钮左边
    g_widgets['bottom_run_button'] = ttk.Button(button_container, text="开始分析", 
                                       command=run_analysis_async)
    g_widgets['bottom_run_button'].pack(side=tk.RIGHT, padx=(5, 10))


def on_closing():
    """窗口关闭时的处理"""
    try:
        # 保存配置
        save_config()
        
        # 如果有正在运行的分析，先安全停止
        with g_thread_lock:
            if g_state['is_analyzing'] and g_state['analysis_thread']:
                print("正在安全停止分析线程...")
                g_state['stop_requested'] = True
                set_api_interrupt()
                
                # 请求线程停止
                g_state['analysis_thread'].request_stop()
                
                # 等待线程结束（最多3秒）
                try:
                    g_state['analysis_thread'].join(timeout=3.0)
                    if g_state['analysis_thread'].is_alive():
                        print("WARNING: 分析线程未能正常结束")
                    else:
                        print("分析线程已安全结束")
                except Exception as e:
                    print(f"等待线程结束时出错: {e}")
        
        print("GUI正在关闭...")
        
    except Exception as e:
        print(f"关闭时出错: {e}")
    finally:
        g_widgets['root'].destroy()


def create_main_window():
    """创建主窗口"""
    root = tk.Tk()
    g_widgets['root'] = root
    
    root.title("AI基金大师 v2.2 - 267278466@qq.com")
    
    # 设置窗口大小和位置（居中显示）
    window_width = 800
    window_height = 540  # 增加窗口高度以容纳固定高度的notebook
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width - window_width) // 2
    y = (screen_height - window_height) // 2
    root.geometry(f"{window_width}x{window_height}+{x}+{y}")
    root.minsize(800, 540)  # 设置最小尺寸
    
    # 设置窗口图标
    try:
        root.iconbitmap("mrcai.ico")
    except Exception as e:
        print(f"设置图标失败: {e}")
    
    # 创建主框架
    main_frame = ttk.Frame(root)
    main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
    
    # 创建标题
    title_label = ttk.Label(main_frame, text="AI基金大师", 
                           font=("Arial", 12, "bold"))
    title_label.pack(pady=(0, 5))
    
    # 创建标签页容器框架，设置固定高度
    notebook_container = ttk.Frame(main_frame)
    notebook_container.pack(fill=tk.BOTH, expand=True)
    
    # 创建标签页
    g_widgets['notebook'] = ttk.Notebook(notebook_container)
    g_widgets['notebook'].pack(fill=tk.BOTH, expand=False, pady=(0, 10))  # 底部留出空间给按钮
    
    # 创建各个标签页
    create_analysts_tab()
    create_config_tab()
    create_run_tab()
    create_results_tab()
    
    # 创建底部按钮区域
    create_bottom_buttons(main_frame)
    
    # 设置关闭事件处理
    root.protocol("WM_DELETE_WINDOW", on_closing)
    
    return root


def main():
    """主函数"""
    try:
        print("启动AI基金大师GUI...")
        
        # 创建主窗口
        root = create_main_window()
        
        # 加载配置
        load_config()
        
        # 延迟执行供应商初始化
        root.after(100, on_provider_changed)
        
        # 启动主循环
        root.mainloop()
        
    except Exception as e:
        print(f"启动GUI时发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
