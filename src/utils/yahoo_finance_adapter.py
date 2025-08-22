#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo财经数据适配器
用于获取股票价格、财务指标等数据，支持美股、港股和中国A股
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import time
import random
import threading

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    yf = None

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YahooFinanceAdapter:
    """Yahoo财经数据适配器"""
    
    def __init__(self):
        self.logger = logger
        if not HAS_YFINANCE:
            self.logger.warning("yfinance库未安装，Yahoo财经功能不可用")
        
        # 限流配置（参考data-collector的稳定配置）
        self.request_delay_min = 1.8  # 最小延迟时间（秒）- 参考稳定配置
        self.request_delay_max = 3.5  # 最大延迟时间（秒）
        self.daily_request_limit = 2000  # 每日请求限制
        self.hourly_request_limit = 100   # 每小时请求限制
        
        # 请求计数器
        self.request_count_daily = 0
        self.request_count_hourly = 0
        self.last_request_time = 0
        self.daily_reset_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.hourly_reset_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        
        # 线程锁
        self._lock = threading.Lock()
    
    def _check_yfinance_available(self):
        """检查yfinance是否可用"""
        if not HAS_YFINANCE:
            raise ImportError("yfinance库未安装，请运行: pip install yfinance")
    
    def _rate_limit_check(self):
        """检查和控制API请求频率"""
        with self._lock:
            current_time = datetime.now()
            
            # 重置计数器
            if current_time >= self.daily_reset_time + timedelta(days=1):
                self.request_count_daily = 0
                self.daily_reset_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
                self.logger.info("每日请求计数器已重置")
            
            if current_time >= self.hourly_reset_time + timedelta(hours=1):
                self.request_count_hourly = 0
                self.hourly_reset_time = current_time.replace(minute=0, second=0, microsecond=0)
                self.logger.debug("每小时请求计数器已重置")
            
            # 检查请求限制
            if self.request_count_daily >= self.daily_request_limit:
                self.logger.warning("已达到每日请求限制，暂停请求")
                raise Exception("Yahoo Finance daily request limit exceeded")
            
            if self.request_count_hourly >= self.hourly_request_limit:
                self.logger.warning("已达到每小时请求限制，等待下一小时")
                wait_time = (self.hourly_reset_time + timedelta(hours=1) - current_time).total_seconds()
                if wait_time > 0:
                    time.sleep(min(wait_time, 60))  # 最多等待60秒
            
            # 控制请求间隔
            time_since_last_request = time.time() - self.last_request_time
            min_interval = random.uniform(self.request_delay_min, self.request_delay_max)
            
            if time_since_last_request < min_interval:
                sleep_time = min_interval - time_since_last_request
                self.logger.debug(f"限流延时: {sleep_time:.2f}秒")
                time.sleep(sleep_time)
            
            # 更新计数器和时间戳
            self.request_count_daily += 1
            self.request_count_hourly += 1
            self.last_request_time = time.time()
            
            self.logger.debug(f"API请求计数: 每日{self.request_count_daily}/{self.daily_request_limit}, "
                            f"每小时{self.request_count_hourly}/{self.hourly_request_limit}")
    
    def _safe_get_data(self, func, **kwargs):
        """安全获取数据，包含重试机制和限流控制（参考data-collector的稳定策略）"""
        max_retries = 5  # 增加重试次数
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                # 执行限流检查
                self._rate_limit_check()
                
                # 执行API请求
                result = func(**kwargs)
                
                # 检查结果有效性
                if result is not None:
                    if hasattr(result, 'empty') and not result.empty:
                        return result
                    elif not hasattr(result, 'empty') and result:
                        return result
                    elif attempt == max_retries - 1:
                        # 最后一次尝试，返回结果（即使为空）
                        return result
                
                # 如果结果为空但不是最后一次尝试，继续重试
                if attempt < max_retries - 1:
                    delay = base_delay + random.uniform(0.5, 1.5)
                    self.logger.debug(f"数据为空，延时{delay:.2f}秒后重试 (尝试 {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    
            except Exception as e:
                error_msg = str(e).lower()
                
                # 特殊处理不同类型的错误
                if any(keyword in error_msg for keyword in ['rate limit', 'too many requests', '429']):
                    self.logger.warning(f"遇到限流错误，增加延时: {e}")
                    # 对于限流错误，使用指数退避
                    extended_delay = base_delay * (2 ** attempt) + random.uniform(10, 20)
                    time.sleep(extended_delay)
                elif any(keyword in error_msg for keyword in ['timeout', 'connection', 'network']):
                    self.logger.warning(f"网络错误，重试: {e}")
                    if attempt < max_retries - 1:
                        delay = base_delay * (1.5 ** attempt) + random.uniform(1, 3)
                        time.sleep(delay)
                elif 'delisted' in error_msg or 'not found' in error_msg:
                    self.logger.warning(f"股票可能已退市或不存在: {e}")
                    return None  # 直接返回，不再重试
                else:
                    self.logger.warning(f"获取数据失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        delay = base_delay * (1.2 ** attempt) + random.uniform(0.5, 1.5)
                        time.sleep(delay)
        
        self.logger.warning(f"所有重试尝试失败，返回None")
        return None
    
    def configure_rate_limits(self, 
                            request_delay_min: float = None,
                            request_delay_max: float = None,
                            daily_limit: int = None,
                            hourly_limit: int = None):
        """配置限流参数"""
        with self._lock:
            if request_delay_min is not None:
                self.request_delay_min = max(0.5, request_delay_min)  # 最小0.5秒
            if request_delay_max is not None:
                self.request_delay_max = max(self.request_delay_min, request_delay_max)
            if daily_limit is not None:
                self.daily_request_limit = max(100, daily_limit)  # 最少100次
            if hourly_limit is not None:
                self.hourly_request_limit = max(10, hourly_limit)  # 最少10次
                
        self.logger.info(f"限流参数已更新: 延时{self.request_delay_min}-{self.request_delay_max}秒, "
                        f"每日{self.daily_request_limit}次, 每小时{self.hourly_request_limit}次")
    
    def get_rate_limit_status(self) -> Dict[str, Any]:
        """获取当前限流状态"""
        return {
            'daily_requests': self.request_count_daily,
            'daily_limit': self.daily_request_limit,
            'hourly_requests': self.request_count_hourly,
            'hourly_limit': self.hourly_request_limit,
            'delay_range': f"{self.request_delay_min}-{self.request_delay_max}s",
            'daily_reset_time': self.daily_reset_time.isoformat(),
            'hourly_reset_time': self.hourly_reset_time.isoformat()
        }
    
    def convert_to_yahoo_symbol(self, ticker: str) -> str:
        """转换股票代码为Yahoo财经格式"""
        ticker = ticker.strip().upper()
        
        # 检测市场类型
        market = self._detect_market(ticker)
        
        if market == 'CN':
            # 中国A股：添加相应的后缀
            if ticker.startswith('00') or ticker.startswith('30'):
                # 深圳交易所
                return f"{ticker}.SZ"
            elif ticker.startswith('6') or ticker.startswith('9'):
                # 上海交易所
                return f"{ticker}.SS"
            else:
                # 默认上海交易所
                return f"{ticker}.SS"
        elif market == 'HK':
            # 港股：添加.HK后缀
            if not ticker.endswith('.HK'):
                # 港股代码需要补齐到4位
                ticker_num = ticker.replace('.HK', '')
                ticker_padded = ticker_num.zfill(4)
                return f"{ticker_padded}.HK"
            return ticker
        elif market == 'US':
            # 美股：直接使用原代码
            return ticker
        
        return ticker
    
    def _detect_market(self, symbol: str) -> str:
        """检测股票所属市场"""
        symbol = symbol.strip().upper()
        
        # 处理带交易所后缀的情况
        if symbol.endswith('.SH') or symbol.endswith('.SS') or symbol.endswith('.SZ'):
            return 'CN'
        elif symbol.endswith('.HK'):
            return 'HK'
        elif symbol.endswith('.US'):
            return 'US'
        
        # 基于长度和字符类型判断
        if len(symbol) == 6 and symbol.isdigit():
            return 'CN'  # A股：6位数字
        elif len(symbol) <= 5 and symbol.isdigit():
            return 'HK'  # 港股：1-5位数字
        elif symbol.isalpha() and 1 <= len(symbol) <= 5:
            return 'US'  # 美股：1-5位字母
        
        # 默认返回US（对于未知格式）
        return 'US'
    
    def get_stock_info(self, ticker: str) -> Dict[str, Any]:
        """获取股票基本信息"""
        self._check_yfinance_available()
        
        try:
            yahoo_symbol = self.convert_to_yahoo_symbol(ticker)
            stock = yf.Ticker(yahoo_symbol)
            
            # 获取股票信息
            info = self._safe_get_data(lambda: stock.info)
            if not info:
                self.logger.warning(f"无法获取股票信息: {ticker}")
                return {}
            
            # 提取关键信息
            stock_info = {
                'symbol': ticker,
                'yahoo_symbol': yahoo_symbol,
                'company_name': info.get('longName', info.get('shortName', '')),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'market_cap': info.get('marketCap', 0),
                'enterprise_value': info.get('enterpriseValue', 0),
                'trailing_pe': info.get('trailingPE', None),  # 优先使用Yahoo的PE
                'forward_pe': info.get('forwardPE', None),
                'price_to_book': info.get('priceToBook', None),
                'price_to_sales_ttm': info.get('priceToSalesTrailing12Months', None),
                'enterprise_to_revenue': info.get('enterpriseToRevenue', None),
                'enterprise_to_ebitda': info.get('enterpriseToEbitda', None),
                'profit_margins': info.get('profitMargins', None),
                'gross_margins': info.get('grossMargins', None),
                'operating_margins': info.get('operatingMargins', None),
                'return_on_assets': info.get('returnOnAssets', None),
                'return_on_equity': info.get('returnOnEquity', None),
                'revenue_ttm': info.get('totalRevenue', 0),
                'net_income_ttm': info.get('netIncomeToCommon', 0),
                'total_cash': info.get('totalCash', 0),
                'total_debt': info.get('totalDebt', 0),
                'current_price': info.get('currentPrice', info.get('regularMarketPrice', 0)),
                'dividend_yield': info.get('dividendYield', None),
                'payout_ratio': info.get('payoutRatio', None),
                'beta': info.get('beta', None),
                'fifty_two_week_high': info.get('fiftyTwoWeekHigh', None),
                'fifty_two_week_low': info.get('fiftyTwoWeekLow', None),
                'shares_outstanding': info.get('sharesOutstanding', 0),
                'float_shares': info.get('floatShares', 0),
                'currency': info.get('currency', 'USD'),
                'exchange': info.get('exchange', ''),
                'country': info.get('country', ''),
                'last_updated': datetime.now().isoformat()
            }
            
            self.logger.info(f"成功获取{ticker}的股票信息")
            return stock_info
            
        except Exception as e:
            self.logger.error(f"获取股票信息失败: {ticker} - {e}")
            return {}
    
    def get_historical_data(self, ticker: str, start_date: str, end_date: str, interval: str = "1d") -> pd.DataFrame:
        """获取历史价格数据"""
        self._check_yfinance_available()
        
        try:
            yahoo_symbol = self.convert_to_yahoo_symbol(ticker)
            stock = yf.Ticker(yahoo_symbol)
            
            # 获取历史数据
            hist_data = self._safe_get_data(
                lambda: stock.history(start=start_date, end=end_date, interval=interval, auto_adjust=True, prepost=True)
            )
            
            if hist_data is None or hist_data.empty:
                self.logger.warning(f"无法获取历史数据: {ticker}")
                return pd.DataFrame()
            
            # 验证数据质量
            if not self._validate_data_quality(hist_data, ticker):
                self.logger.warning(f"历史数据质量验证失败: {ticker}")
                return pd.DataFrame()
            
            # 重置索引，将日期作为列
            hist_data = hist_data.reset_index()
            
            # 标准化列名
            if 'Date' in hist_data.columns:
                hist_data = hist_data.rename(columns={'Date': 'date'})
            elif 'Datetime' in hist_data.columns:
                hist_data = hist_data.rename(columns={'Datetime': 'date'})
            
            # 确保必要的列存在并处理异常值
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in required_columns:
                if col not in hist_data.columns:
                    hist_data[col] = 0
                else:
                    # 清理异常值
                    hist_data[col] = pd.to_numeric(hist_data[col], errors='coerce').fillna(0)
            
            # 过滤掉无效的数据行
            valid_mask = (hist_data['Close'] > 0) & (hist_data['High'] >= hist_data['Low'])
            hist_data = hist_data[valid_mask]
            
            if hist_data.empty:
                self.logger.warning(f"过滤后无有效历史数据: {ticker}")
                return pd.DataFrame()
            
            self.logger.info(f"成功获取{ticker}的历史数据，{len(hist_data)}条记录")
            return hist_data
            
        except Exception as e:
            self.logger.error(f"获取历史数据失败: {ticker} - {e}")
            return pd.DataFrame()
    
    def get_financial_data(self, ticker: str) -> Dict[str, Any]:
        """获取财务数据"""
        self._check_yfinance_available()
        
        try:
            yahoo_symbol = self.convert_to_yahoo_symbol(ticker)
            stock = yf.Ticker(yahoo_symbol)
            
            financial_data = {}
            
            # 获取财务报表
            try:
                # 资产负债表
                balance_sheet = self._safe_get_data(lambda: stock.balance_sheet)
                if balance_sheet is not None and not balance_sheet.empty:
                    financial_data['balance_sheet'] = balance_sheet.to_dict()
                
                # 利润表
                income_stmt = self._safe_get_data(lambda: stock.financials)
                if income_stmt is not None and not income_stmt.empty:
                    financial_data['income_statement'] = income_stmt.to_dict()
                
                # 现金流量表
                cash_flow = self._safe_get_data(lambda: stock.cashflow)
                if cash_flow is not None and not cash_flow.empty:
                    financial_data['cash_flow'] = cash_flow.to_dict()
                
            except Exception as e:
                self.logger.warning(f"获取财务报表失败: {ticker} - {e}")
            
            # 获取季度财务数据
            try:
                quarterly_balance_sheet = self._safe_get_data(lambda: stock.quarterly_balance_sheet)
                if quarterly_balance_sheet is not None and not quarterly_balance_sheet.empty:
                    financial_data['quarterly_balance_sheet'] = quarterly_balance_sheet.to_dict()
                
                quarterly_income_stmt = self._safe_get_data(lambda: stock.quarterly_financials)
                if quarterly_income_stmt is not None and not quarterly_income_stmt.empty:
                    financial_data['quarterly_income_statement'] = quarterly_income_stmt.to_dict()
                
                quarterly_cash_flow = self._safe_get_data(lambda: stock.quarterly_cashflow)
                if quarterly_cash_flow is not None and not quarterly_cash_flow.empty:
                    financial_data['quarterly_cash_flow'] = quarterly_cash_flow.to_dict()
                
            except Exception as e:
                self.logger.warning(f"获取季度财务数据失败: {ticker} - {e}")
            
            self.logger.info(f"成功获取{ticker}的财务数据")
            return financial_data
            
        except Exception as e:
            self.logger.error(f"获取财务数据失败: {ticker} - {e}")
            return {}
    
    def calculate_financial_ratios(self, ticker: str, stock_info: Dict[str, Any] = None) -> Dict[str, float]:
        """计算财务比率（数据增强器）"""
        if stock_info is None:
            stock_info = self.get_stock_info(ticker)
        
        if not stock_info:
            return {}
        
        ratios = {}
        
        try:
            # 基本价格信息
            current_price = stock_info.get('current_price', 0)
            shares_outstanding = stock_info.get('shares_outstanding', 0)
            market_cap = stock_info.get('market_cap', 0)
            
            # 财务数据
            revenue_ttm = stock_info.get('revenue_ttm', 0)
            net_income_ttm = stock_info.get('net_income_ttm', 0)
            total_cash = stock_info.get('total_cash', 0)
            total_debt = stock_info.get('total_debt', 0)
            
            # 计算市盈率（如果Yahoo没有提供）
            if stock_info.get('trailing_pe') is None and net_income_ttm > 0 and shares_outstanding > 0:
                eps = net_income_ttm / shares_outstanding
                if eps > 0:
                    ratios['calculated_pe'] = current_price / eps
            
            # 计算市销率（如果Yahoo没有提供）
            if stock_info.get('price_to_sales_ttm') is None and revenue_ttm > 0:
                ratios['calculated_ps'] = market_cap / revenue_ttm
            
            # 计算净债务
            if total_debt > 0 or total_cash > 0:
                ratios['net_debt'] = total_debt - total_cash
                ratios['net_debt_to_market_cap'] = ratios['net_debt'] / market_cap if market_cap > 0 else 0
            
            # 每股收益
            if net_income_ttm > 0 and shares_outstanding > 0:
                ratios['earnings_per_share'] = net_income_ttm / shares_outstanding
            
            # 每股销售额
            if revenue_ttm > 0 and shares_outstanding > 0:
                ratios['sales_per_share'] = revenue_ttm / shares_outstanding
            
            # 每股现金
            if total_cash > 0 and shares_outstanding > 0:
                ratios['cash_per_share'] = total_cash / shares_outstanding
            
            self.logger.info(f"成功计算{ticker}的财务比率")
            
        except Exception as e:
            self.logger.error(f"计算财务比率失败: {ticker} - {e}")
        
        return ratios
    
    def _safe_float(self, value):
        """安全转换为浮点数（参考data-collector）"""
        try:
            if pd.isna(value) or value is None or value == '' or value == 'N/A':
                return None
            if isinstance(value, str) and value.strip() == '':
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _validate_data_quality(self, df: pd.DataFrame, ticker: str) -> bool:
        """验证数据质量（参考data-collector的数据验证）"""
        if df is None or df.empty:
            return False
        
        # 检查必需的列
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume'] if 'Open' in df.columns else ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_columns):
            self.logger.warning(f"数据缺少必需列: {ticker}")
            return False
        
        # 检查数据异常（参考data-collector的异常检测）
        close_col = 'Close' if 'Close' in df.columns else 'close'
        volume_col = 'Volume' if 'Volume' in df.columns else 'volume'
        
        # 检查价格异常
        if close_col in df.columns:
            valid_prices = df[close_col].dropna()
            if len(valid_prices) == 0:
                self.logger.warning(f"所有价格数据无效: {ticker}")
                return False
            
            # 检查价格变化异常（参考data-collector的逻辑）
            if len(valid_prices) > 1:
                price_changes = valid_prices.pct_change().dropna()
                extreme_changes = price_changes[(price_changes > 10) | (price_changes < -0.9)]
                if len(extreme_changes) > len(price_changes) * 0.1:  # 超过10%的数据异常
                    self.logger.warning(f"价格变化异常过多: {ticker}")
        
        return True
    
    def get_comprehensive_data(self, ticker: str, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """获取综合数据，包含基本信息、历史数据和财务数据"""
        comprehensive_data = {
            'ticker': ticker,
            'yahoo_symbol': self.convert_to_yahoo_symbol(ticker),
            'market': self._detect_market(ticker),
            'data_source': 'yahoo_finance',
            'timestamp': datetime.now().isoformat()
        }
        
        # 获取基本信息
        stock_info = self.get_stock_info(ticker)
        if stock_info:
            comprehensive_data['basic_info'] = stock_info
        
        # 获取历史数据
        if start_date and end_date:
            hist_data = self.get_historical_data(ticker, start_date, end_date)
            if not hist_data.empty:
                comprehensive_data['historical_data'] = hist_data.to_dict('records')
        
        # 获取财务数据
        financial_data = self.get_financial_data(ticker)
        if financial_data:
            comprehensive_data['financial_data'] = financial_data
        
        # 计算增强的财务比率
        if stock_info:
            enhanced_ratios = self.calculate_financial_ratios(ticker, stock_info)
            if enhanced_ratios:
                comprehensive_data['enhanced_ratios'] = enhanced_ratios
        
        return comprehensive_data

# 创建全局实例
yahoo_adapter = YahooFinanceAdapter()
