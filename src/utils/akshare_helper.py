#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AKShare辅助工具，提供错误处理和重试机制"""

import akshare as ak
import time
import random
import requests
import os
from typing import Any, Optional, Callable
from functools import wraps
import logging

# 配置日志
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class AKShareHelper:
    """AKShare辅助类，提供错误处理和重试机制"""
    
    def __init__(self):
        self.max_retries = 0
        self.base_delay = 5.0
        self.max_delay = 5.0
        self.backoff_factor = 1.0
        
        # 配置requests会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 设置AKShare的requests会话
        # 注意：AKShare大部分接口不需要token，移除空token设置
        # 如果需要特定接口的token，请在.env文件中配置AKSHARE_TOKEN
        akshare_token = os.getenv('AKSHARE_TOKEN')
        if akshare_token:
            try:
                ak.set_token(akshare_token)
                logger.info("AKShare token已设置")
            except Exception as e:
                logger.warning(f"设置AKShare token失败: {e}")
        else:
            logger.info("未配置AKShare token，使用免费接口")
    
    def retry_with_backoff(self, func: Callable, *args, **kwargs) -> Any:
        """执行函数，不进行重试"""
        try:
            # 添加5秒延迟
            time.sleep(self.base_delay)
            
            # 执行函数
            result = func(*args, **kwargs)
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"调用失败: {error_msg}")
            raise e
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """判断错误是否可重试"""
        error_str = str(error).lower()
        
        # 网络相关错误通常可以重试
        retryable_errors = [
            'connection aborted',
            'remote end closed connection',
            'connection error',
            'timeout',
            'read timeout',
            'connection timeout',
            'temporary failure',
            'service unavailable',
            'bad gateway',
            'gateway timeout',
            'internal server error'
        ]
        
        return any(err in error_str for err in retryable_errors)
    
    def safe_call(self, func: Callable, *args, **kwargs) -> Optional[Any]:
        """安全调用AKShare函数，返回None如果失败"""
        try:
            return self.retry_with_backoff(func, *args, **kwargs)
        except Exception as e:
            logger.error(f"AKShare调用最终失败: {func.__name__} - {e}")
            return None
    
    # 包装常用的AKShare函数
    def get_stock_list(self):
        """获取股票列表"""
        return self.safe_call(ak.stock_zh_a_spot_em)
    
    def get_stock_info(self, symbol: str):
        """获取股票基本信息"""
        return self.safe_call(ak.stock_individual_info_em, symbol=symbol)
    
    def get_financial_abstract(self, symbol: str):
        """获取财务摘要"""
        return self.safe_call(ak.stock_financial_abstract, symbol=symbol)
    
    def get_stock_history(self, symbol: str, period: str = "daily", 
                         start_date: str = None, end_date: str = None, 
                         adjust: str = "qfq"):
        """获取股票历史数据"""
        kwargs = {
            'symbol': symbol,
            'period': period,
            'adjust': adjust
        }
        
        if start_date:
            kwargs['start_date'] = start_date
        if end_date:
            kwargs['end_date'] = end_date
            
        return self.safe_call(ak.stock_zh_a_hist, **kwargs)
    
    def get_stock_news(self, symbol: str):
        """获取股票新闻"""
        return self.safe_call(ak.stock_news_em, symbol=symbol)
    
    def get_stock_financial_data(self, symbol: str, indicator: str = "资产负债表", period: str = "yearly"):
        """获取财务数据"""
        try:
            if indicator == "资产负债表":
                # 资产负债表只有按年度的接口
                return self.safe_call(ak.stock_balance_sheet_by_yearly_em, symbol=symbol)
            elif indicator == "利润表":
                if period == "quarterly":
                    return self.safe_call(ak.stock_profit_sheet_by_quarterly_em, symbol=symbol)
                else:
                    return self.safe_call(ak.stock_profit_sheet_by_yearly_em, symbol=symbol)
            elif indicator == "现金流量表":
                if period == "quarterly":
                    return self.safe_call(ak.stock_cash_flow_sheet_by_quarterly_em, symbol=symbol)
                else:
                    return self.safe_call(ak.stock_cash_flow_sheet_by_yearly_em, symbol=symbol)
            else:
                logger.warning(f"未知的财务指标类型: {indicator}")
                return None
        except Exception as e:
            logger.error(f"获取财务数据失败: {symbol} - {indicator} - {e}")
            return None
    
    def get_macro_economic_data(self, limit: int = 100):
        """获取宏观经济数据 - 全球事件
        
        Args:
            limit: 数据条数限制，默认100条
            
        Returns:
            宏观经济数据DataFrame或None
        """
        try:
            # 获取宏观-全球事件数据
            macro_data = self.safe_call(ak.news_economic_baidu)
            
            if macro_data is None or macro_data.empty:
                logger.warning("宏观经济数据为空")
                return None
            
            # 限制数据条数
            if len(macro_data) > limit:
                macro_data = macro_data.head(limit)
            
            # 过滤最近一年的数据
            if '时间' in macro_data.columns:
                try:
                    import pandas as pd
                    # 转换时间列为datetime
                    macro_data['时间'] = pd.to_datetime(macro_data['时间'])
                    # 获取最近一年的数据
                    one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
                    macro_data = macro_data[macro_data['时间'] >= one_year_ago]
                except Exception as e:
                    logger.warning(f"时间过滤失败: {e}")
            
            logger.info(f"成功获取{len(macro_data)}条宏观经济数据")
            return macro_data
            
        except Exception as e:
            logger.error(f"获取宏观经济数据失败: {e}")
            return None

# 创建全局实例
akshare_helper = AKShareHelper()

# 装饰器函数
def akshare_retry(max_retries: int = 3, base_delay: float = 1.0):
    """AKShare重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            helper = AKShareHelper()
            helper.max_retries = max_retries
            helper.base_delay = base_delay
            return helper.retry_with_backoff(func, *args, **kwargs)
        return wrapper
    return decorator

# 测试函数
def test_akshare_helper():
    """测试AKShare辅助工具"""
    print("测试AKShare辅助工具")
    print("=" * 50)
    
    helper = AKShareHelper()
    
    # 测试1: 获取股票列表
    print("\n1. 测试获取股票列表...")
    df_stocks = helper.get_stock_list()
    if df_stocks is not None and not df_stocks.empty:
        print(f"   成功获取 {len(df_stocks)} 只股票")
        print(f"   示例: {df_stocks.head(3)[['代码', '名称']].to_string(index=False)}")
    else:
        print("   失败")
    
    # 测试2: 获取特定股票信息
    print("\n2. 测试获取股票信息 (600030)...")
    df_info = helper.get_stock_info("600030")
    if df_info is not None and not df_info.empty:
        print(f"   成功获取股票信息，共 {len(df_info)} 项")
        info_dict = dict(zip(df_info['item'], df_info['value']))
        for key in ['股票代码', '股票简称', '总市值']:
            if key in info_dict:
                print(f"   {key}: {info_dict[key]}")
    else:
        print("   失败")
    
    # 测试3: 获取财务摘要
    print("\n3. 测试获取财务摘要 (600030)...")
    df_abstract = helper.get_financial_abstract("600030")
    if df_abstract is not None and not df_abstract.empty:
        print(f"   成功获取财务摘要，共 {len(df_abstract)} 项指标")
        print(f"   示例指标: {df_abstract.head(3)['指标'].tolist()}")
    else:
        print("   失败")
    
    # 测试4: 获取历史价格
    print("\n4. 测试获取历史价格 (600030)...")
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    
    df_hist = helper.get_stock_history("600030", start_date=start_date, end_date=end_date)
    if df_hist is not None and not df_hist.empty:
        print(f"   成功获取历史价格，共 {len(df_hist)} 个交易日")
        print(f"   最新价格: {df_hist.iloc[-1]['收盘']:.2f}")
    else:
        print("   失败")
    
    print("\n测试完成")

if __name__ == "__main__":
    test_akshare_helper()