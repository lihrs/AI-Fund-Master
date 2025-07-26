import math

from langchain_core.messages import HumanMessage

from src.graph.state import AgentState, show_agent_reasoning

import json
import pandas as pd
import numpy as np

from src.utils.progress import progress
from src.utils.data_analysis_rules import (
    standardize_data_analysis_approach, 
    get_analysis_scoring_adjustment,
    apply_minimum_score_rule,
    format_analysis_reasoning,
    ANALYST_CORE_METRICS
)


def safe_float(value, default=0.0):
    """
    Safely convert a value to float, handling NaN cases
    
    Args:
        value: The value to convert (can be pandas scalar, numpy value, etc.)
        default: Default value to return if the input is NaN or invalid
    
    Returns:
        float: The converted value or default if NaN/invalid
    """
    try:
        if pd.isna(value) or np.isnan(value):
            return default
        return float(value)
    except (ValueError, TypeError, OverflowError):
        return default


##### 技术分析师 #####
def technical_analyst_agent(state: AgentState, agent_id: str = "technical_analyst_agent"):
    """
    使用统一数据访问适配器的复杂技术分析系统,结合多种交易策略:
    1. 趋势跟踪
    2. 均值回归
    3. 动量分析
    4. 波动率分析
    5. 统计套利信号
    """
    data = state["data"]
    tickers = data["tickers"]
    prefetched_data = data["prefetched_data"]
    unified_data_accessor = data["unified_data_accessor"]

    # 为每个股票代码初始化分析
    technical_analysis = {}

    for ticker in tickers:
        progress.update_status(agent_id, ticker, "分析价格数据")

        # 从统一数据访问适配器获取综合数据
        comprehensive_data = unified_data_accessor.data_prefetcher.get_comprehensive_data(ticker, prefetched_data)
        price_data = comprehensive_data.get('price_data', [])

        if not price_data:
            progress.update_status(agent_id, ticker, "失败:未找到价格数据")
            continue

        # 将价格数据转换为DataFrame(适应原有函数)
        prices_df = convert_prices_to_df(price_data)

        progress.update_status(agent_id, ticker, "计算趋势信号")
        trend_signals = calculate_trend_signals(prices_df)

        progress.update_status(agent_id, ticker, "计算均值回归")
        mean_reversion_signals = calculate_mean_reversion_signals(prices_df)

        progress.update_status(agent_id, ticker, "计算动量")
        momentum_signals = calculate_momentum_signals(prices_df)

        progress.update_status(agent_id, ticker, "分析波动率")
        volatility_signals = calculate_volatility_signals(prices_df)

        progress.update_status(agent_id, ticker, "统计分析")
        stat_arb_signals = calculate_stat_arb_signals(prices_df)

        # 使用加权集成方法组合所有信号
        strategy_weights = {
            "trend": 0.25,
            "mean_reversion": 0.20,
            "momentum": 0.25,
            "volatility": 0.15,
            "stat_arb": 0.15,
        }

        progress.update_status(agent_id, ticker, "组合信号")
        combined_signal = weighted_signal_combination(
            {
                "trend": trend_signals,
                "mean_reversion": mean_reversion_signals,
                "momentum": momentum_signals,
                "volatility": volatility_signals,
                "stat_arb": stat_arb_signals,
            },
            strategy_weights,
        )

        # 为此股票代码生成详细分析报告
        technical_analysis[ticker] = {
            "signal": combined_signal["signal"],
            "confidence": round(combined_signal["confidence"] * 100),
            "reasoning": {
                "trend_following": {
                    "signal": trend_signals["signal"],
                    "confidence": round(trend_signals["confidence"] * 100),
                    "metrics": normalize_pandas(trend_signals["metrics"]),
                },
                "mean_reversion": {
                    "signal": mean_reversion_signals["signal"],
                    "confidence": round(mean_reversion_signals["confidence"] * 100),
                    "metrics": normalize_pandas(mean_reversion_signals["metrics"]),
                },
                "momentum": {
                    "signal": momentum_signals["signal"],
                    "confidence": round(momentum_signals["confidence"] * 100),
                    "metrics": normalize_pandas(momentum_signals["metrics"]),
                },
                "volatility": {
                    "signal": volatility_signals["signal"],
                    "confidence": round(volatility_signals["confidence"] * 100),
                    "metrics": normalize_pandas(volatility_signals["metrics"]),
                },
                "statistical_arbitrage": {
                    "signal": stat_arb_signals["signal"],
                    "confidence": round(stat_arb_signals["confidence"] * 100),
                    "metrics": normalize_pandas(stat_arb_signals["metrics"]),
                },
            },
        }
        progress.update_status(agent_id, ticker, "完成", analysis=json.dumps(technical_analysis, indent=4))

    # 创建技术分析师消息
    message = HumanMessage(
        content=json.dumps(technical_analysis),
        name=agent_id,
    )

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(technical_analysis, "技术分析师")

    # 将信号添加到分析师信号列表
    state["data"]["analyst_signals"][agent_id] = technical_analysis

    progress.update_status(agent_id, None, "完成")

    return {
        "messages": state["messages"] + [message],
        "data": data,
    }


def convert_prices_to_df(price_data):
    """
    将价格数据转换为DataFrame,适应原有的技术分析函数
    
    Args:
        price_data: 来自统一数据适配器的价格数据列表
        
    Returns:
        pd.DataFrame: 包含OHLCV数据的DataFrame
    """
    if not price_data:
        return pd.DataFrame()
    
    # 构建DataFrame
    df_data = []
    for price in price_data:
        df_data.append({
            'date': None,  # date字段不支持
            'open': None,  # open字段不支持
            'high': None,  # high字段不支持
            'low': None,  # low字段不支持
            'current_price': None,  # close字段不支持
            'volume': None  # volume字段不支持
        })
    
    df = pd.DataFrame(df_data)
    if not df.empty and 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)
    
    return df


def calculate_trend_signals(prices_df):
    """
    使用多个时间框架和指标的高级趋势跟踪策略
    """
    if prices_df.empty or len(prices_df) < 55:
        return {"signal": "neutral", "confidence": 0.0, "metrics": {}}
        
    # 计算多个时间框架的EMA
    ema_8 = calculate_ema(prices_df, 8)
    ema_21 = calculate_ema(prices_df, 21)
    ema_55 = calculate_ema(prices_df, 55)

    # 计算ADX以确定趋势强度
    adx = calculate_adx(prices_df, 14)

    # 确定趋势方向和强度
    short_trend = ema_8 > ema_21
    medium_trend = ema_21 > ema_55

    # 结合信号与置信度权重
    trend_strength = adx["adx"].iloc[-1] / 100.0

    if short_trend.iloc[-1] and medium_trend.iloc[-1]:
        signal = "bullish"
        confidence = trend_strength
    elif not short_trend.iloc[-1] and not medium_trend.iloc[-1]:
        signal = "bearish"
        confidence = trend_strength
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "adx": safe_float(adx["adx"].iloc[-1]),
            "trend_strength": safe_float(trend_strength),
        },
    }


def calculate_mean_reversion_signals(prices_df):
    """
    使用统计测量和布林带的均值回归策略
    """
    if prices_df.empty or len(prices_df) < 50:
        return {"signal": "neutral", "confidence": 0.0, "metrics": {}}
    
    # 计算价格相对于移动平均线的z-score
    ma_50 = prices_df["current_price"].rolling(window=50).mean()
    std_50 = prices_df["current_price"].rolling(window=50).std()
    z_score = (prices_df["current_price"] - ma_50) / std_50

    # 计算布林带
    bb_upper, bb_lower = calculate_bollinger_bands(prices_df)

    # 计算多个时间框架的RSI
    rsi_14 = calculate_rsi(prices_df, 14)
    rsi_28 = calculate_rsi(prices_df, 28)

    # 均值回归信号
    price_vs_bb = (prices_df["current_price"].iloc[-1] - bb_lower.iloc[-1]) / (bb_upper.iloc[-1] - bb_lower.iloc[-1])

    # 组合信号
    if z_score.iloc[-1] < -2 and price_vs_bb < 0.2:
        signal = "bullish"
        confidence = min(abs(z_score.iloc[-1]) / 4, 1.0)
    elif z_score.iloc[-1] > 2 and price_vs_bb > 0.8:
        signal = "bearish"
        confidence = min(abs(z_score.iloc[-1]) / 4, 1.0)
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "z_score": safe_float(z_score.iloc[-1]),
            "price_vs_bb": safe_float(price_vs_bb),
            "rsi_14": safe_float(rsi_14.iloc[-1]),
            "rsi_28": safe_float(rsi_28.iloc[-1]),
        },
    }


def calculate_momentum_signals(prices_df):
    """
    多因子动量策略
    """
    if prices_df.empty or len(prices_df) < 126:
        return {"signal": "neutral", "confidence": 0.0, "metrics": {}}
    
    # 价格动量
    returns = prices_df["current_price"].pct_change()
    mom_1m = returns.rolling(21).sum()
    mom_3m = returns.rolling(63).sum()
    mom_6m = returns.rolling(126).sum()

    # 成交量动量
    volume_ma = prices_df["volume"].rolling(21).mean()
    volume_momentum = prices_df["volume"] / volume_ma

    # 相对强度
    # (在实际实施中会与市场/板块比较)

    # 计算动量得分
    momentum_score = (0.4 * mom_1m + 0.3 * mom_3m + 0.3 * mom_6m).iloc[-1]

    # 成交量确认
    volume_confirmation = volume_momentum.iloc[-1] > 1.0

    if momentum_score > 0.05 and volume_confirmation:
        signal = "bullish"
        confidence = min(abs(momentum_score) * 5, 1.0)
    elif momentum_score < -0.05 and volume_confirmation:
        signal = "bearish"
        confidence = min(abs(momentum_score) * 5, 1.0)
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "momentum_1m": safe_float(mom_1m.iloc[-1]),
            "momentum_3m": safe_float(mom_3m.iloc[-1]),
            "momentum_6m": safe_float(mom_6m.iloc[-1]),
            "volume_momentum": safe_float(volume_momentum.iloc[-1]),
        },
    }


def calculate_volatility_signals(prices_df):
    """
    基于波动率的交易策略
    """
    if prices_df.empty or len(prices_df) < 63:
        return {"signal": "neutral", "confidence": 0.0, "metrics": {}}
    
    # 计算各种波动率指标
    returns = prices_df["current_price"].pct_change()

    # 历史波动率
    hist_vol = returns.rolling(21).std() * math.sqrt(252)

    # 波动率制度检测
    vol_ma = hist_vol.rolling(63).mean()
    vol_regime = hist_vol / vol_ma

    # 波动率均值回归
    vol_z_score = (hist_vol - vol_ma) / hist_vol.rolling(63).std()

    # ATR比率
    atr = calculate_atr(prices_df)
    atr_ratio = atr / prices_df["current_price"]

    # 根据波动率制度生成信号
    current_vol_regime = vol_regime.iloc[-1]
    vol_z = vol_z_score.iloc[-1]

    if current_vol_regime < 0.8 and vol_z < -1:
        signal = "bullish"  # 低波动率制度,扩张潜力
        confidence = min(abs(vol_z) / 3, 1.0)
    elif current_vol_regime > 1.2 and vol_z > 1:
        signal = "bearish"  # 高波动率制度,收缩潜力
        confidence = min(abs(vol_z) / 3, 1.0)
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "historical_volatility": safe_float(hist_vol.iloc[-1]),
            "volatility_regime": safe_float(current_vol_regime),
            "volatility_z_score": safe_float(vol_z),
            "atr_ratio": safe_float(atr_ratio.iloc[-1]),
        },
    }


def calculate_stat_arb_signals(prices_df):
    """
    基于价格行为分析的统计套利信号
    """
    if prices_df.empty or len(prices_df) < 63:
        return {"signal": "neutral", "confidence": 0.0, "metrics": {}}
    
    # 计算价格分布统计
    returns = prices_df["current_price"].pct_change()

    # 偏度和峰度
    skew = returns.rolling(63).skew()
    kurt = returns.rolling(63).kurt()

    # 使用Hurst指数测试均值回归
    hurst = calculate_hurst_exponent(prices_df["current_price"])

    # 相关性分析
    # (在实际实施中会包括与相关证券的相关性)

    # 基于统计特性生成信号
    if hurst < 0.4 and skew.iloc[-1] > 1:
        signal = "bullish"
        confidence = (0.5 - hurst) * 2
    elif hurst < 0.4 and skew.iloc[-1] < -1:
        signal = "bearish"
        confidence = (0.5 - hurst) * 2
    else:
        signal = "neutral"
        confidence = 0.5

    return {
        "signal": signal,
        "confidence": confidence,
        "metrics": {
            "hurst_exponent": safe_float(hurst),
            "skewness": safe_float(skew.iloc[-1]),
            "kurtosis": safe_float(kurt.iloc[-1]),
        },
    }


def weighted_signal_combination(signals, weights):
    """
    使用加权方法组合多个交易信号
    """
    # 将信号转换为数值
    signal_values = {"bullish": 1, "neutral": 0, "bearish": -1}

    weighted_sum = 0
    total_confidence = 0

    for strategy, signal in signals.items():
        numeric_signal = signal_values[signal["signal"]]
        weight = weights[strategy]
        confidence = signal["confidence"]

        weighted_sum += numeric_signal * weight * confidence
        total_confidence += weight * confidence

    # 标准化加权和
    if total_confidence > 0:
        final_score = weighted_sum / total_confidence
    else:
        final_score = 0

    # 转换回信号
    if final_score > 0.2:
        signal = "bullish"
    elif final_score < -0.2:
        signal = "bearish"
    else:
        signal = "neutral"

    return {"signal": signal, "confidence": abs(final_score)}


def normalize_pandas(obj):
    """将pandas Series/DataFrames转换为原始Python类型"""
    if isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict("records")
    elif isinstance(obj, dict):
        return {k: normalize_pandas(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [normalize_pandas(item) for item in obj]
    return obj


# ── 辅助技术指标计算函数 ──
def calculate_rsi(prices_df: pd.DataFrame, period: int = 14) -> pd.Series:
    delta = prices_df["current_price"].diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_bollinger_bands(prices_df: pd.DataFrame, window: int = 20) -> tuple[pd.Series, pd.Series]:
    sma = prices_df["current_price"].rolling(window).mean()
    std_dev = prices_df["current_price"].rolling(window).std()
    upper_band = sma + (std_dev * 2)
    lower_band = sma - (std_dev * 2)
    return upper_band, lower_band


def calculate_ema(df: pd.DataFrame, window: int) -> pd.Series:
    """
    计算指数移动平均线

    Args:
        df: 包含价格数据的DataFrame
        window: EMA周期

    Returns:
        pd.Series: EMA值
    """
    return df["current_price"].ewm(span=window, adjust=False).mean()


def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    计算平均方向指数(ADX)

    Args:
        df: 包含OHLC数据的DataFrame
        period: 计算周期

    Returns:
        DataFrame: 包含ADX值
    """
    # 计算真实范围
    df = df.copy()
    df["high_low"] = df["high"] - df["low"]
    df["high_close"] = abs(df["high"] - df["current_price"].shift())
    df["low_close"] = abs(df["low"] - df["current_price"].shift())
    df["tr"] = df[["high_low", "high_close", "low_close"]].max(axis=1)

    # 计算方向移动
    df["up_move"] = df["high"] - df["high"].shift()
    df["down_move"] = df["low"].shift() - df["low"]

    df["plus_dm"] = np.where((df["up_move"] > df["down_move"]) & (df["up_move"] > 0), df["up_move"], 0)
    df["minus_dm"] = np.where((df["down_move"] > df["up_move"]) & (df["down_move"] > 0), df["down_move"], 0)

    # 计算ADX
    df["+di"] = 100 * (df["plus_dm"].ewm(span=period).mean() / df["tr"].ewm(span=period).mean())
    df["-di"] = 100 * (df["minus_dm"].ewm(span=period).mean() / df["tr"].ewm(span=period).mean())
    df["dx"] = 100 * abs(df["+di"] - df["-di"]) / (df["+di"] + df["-di"])
    df["adx"] = df["dx"].ewm(span=period).mean()

    return df[["adx", "+di", "-di"]]


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算平均真实范围

    Args:
        df: 包含OHLC数据的DataFrame
        period: ATR计算周期

    Returns:
        pd.Series: ATR值
    """
    high_low = df["high"] - df["low"]
    high_close = abs(df["high"] - df["current_price"].shift())
    low_close = abs(df["low"] - df["current_price"].shift())

    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)

    return true_range.rolling(period).mean()


def calculate_hurst_exponent(price_series: pd.Series, max_lag: int = 20) -> float:
    """
    计算Hurst指数以确定时间序列的长期记忆
    H < 0.5: 均值回归序列
    H = 0.5: 随机游走
    H > 0.5: 趋势序列

    Args:
        price_series: 价格数据
        max_lag: R/S计算的最大滞后

    Returns:
        float: Hurst指数
    """
    lags = range(2, max_lag)
    # 添加小的epsilon以避免log(0)
    tau = [max(1e-8, np.sqrt(np.std(np.subtract(price_series[lag:], price_series[:-lag])))) for lag in lags]

    # 从线性拟合返回Hurst指数
    try:
        reg = np.polyfit(np.log(lags), np.log(tau), 1)
        return reg[0]  # Hurst指数是斜率
    except (ValueError, RuntimeWarning):
        # 如果计算失败,返回0.5(随机游走)
        return 0.5
