#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版报告生成器 - 生成详细的HTML智能分析报告
"""

from typing import Dict, List, Any
from datetime import datetime
import json


class EnhancedReportGenerator:
    """
    增强版报告生成器
    """
    
    def __init__(self):
        self.report_style = """
        <style>
            body {
                font-family: 'Microsoft YaHei', Arial, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
                color: #333;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background-color: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            .header {
                text-align: center;
                border-bottom: 3px solid #2c3e50;
                padding-bottom: 20px;
                margin-bottom: 30px;
            }
            .header h1 {
                color: #2c3e50;
                margin: 0;
                font-size: 2.5em;
            }
            .header .subtitle {
                color: #7f8c8d;
                margin-top: 10px;
                font-size: 1.1em;
            }
            .section {
                margin-bottom: 40px;
                padding: 20px;
                border: 1px solid #ecf0f1;
                border-radius: 6px;
                background-color: #fafafa;
            }
            .section h2 {
                color: #2c3e50;
                border-bottom: 2px solid #3498db;
                padding-bottom: 10px;
                margin-top: 0;
            }
            .section h3 {
                color: #34495e;
                margin-top: 25px;
            }
            .stock-card {
                background: white;
                border: 1px solid #ddd;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            .stock-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 15px;
                padding-bottom: 10px;
                border-bottom: 1px solid #eee;
            }
            .stock-title {
                font-size: 1.4em;
                font-weight: bold;
                color: #2c3e50;
            }
            .signal-badge {
                padding: 5px 15px;
                border-radius: 20px;
                color: #000000;
                font-weight: bold;
                font-size: 0.9em;
            }
            .signal-buy { background-color: #27ae60; }
            .signal-strong-buy { background-color: #16a085; }
            .signal-sell { background-color: #e74c3c; }
            .signal-strong-sell { background-color: #c0392b; }
            .signal-hold { background-color: #f39c12; }
            .metrics-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin: 15px 0;
            }
            .metric-item {
                background: #f8f9fa;
                padding: 12px;
                border-radius: 5px;
                border-left: 4px solid #3498db;
            }
            .metric-label {
                font-size: 0.9em;
                color: #7f8c8d;
                margin-bottom: 5px;
            }
            .metric-value {
                font-size: 1.1em;
                font-weight: bold;
                color: #2c3e50;
            }
            .analysis-section {
                margin: 20px 0;
                padding: 15px;
                background: #f8f9fa;
                border-radius: 5px;
                border-left: 4px solid #9b59b6;
            }
            .risk-indicator {
                display: inline-block;
                padding: 3px 8px;
                border-radius: 12px;
                font-size: 0.8em;
                font-weight: bold;
            }
            .risk-low { background-color: #d5f4e6; color: #27ae60; }
            .risk-medium { background-color: #fef9e7; color: #f39c12; }
            .risk-high { background-color: #fadbd8; color: #e74c3c; }
            .score-bar {
                width: 100%;
                height: 20px;
                background-color: #ecf0f1;
                border-radius: 10px;
                overflow: hidden;
                margin: 5px 0;
            }
            .score-fill {
                height: 100%;
                border-radius: 10px;
                transition: width 0.3s ease;
            }
            .score-excellent { background-color: #27ae60; }
            .score-good { background-color: #2ecc71; }
            .score-average { background-color: #f39c12; }
            .score-poor { background-color: #e74c3c; }
            .market-overview {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: #000000;
                padding: 25px;
                border-radius: 8px;
                margin-bottom: 30px;
            }
            .market-overview h2 {
                color: #000000;
                border-bottom: 2px solid rgba(255,255,255,0.3);
            }
            .overview-stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                gap: 20px;
                margin-top: 20px;
            }
            .stat-item {
                text-align: center;
                background: rgba(255,255,255,0.1);
                padding: 15px;
                border-radius: 5px;
            }
            .stat-number {
                font-size: 2em;
                font-weight: bold;
                display: block;
            }
            .stat-label {
                font-size: 0.9em;
                opacity: 0.8;
            }
            .prediction-section {
                background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
                color: #000000;
                padding: 20px;
                border-radius: 8px;
                margin: 15px 0;
            }
            .prediction-section h4 {
                color: #000000;
                margin-top: 0;
            }
            .trend-indicator {
                display: inline-block;
                padding: 5px 10px;
                background: rgba(255,255,255,0.2);
                border-radius: 15px;
                margin: 5px;
                font-size: 0.9em;
            }
            .footer {
                text-align: center;
                margin-top: 40px;
                padding-top: 20px;
                border-top: 1px solid #ecf0f1;
                color: #7f8c8d;
                font-size: 0.9em;
            }
            .error-message {
                background-color: #fadbd8;
                color: #e74c3c;
                padding: 15px;
                border-radius: 5px;
                border-left: 4px solid #e74c3c;
                margin: 10px 0;
            }
            .warning-message {
                background-color: #fef9e7;
                color: #f39c12;
                padding: 15px;
                border-radius: 5px;
                border-left: 4px solid #f39c12;
                margin: 10px 0;
            }
        </style>
        """
    
    def generate_enhanced_html_report(self, analysis_results: Dict[str, Any]) -> str:
        """
        生成增强版HTML报告
        """
        try:
            html_content = f"""
            <!DOCTYPE html>
            <html lang="zh-CN">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>智能投资分析报告</title>
                {self.report_style}
            </head>
            <body>
                <div class="container">
                    {self._generate_header(analysis_results)}
                    {self._generate_market_overview(analysis_results.get('market_overview', {}))}
                    {self._generate_individual_analysis(analysis_results.get('individual_analysis', {}))}
                    {self._generate_summary_section(analysis_results)}
                    {self._generate_footer()}
                </div>
            </body>
            </html>
            """
            
            return html_content
            
        except Exception as e:
            return self._generate_error_report(str(e))
    
    def _generate_header(self, analysis_results: Dict[str, Any]) -> str:
        """生成报告头部"""
        timestamp = analysis_results.get('analysis_timestamp', datetime.now().isoformat())
        try:
            formatted_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).strftime('%Y年%m月%d日 %H:%M:%S')
        except:
            formatted_time = timestamp
        
        total_stocks = len(analysis_results.get('individual_analysis', {}))
        
        return f"""
        <div class="header">
            <h1>智能投资分析报告</h1>
            <div class="subtitle">
                分析时间: {formatted_time} | 分析股票数量: {total_stocks}只
            </div>
            <div class="subtitle">
                作者：267278466@qq.com
            </div>
        </div>
        """
    
    def _generate_market_overview(self, market_overview: Dict[str, Any]) -> str:
        """生成市场概览"""
        if not market_overview or 'error' in market_overview:
            return '<div class="warning-message">市场概览数据不可用</div>'
        
        total_analyzed = market_overview.get('total_analyzed', 0)
        buy_signals = market_overview.get('buy_signals', 0)
        sell_signals = market_overview.get('sell_signals', 0)
        hold_signals = market_overview.get('hold_signals', 0)
        market_sentiment = market_overview.get('market_sentiment', '中性')
        technical_trend = market_overview.get('technical_trend', '震荡')
        avg_technical_score = market_overview.get('avg_technical_score', 50) or 50
        avg_sentiment_score = market_overview.get('avg_sentiment_score', 50) or 50
        
        return f"""
        <div class="market-overview">
            <h2>市场概览</h2>
            <div class="overview-stats">
                <div class="stat-item">
                    <span class="stat-number">{total_analyzed}</span>
                    <span class="stat-label">分析股票</span>
                </div>
                <div class="stat-item">
                    <span class="stat-number">{buy_signals}</span>
                    <span class="stat-label">买入信号</span>
                </div>
                <div class="stat-item">
                    <span class="stat-number">{sell_signals}</span>
                    <span class="stat-label">卖出信号</span>
                </div>
                <div class="stat-item">
                    <span class="stat-number">{hold_signals}</span>
                    <span class="stat-label">持有信号</span>
                </div>
            </div>
            <div style="margin-top: 20px;">
                <p><strong>市场情绪:</strong> {market_sentiment} | <strong>技术趋势:</strong> {technical_trend}</p>
                <p><strong>平均技术得分:</strong> {avg_technical_score:.1f} | <strong>平均情绪得分:</strong> {avg_sentiment_score:.1f}</p>
                <p><strong>市场总结:</strong> {market_overview.get('market_summary', '数据不足')}</p>
            </div>
        </div>
        """
    
    def _generate_individual_analysis(self, individual_analysis: Dict[str, Any]) -> str:
        """生成个股分析"""
        if not individual_analysis:
            return '<div class="warning-message">无个股分析数据</div>'
        
        content = '<div class="section"><h2>个股详细分析</h2>'
        
        for ticker, analysis in individual_analysis.items():
            content += self._generate_stock_card(ticker, analysis)
        
        content += '</div>'
        return content
    
    def _generate_stock_card(self, ticker: str, analysis: Dict[str, Any]) -> str:
        """生成单个股票分析卡片"""
        if 'error' in analysis:
            return f"""
            <div class="stock-card">
                <div class="stock-header">
                    <div class="stock-title">{ticker}</div>
                    <div class="signal-badge signal-hold">分析失败</div>
                </div>
                <div class="error-message">
                    错误信息: {analysis.get('error', '未知错误')}
                </div>
            </div>
            """
        
        # 获取基本信息 - 优先使用迷你投资大师的建议
        master_indicators = analysis.get('master_indicators', {})
        investment_advice = master_indicators.get('investment_advice', {})
        master_recommendation = investment_advice.get('recommendation', '')
        
        # 如果有迷你投资大师建议，优先使用；否则使用传统交易信号
        if master_recommendation:
            overall_signal = master_recommendation
            signal_strength = investment_advice.get('confidence', 0) or 0
        else:
            trading_signals = analysis.get('trading_signals', {})
            overall_signal = trading_signals.get('overall_signal', '持有') or '持有'
            signal_strength = trading_signals.get('signal_strength', 0) or 0
        
        # 确定信号样式
        signal_class = self._get_signal_class(overall_signal)
        
        # 获取价格分析
        price_analysis = analysis.get('price_analysis', {})
        current_price = price_analysis.get('current_price', 0)
        if current_price is None or (isinstance(current_price, (int, float)) and current_price <= 0):
            current_price = 0
        price_change_1d = price_analysis.get('price_change_1d', 0)
        if price_change_1d is None:
            price_change_1d = 0
        
        # 获取综合评分
        overall_score = analysis.get('overall_score', {})
        total_score = overall_score.get('total_score', 50) or 50
        rating = overall_score.get('rating', '一般') or '一般'
        
        # 获取风险评估
        risk_assessment = analysis.get('risk_assessment', {})
        risk_level = risk_assessment.get('overall_risk', '中等')
        risk_class = self._get_risk_class(risk_level)
        
        card_content = f"""
        <div class="stock-card">
            <div class="stock-header">
                <div class="stock-title">{ticker}</div>
                <div class="signal-badge {signal_class}">{overall_signal}</div>
            </div>
            
            <div class="metrics-grid">
                <div class="metric-item">
                    <div class="metric-label">当前价格</div>
                    <div class="metric-value">{current_price:.2f}元</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">日涨跌幅</div>
                    <div class="metric-value" style="color: {'#27ae60' if price_change_1d >= 0 else '#e74c3c'}">
                        {price_change_1d:+.2f}%
                    </div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">信号强度</div>
                    <div class="metric-value">{signal_strength:.0f}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">综合评分</div>
                    <div class="metric-value">{total_score:.0f} ({rating})</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">风险等级</div>
                    <div class="metric-value">
                        <span class="risk-indicator {risk_class}">{risk_level}</span>
                    </div>
                </div>
            </div>
            
            {self._generate_technical_analysis_section(analysis.get('technical_analysis', {}))}
            {self._generate_volume_analysis_section(analysis.get('volume_analysis', {}))}
            {self._generate_sentiment_analysis_section(analysis.get('sentiment_analysis', {}))}
            {self._generate_trend_prediction_section(analysis.get('trend_prediction', {}))}
            {self._generate_trading_signals_section(analysis.get('trading_signals', {}))}
            {self._generate_mini_master_analysis_section(analysis.get('master_indicators', {}))}
            
            <div class="analysis-section">
                <h4>分析摘要</h4>
                <p>{analysis.get('analysis_summary', '暂无摘要')}</p>
            </div>
        </div>
        """
        
        return card_content
    
    def _generate_technical_analysis_section(self, technical_analysis: Dict[str, Any]) -> str:
        """生成技术分析部分"""
        if not technical_analysis or 'error' in technical_analysis:
            error_msg = technical_analysis.get('error', '数据获取失败') if technical_analysis else '数据不可用'
            suggestions = technical_analysis.get('suggestions', []) if technical_analysis else []
            troubleshooting = technical_analysis.get('troubleshooting', '') if technical_analysis else ''
            fallback_analysis = technical_analysis.get('fallback_analysis', '') if technical_analysis else ''
            
            warning_content = f'<div class="warning-message">技术分析数据不可用: {error_msg}'
            if suggestions:
                warning_content += '<br><strong>建议:</strong> ' + '; '.join(suggestions)
            if troubleshooting:
                warning_content += f'<br><strong>故障排除:</strong> {troubleshooting}'
            if fallback_analysis:
                warning_content += f'<br><strong>备用分析:</strong> {fallback_analysis}'
            warning_content += '</div>'
            
            # 添加技术方法说明
            warning_content += self._generate_technical_methods_explanation()
            return warning_content
        
        technical_score = technical_analysis.get('technical_score', 50) or 50
        score_class = self._get_score_class(technical_score)
        
        moving_averages = technical_analysis.get('moving_averages', {})
        macd_analysis = technical_analysis.get('macd_analysis', {})
        rsi_analysis = technical_analysis.get('rsi_analysis', {})
        bollinger_analysis = technical_analysis.get('bollinger_analysis', {})
        kdj_analysis = technical_analysis.get('kdj_analysis', {})
        
        # 获取详细的技术指标数据
        indicators = technical_analysis.get('indicators', {})
        
        technical_content = f"""
        <div class="analysis-section">
            <h4>技术分析</h4>
            <div class="score-bar">
                <div class="score-fill {score_class}" style="width: {technical_score}%"></div>
            </div>
            <p><strong>技术得分:</strong> {technical_score:.0f}/100</p>
            
            <div class="metrics-grid">
                <!-- 均线趋势项已移除，因为无法可靠提供 -->
                <div class="metric-item">
                    <div class="metric-label">MACD信号</div>
                    <div class="metric-value">{macd_analysis.get('signal', '中性')}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">RSI水平</div>
                    <div class="metric-value">{rsi_analysis.get('level', '正常')}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">布林带位置</div>
                    <div class="metric-value">{bollinger_analysis.get('position', '中轨')}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">KDJ信号</div>
                    <div class="metric-value">{kdj_analysis.get('signal', '中性')}</div>
                </div>
            </div>
            
            <p>{technical_analysis.get('technical_summary', '暂无总结')}</p>
            
            <!-- 详细技术指标数据 -->
            <div class="technical-details" style="margin-top: 15px; padding: 10px; background-color: #f8f9fa; border-radius: 5px;">
        """
        
        # 添加具体指标数值 - 显示所有可用的技术指标
        has_indicators = False
        
        if indicators:
            # 移动平均线
            ma_values = []
            for ma_period in ['ma5', 'ma10', 'ma20', 'ma30', 'ma60']:
                if ma_period in indicators and indicators[ma_period] is not None:
                    ma_values.append(f"{ma_period.upper()}={indicators[ma_period]:.2f}")
            if ma_values:
                technical_content += f"<p><strong>移动平均线:</strong> {', '.join(ma_values)}</p>"
                has_indicators = True
            
            # MACD指标
            if 'macd' in indicators:
                macd_data = indicators['macd']
                if isinstance(macd_data, dict):
                    macd_values = []
                    for key, label in [('dif', 'DIF'), ('dea', 'DEA'), ('macd', 'MACD')]:
                        if key in macd_data and macd_data[key] is not None:
                            macd_values.append(f"{label}={macd_data[key]:.4f}")
                    if macd_values:
                        technical_content += f"<p><strong>MACD指标:</strong> {', '.join(macd_values)}</p>"
                        has_indicators = True
            
            # RSI指标
            if 'rsi' in indicators and indicators['rsi'] is not None:
                technical_content += f"<p><strong>RSI指标:</strong> {indicators['rsi']:.2f}</p>"
                has_indicators = True
            
            # 布林带
            if 'bollinger' in indicators:
                bb_data = indicators['bollinger']
                if isinstance(bb_data, dict):
                    bb_values = []
                    for key, label in [('upper', '上轨'), ('middle', '中轨'), ('lower', '下轨')]:
                        if key in bb_data and bb_data[key] is not None:
                            bb_values.append(f"{label}={bb_data[key]:.2f}")
                    if bb_values:
                        technical_content += f"<p><strong>布林带:</strong> {', '.join(bb_values)}</p>"
                        has_indicators = True
            
            # KDJ指标
            if 'kdj' in indicators:
                kdj_data = indicators['kdj']
                if isinstance(kdj_data, dict):
                    kdj_values = []
                    for key in ['k', 'd', 'j']:
                        if key in kdj_data and kdj_data[key] is not None:
                            kdj_values.append(f"{key.upper()}={kdj_data[key]:.2f}")
                    if kdj_values:
                        technical_content += f"<p><strong>KDJ指标:</strong> {', '.join(kdj_values)}</p>"
                        has_indicators = True
            
            # 成交量指标
            if 'volume' in indicators and indicators['volume'] is not None:
                technical_content += f"<p><strong>成交量:</strong> {indicators['volume']:,.0f}</p>"
                has_indicators = True
            
            # 当前价格
            if 'current_price' in indicators and indicators['current_price'] is not None:
                technical_content += f"<p><strong>当前价格:</strong> {indicators['current_price']:.2f}元</p>"
                has_indicators = True
        
        # 如果没有任何指标数据，保持空白
        # if not has_indicators:
        #     technical_content += "<p style='color: #666; font-style: italic;'>暂无详细技术指标数据</p>"
        
        technical_content += """
            </div>
        </div>
        """
        
        # 添加技术方法说明
        technical_content += self._generate_technical_methods_explanation()
        
        return technical_content
    
    def _generate_mini_master_analysis_section(self, master_indicators: Dict[str, Any]) -> str:
        """生成迷你投资大师分析部分"""
        if not master_indicators:
            return ''
        
        investment_advice = master_indicators.get('investment_advice', {})
        if not investment_advice:
            return ''
        
        recommendation = investment_advice.get('recommendation', '观望')
        confidence = investment_advice.get('confidence', 0)
        risk_level = investment_advice.get('risk_level', '中等')
        reasoning = investment_advice.get('reasoning', '暂无分析')
        master_insights = investment_advice.get('master_insights', [])
        strategy_scores = investment_advice.get('strategy_scores', {})
        
        # 获取信号样式
        signal_class = self._get_signal_class(recommendation)
        risk_class = self._get_risk_class(risk_level)
        
        content = f"""
        <div class="analysis-section" style="border: 2px solid #3498db; border-radius: 8px; padding: 15px; margin: 15px 0; background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);">
            <h4 style="color: #2c3e50; margin-bottom: 15px;">🧠 迷你投资大师分析</h4>
            
            <div class="metrics-grid">
                <div class="metric-item">
                    <div class="metric-label">投资建议</div>
                    <div class="metric-value">
                        <span class="signal-badge {signal_class}">{recommendation}</span>
                    </div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">置信度</div>
                    <div class="metric-value">{confidence:.1f}%</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">风险水平</div>
                    <div class="metric-value">
                        <span class="risk-indicator {risk_class}">{risk_level}</span>
                    </div>
                </div>
            </div>
            
            <div style="margin: 15px 0;">
                <p><strong>分析推理:</strong> {reasoning}</p>
            </div>
        """
        
        # 添加投资大师洞察
        if master_insights:
            content += """
            <div style="margin: 15px 0;">
                <h5 style="color: #2c3e50; margin-bottom: 10px;">投资大师洞察:</h5>
                <ul style="margin: 0; padding-left: 20px;">
            """
            for insight in master_insights:
                content += f"<li style='margin-bottom: 5px;'>{insight}</li>"
            content += "</ul></div>"
        
        # 添加策略得分
        if strategy_scores:
            content += """
            <div style="margin: 15px 0;">
                <h5 style="color: #2c3e50; margin-bottom: 10px;">策略得分:</h5>
                <div class="metrics-grid">
            """
            for strategy, score in strategy_scores.items():
                strategy_name = {
                    'buffett': '巴菲特策略',
                    'lynch': '彼得·林奇策略', 
                    'graham': '格雷厄姆策略',
                    'druckenmiller': '德鲁肯米勒策略',
                    'burry': '迈克尔·伯里策略'
                }.get(strategy, strategy)
                
                content += f"""
                <div class="metric-item">
                    <div class="metric-label">{strategy_name}</div>
                    <div class="metric-value">{score:.0f}</div>
                </div>
                """
            content += "</div></div>"
        
        content += "</div>"
        return content
    
    def _generate_volume_analysis_section(self, volume_analysis: Dict[str, Any]) -> str:
        """生成量价分析部分"""
        if not volume_analysis or 'error' in volume_analysis:
            return '<div class="warning-message">量价分析数据不可用</div>'
        
        volume_trend = volume_analysis.get('volume_trend', '正常') or '正常'
        volume_breakouts = volume_analysis.get('volume_breakout_signals', []) or []
        
        return f"""
        <div class="analysis-section">
            <h4>量价分析</h4>
            <div class="metrics-grid">
                <div class="metric-item">
                    <div class="metric-label">成交量趋势</div>
                    <div class="metric-value">{volume_trend}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">突破信号</div>
                    <div class="metric-value">{', '.join(volume_breakouts) if volume_breakouts else '无'}</div>
                </div>
            </div>
            <p><strong>量价总结:</strong> {volume_analysis.get('volume_analysis_summary', '暂无总结')}</p>
        </div>
        """
    
    def _generate_sentiment_analysis_section(self, sentiment_analysis: Dict[str, Any]) -> str:
        """生成市场情绪分析部分"""
        if not sentiment_analysis or 'error' in sentiment_analysis:
            return '<div class="warning-message">市场情绪分析数据不可用</div>'
        
        sentiment_score = sentiment_analysis.get('sentiment_score', 50) or 50
        sentiment_trend = sentiment_analysis.get('sentiment_trend', '中性') or '中性'
        money_flow_analysis = sentiment_analysis.get('money_flow_analysis', {})
        
        return f"""
        <div class="analysis-section">
            <h4>市场情绪分析</h4>
            <div class="metrics-grid">
                <div class="metric-item">
                    <div class="metric-label">情绪得分</div>
                    <div class="metric-value">{sentiment_score:.0f}/100</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">情绪趋势</div>
                    <div class="metric-value">{sentiment_trend}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">资金流向</div>
                    <div class="metric-value">{money_flow_analysis.get('trend', '未知')}</div>
                </div>
            </div>
            <p><strong>情绪总结:</strong> {sentiment_analysis.get('sentiment_summary', '暂无总结')}</p>
        </div>
        """
    
    def _generate_trend_prediction_section(self, trend_prediction: Dict[str, Any]) -> str:
        """生成趋势预测部分"""
        if not trend_prediction or 'error' in trend_prediction:
            return '<div class="warning-message">趋势预测数据不可用</div>'
        
        short_term = trend_prediction.get('short_term_trend', '未知') or '未知'
        medium_term = trend_prediction.get('medium_term_trend', '未知') or '未知'
        long_term = trend_prediction.get('long_term_trend', '未知') or '未知'
        confidence = trend_prediction.get('confidence_level', 50) or 50
        
        return f"""
        <div class="prediction-section">
            <h4>趋势预测</h4>
            <div style="margin: 15px 0;">
                <span class="trend-indicator">短期: {short_term}</span>
                <span class="trend-indicator">中期: {medium_term}</span>
                <span class="trend-indicator">长期: {long_term}</span>
            </div>
            <p><strong>预测置信度:</strong> {confidence:.0f}%</p>
            <p><strong>预测总结:</strong> {trend_prediction.get('prediction_summary', '暂无总结')}</p>
        </div>
        """
    
    def _generate_trading_signals_section(self, trading_signals: Dict[str, Any]) -> str:
        """生成交易信号部分"""
        if not trading_signals or 'error' in trading_signals:
            return '<div class="warning-message">交易信号数据不可用</div>'
        
        stop_loss = trading_signals.get('stop_loss', 0) or 0
        take_profit = trading_signals.get('take_profit', 0) or 0
        position_sizing = trading_signals.get('position_sizing', '中等') or '中等'
        time_horizon = trading_signals.get('time_horizon', '中期') or '中期'
        
        return f"""
        <div class="analysis-section">
            <h4>交易建议</h4>
            <div class="metrics-grid">
                <div class="metric-item">
                    <div class="metric-label">建议仓位</div>
                    <div class="metric-value">{position_sizing}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">持有周期</div>
                    <div class="metric-value">{time_horizon}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">止损价位</div>
                    <div class="metric-value">{stop_loss:.2f}元</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">止盈价位</div>
                    <div class="metric-value">{take_profit:.2f}元</div>
                </div>
            </div>
            <p><strong>交易策略:</strong> {trading_signals.get('trading_strategy', '暂无策略')}</p>
        </div>
        """
    
    def _generate_summary_section(self, analysis_results: Dict[str, Any]) -> str:
        """生成总结部分"""
        analysis_summary = analysis_results.get('analysis_summary', '暂无总结')
        
        return f"""
        <div class="section">
            <h2>分析总结</h2>
            <p style="font-size: 1.1em; line-height: 1.6;">{analysis_summary}</p>
        </div>
        """
    
    def _generate_technical_methods_explanation(self) -> str:
        """生成技术方法说明"""
        return """
        <div class="technical-methods" style="margin-top: 20px; padding: 15px; background-color: #e8f4fd; border-left: 4px solid #3498db; border-radius: 5px;">
            <h5 style="color: #2c3e50; margin-top: 0;">🧠 迷你投资大师分析说明</h5>
            <div style="font-size: 0.9em; line-height: 1.5;">
                <p><strong>迷你投资大师是一个智能投资分析系统，融合了多位投资大师的核心理念：</strong></p>
                <ul style="margin: 10px 0; padding-left: 20px;">
                    <li><strong>巴菲特价值投资：</strong>关注企业内在价值，寻找被低估的优质公司，适合长期持有</li>
                    <li><strong>彼得·林奇成长投资：</strong>发现高成长潜力的公司，重视业绩增长和市场前景</li>
                    <li><strong>格雷厄姆安全边际：</strong>强调投资安全性，通过深度价值分析降低投资风险</li>
                    <li><strong>德鲁肯米勒趋势投资：</strong>把握市场趋势和宏观经济变化，灵活调整投资策略</li>
                    <li><strong>迈克尔·伯里逆向投资：</strong>在市场恐慌时发现机会，敢于逆向思考和投资</li>
                </ul>
                <p><strong>分析特色：</strong></p>
                <ul style="margin: 10px 0; padding-left: 20px;">
                    <li><strong>多策略融合：</strong>综合运用价值、成长、趋势等多种投资策略</li>
                    <li><strong>智能评分：</strong>基于技术指标和市场数据，为每种策略提供量化评分</li>
                    <li><strong>风险控制：</strong>提供置信度和风险等级评估，帮助投资者理性决策</li>
                    <li><strong>实时分析：</strong>基于最新市场数据，提供及时的投资建议和策略调整</li>
                </ul>
                <p style="color: #7f8c8d; font-size: 0.85em; margin-top: 15px;">
                    <strong>注意：</strong>迷你投资大师分析仅供参考，不构成投资建议。投资有风险，决策需谨慎。
                </p>
            </div>
        </div>
        """
    
    def _generate_footer(self) -> str:
        """生成页脚"""
        return f"""
        <div class="footer">
            <p>本报告由AI智能分析系统生成，仅供参考，不构成投资建议</p>
            <p>生成时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}</p>
        </div>
        """
    
    def _generate_error_report(self, error_message: str) -> str:
        """生成错误报告"""
        return f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <title>报告生成失败</title>
            {self.report_style}
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>报告生成失败</h1>
                </div>
                <div class="error-message">
                    <h3>错误信息:</h3>
                    <p>{error_message}</p>
                </div>
                {self._generate_footer()}
            </div>
        </body>
        </html>
        """
    
    def _get_signal_class(self, signal: str) -> str:
        """获取信号样式类"""
        signal_map = {
            '强烈买入': 'signal-strong-buy',
            '买入': 'signal-buy',
            '谨慎买入': 'signal-buy',
            '持有': 'signal-hold',
            '观望': 'signal-hold',
            '谨慎卖出': 'signal-sell',
            '卖出': 'signal-sell',
            '强烈卖出': 'signal-strong-sell'
        }
        return signal_map.get(signal, 'signal-hold')
    
    def _get_risk_class(self, risk_level: str) -> str:
        """获取风险等级样式类"""
        risk_map = {
            '低': 'risk-low',
            '中': 'risk-medium',
            '中等': 'risk-medium',
            '高': 'risk-high'
        }
        return risk_map.get(risk_level, 'risk-medium')
    
    def _get_score_class(self, score: float) -> str:
        """获取得分样式类"""
        if score >= 80:
            return 'score-excellent'
        elif score >= 60:
            return 'score-good'
        elif score >= 40:
            return 'score-average'
        else:
            return 'score-poor'


# 创建全局报告生成器实例
enhanced_report_generator = EnhancedReportGenerator()


# 兼容性函数
def generate_enhanced_html_report(analysis_results: Dict[str, Any]) -> str:
    """
    生成增强版HTML报告（兼容性接口）
    """
    return enhanced_report_generator.generate_enhanced_html_report(analysis_results)