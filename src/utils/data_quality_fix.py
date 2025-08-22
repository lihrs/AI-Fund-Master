# -*- coding: utf-8 -*-
"""
数据质量修复模块

解决分析师算法中的数据质量问题，包括：
1. 数据验证和清洗
2. 缺失数据处理
3. 异常值检测和修正
4. 数据一致性检查
5. 错误处理和重试机制
"""

from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum

# 配置日志
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class DataQualityLevel(Enum):
    """数据质量等级"""
    EXCELLENT = "excellent"  # 90-100%
    GOOD = "good"           # 70-89%
    FAIR = "fair"           # 50-69%
    POOR = "poor"           # 30-49%
    CRITICAL = "critical"   # 0-29%


@dataclass
class DataQualityReport:
    """数据质量报告"""
    ticker: str
    overall_score: float
    quality_level: DataQualityLevel
    completeness_score: float
    accuracy_score: float
    consistency_score: float
    missing_fields: List[str]
    invalid_fields: List[str]
    warnings: List[str]
    recommendations: List[str]


class DataQualityFixer:
    """数据质量修复器"""
    
    def __init__(self):
        self.required_fields = {
            'financial_metrics': [
                'revenue', 'net_income', 'operating_income', 'total_assets',
                'shareholders_equity', 'operating_cash_flow'
            ],
            'line_items': [
                'revenue', 'net_income', 'total_assets', 'shareholders_equity'
            ],
            'market_data': ['market_cap'],
            'price_data': ['close', 'volume'],
        }
        
        self.optional_fields = {
            'financial_metrics': [
                'gross_profit', 'ebitda', 'free_cash_flow', 'total_debt',
                'current_assets', 'current_liabilities'
            ],
            'calculated_metrics': [
                'enterprise_value', 'ev_to_ebitda', 'ev_to_sales',
                'price_to_book', 'price_to_sales', 'debt_to_equity_ratio'
            ]
        }
        
        # 数据范围验证规则
        self.validation_rules = {
            'revenue': {'min': 0, 'max': 1e12},
            'net_income': {'min': -1e11, 'max': 1e11},
            'market_cap': {'min': 1e6, 'max': 1e13},
            'total_assets': {'min': 0, 'max': 1e13},
            'shareholders_equity': {'min': -1e12, 'max': 1e12},
            'operating_cash_flow': {'min': -1e11, 'max': 1e11},
            'pe_ratio': {'min': 0, 'max': 1000},  # PE比率不能为负
            'pb_ratio': {'min': 0, 'max': 100},
            'debt_to_equity_ratio': {'min': 0, 'max': 10}  # 债务权益比超过10通常被认为过高
        }
    
    def validate_data_completeness(self, data: Dict[str, Any], data_type: str) -> Tuple[float, List[str]]:
        """验证数据完整性"""
        required = self.required_fields.get(data_type, [])
        if not required:
            return 1.0, []
        
        missing_fields = []
        valid_count = 0
        
        for field in required:
            value = data.get(field)
            if value is None or value == 0 or (isinstance(value, str) and not value.strip()):
                missing_fields.append(field)
            else:
                valid_count += 1
        
        completeness_score = valid_count / len(required) if required else 1.0
        return completeness_score, missing_fields
    
    def validate_data_accuracy(self, data: Dict[str, Any]) -> Tuple[float, List[str]]:
        """验证数据准确性"""
        invalid_fields = []
        valid_count = 0
        total_count = 0
        
        for field, value in data.items():
            if field in self.validation_rules and value is not None:
                total_count += 1
                rules = self.validation_rules[field]
                
                try:
                    numeric_value = float(value)
                    if rules['min'] <= numeric_value <= rules['max']:
                        valid_count += 1
                    else:
                        invalid_fields.append(f"{field}: {value} (范围: {rules['min']}-{rules['max']})")
                except (ValueError, TypeError):
                    invalid_fields.append(f"{field}: {value} (无法转换为数字)")
        
        accuracy_score = valid_count / total_count if total_count > 0 else 1.0
        return accuracy_score, invalid_fields
    
    def validate_data_consistency(self, data: Dict[str, Any]) -> Tuple[float, List[str]]:
        """验证数据一致性"""
        warnings = []
        consistency_issues = 0
        total_checks = 0
        
        # 检查基本财务关系
        revenue = data.get('revenue', 0)
        net_income = data.get('net_income', 0)
        total_assets = data.get('total_assets', 0)
        shareholders_equity = data.get('shareholders_equity', 0)
        total_liabilities = data.get('total_liabilities', 0)
        
        # 检查1: 净利润率是否合理
        if revenue > 0 and net_income != 0:
            total_checks += 1
            net_margin = net_income / revenue
            if abs(net_margin) > 1:  # 净利润率超过100%
                consistency_issues += 1
                warnings.append(f"净利润率异常: {net_margin:.2%}")
        
        # 检查2: 资产负债表平衡
        if total_assets > 0 and shareholders_equity != 0 and total_liabilities > 0:
            total_checks += 1
            balance_diff = abs(total_assets - (shareholders_equity + total_liabilities))
            if balance_diff / total_assets > 0.05:  # 差异超过5%
                consistency_issues += 1
                warnings.append(f"资产负债表不平衡，差异: {balance_diff:,.0f}")
        
        # 检查3: 市值与财务数据的合理性
        market_cap = data.get('market_cap', 0)
        if market_cap > 0 and revenue > 0:
            total_checks += 1
            ps_ratio = market_cap / revenue
            if ps_ratio > 100:  # 市销率过高
                consistency_issues += 1
                warnings.append(f"市销率异常高: {ps_ratio:.1f}")
        
        consistency_score = 1 - (consistency_issues / total_checks) if total_checks > 0 else 1.0
        return consistency_score, warnings
    
    def fix_invalid_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """修复无效数据"""
        fixed_data = data.copy()
        
        for field, value in list(fixed_data.items()):
            if field in self.validation_rules and value is not None:
                rules = self.validation_rules[field]
                
                try:
                    numeric_value = float(value)
                    if not (rules['min'] <= numeric_value <= rules['max']):
                        logger.warning(f"警告: 字段 {field} 的值 {value} 超出有效范围 [{rules['min']}-{rules['max']}]，已从数据中删除.")
                        del fixed_data[field]
                except (ValueError, TypeError):
                    logger.warning(f"警告: 字段 {field} 的值 {value} 无法转换为数字，已从数据中删除.")
                    del fixed_data[field]
        
        return fixed_data
    
    def fix_missing_data(self, data: Dict[str, Any], ticker: str) -> Dict[str, Any]:
        """修复缺失数据"""
        fixed_data = data.copy()
        
        # 使用财务计算器补充缺失的计算指标
        try:
            from src.utils.financial_calculations import financial_calculator
            enhanced_data = financial_calculator.enhance_financial_metrics(fixed_data)
            fixed_data.update(enhanced_data)
        except Exception as e:
            logger.warning(f"财务指标增强失败 {ticker}: {e}")
        
        # 特殊处理：如果缺少关键数据，尝试从其他字段推导
        self._derive_missing_fields(fixed_data)
        
        return fixed_data
    
    def _derive_missing_fields(self, data: Dict[str, Any]):
        """从现有数据推导缺失字段"""
        # 如果缺少总负债，尝试从总资产和股东权益计算
        if not data.get('total_liabilities'):
            total_assets = data.get('total_assets', 0)
            shareholders_equity = data.get('shareholders_equity', 0)
            if total_assets > 0 and shareholders_equity > 0:
                data['total_liabilities'] = total_assets - shareholders_equity
        
        # 如果缺少毛利润，尝试从收入和销售成本计算
        if not data.get('gross_profit'):
            revenue = data.get('revenue', 0)
            cost_of_revenue = data.get('cost_of_revenue', 0)
            if revenue > 0 and cost_of_revenue > 0:
                data['gross_profit'] = revenue - cost_of_revenue
        
        # 如果缺少每股收益，尝试从净利润和流通股数计算
        if not data.get('earnings_per_share'):
            net_income = data.get('net_income', 0)
            shares_outstanding = data.get('shares_outstanding', 0)
            if net_income != 0 and shares_outstanding > 0:
                data['earnings_per_share'] = net_income / shares_outstanding


# 全局数据质量修复器实例
data_quality_fixer = DataQualityFixer()