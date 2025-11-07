"""
基金分析工具 - 为普通投资者提供实用的基金分析功能
Fund Analyzer - Practical fund analysis for retail investors

核心功能：
1. 基金评分系统
2. 风险评估
3. 收益分析
4. 基金经理分析
5. 持仓分析
6. 同类对比
"""

import sqlite3
import gzip
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from config import DB_PATH, COMPRESSED_DB_PATH
import tempfile
import shutil
import atexit


class FundAnalyzer:
    """基金分析器 - 提供多维度的基金分析"""
    
    def __init__(self, db_path: Path = None):
        """初始化分析器"""
        self.db_path = db_path or DB_PATH
        
        # 判断是否为压缩文件（根据文件扩展名）
        self.is_compressed = str(self.db_path).endswith('.gz')
        
        # 如果是压缩文件，一次性解压到临时文件
        self._temp_db_path = None
        if self.is_compressed:
            self._extract_database()
        
        # 创建性能索引（提升查询速度）
        self._create_indexes()
    
    def _extract_database(self):
        """解压数据库到临时文件（只执行一次）"""
        if self._temp_db_path is None:
            # 创建临时文件
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            self._temp_db_path = temp_file.name
            temp_file.close()
            
            # 解压数据库
            with gzip.open(self.db_path, 'rb') as f_in:
                with open(self._temp_db_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # 注册清理函数
            atexit.register(self._cleanup)
    
    def _cleanup(self):
        """清理临时文件"""
        if self._temp_db_path and Path(self._temp_db_path).exists():
            try:
                Path(self._temp_db_path).unlink()
            except:
                pass
    
    def _connect(self):
        """连接数据库（支持.gz压缩格式）"""
        if self.is_compressed:
            return sqlite3.connect(self._temp_db_path)
        else:
            return sqlite3.connect(str(self.db_path))
    
    def _create_indexes(self):
        """创建数据库索引以提升查询性能"""
        try:
            conn = self._connect()
            cursor = conn.cursor()
            
            # 基金净值表索引（最重要 - 加速收益计算）
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fund_nav_ts_code_date 
                ON fund_nav(ts_code, nav_date)
            """)
            
            # 基金持仓表索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fund_portfolio_ts_code_date 
                ON fund_portfolio(ts_code, end_date)
            """)
            
            # 基金基本信息索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fund_basic_status 
                ON fund_basic(status)
            """)
            
            conn.commit()
            conn.close()
        except Exception as e:
            # 忽略错误（可能索引已存在）
            pass
    
    def check_cache_status(self) -> dict:
        """检查预计算缓存状态"""
        conn = self._connect()
        cursor = conn.cursor()
        
        try:
            # 检查缓存表是否存在
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='fund_returns_cache'
            """)
            
            if not cursor.fetchone():
                conn.close()
                return {
                    "has_cache": False,
                    "message": "缓存表不存在"
                }
            
            # 检查所有历史缓存（不限制日期）
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT ts_code) as fund_count,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT year) as year_count,
                    MIN(year) as min_year,
                    MAX(year) as max_year,
                    MAX(computed_date) as last_computed
                FROM fund_returns_cache
            """)
            
            row = cursor.fetchone()
            fund_count, record_count, year_count, min_year, max_year, last_computed = row
            
            if fund_count == 0:
                conn.close()
                return {
                    "has_cache": False,
                    "message": "无缓存数据"
                }
            
            # 获取所有可用年份（不限制日期）
            cursor.execute("""
                SELECT DISTINCT year
                FROM fund_returns_cache
                ORDER BY year DESC
            """)
            
            available_years = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            return {
                "has_cache": True,
                "fund_count": fund_count,
                "record_count": record_count,
                "year_count": year_count,
                "available_years": available_years,
                "year_range": f"{min_year}-{max_year}",
                "last_computed": str(last_computed),
                "message": f"找到 {fund_count} 只基金的预计算数据（最后更新：{last_computed}）"
            }
            
        except Exception as e:
            conn.close()
            return {
                "has_cache": False,
                "message": f"检查缓存失败: {str(e)}"
            }
    
    def batch_get_cached_returns(
        self, 
        ts_codes: List[str], 
        years: List[str] = None,
        fallback_to_realtime: bool = True
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """
        批量从缓存获取年度收益
        
        参数:
            ts_codes: 基金代码列表
            years: 年份列表（None表示获取所有可用年份）
            fallback_to_realtime: 如果缓存缺失，是否自动切换到实时计算
        
        返回:
            {ts_code: {year: return_rate}}
        """
        from datetime import date
        
        if not ts_codes:
            return {}
        
        conn = self._connect()
        cursor = conn.cursor()
        today = date.today()
        
        try:
            # 检查缓存表是否存在
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='fund_returns_cache'
            """)
            
            if not cursor.fetchone():
                # 缓存表不存在，使用实时计算
                conn.close()
                return self.batch_calculate_year_returns(ts_codes, years or ['2025', '2024', '2023'])
            
            # 如果没有指定年份，获取所有可用年份
            if years is None:
                cursor.execute("""
                    SELECT DISTINCT year
                    FROM fund_returns_cache
                    WHERE computed_date = ?
                    ORDER BY year DESC
                """, (today,))
                years = [row[0] for row in cursor.fetchall()]
                
                if not years:
                    # 无缓存数据，使用实时计算
                    conn.close()
                    return self.batch_calculate_year_returns(ts_codes, ['2025', '2024', '2023'])
            
            # 批量查询缓存
            placeholders_codes = ','.join(['?' for _ in ts_codes])
            placeholders_years = ','.join(['?' for _ in years])
            
            query = f"""
                SELECT ts_code, year, return_rate, MAX(computed_date) as latest_date
                FROM fund_returns_cache
                WHERE ts_code IN ({placeholders_codes})
                  AND year IN ({placeholders_years})
                GROUP BY ts_code, year
            """
            
            cursor.execute(query, (*ts_codes, *years))
            
            # 初始化结果
            result = {ts_code: {year: None for year in years} for ts_code in ts_codes}
            
            # 填充缓存数据
            for ts_code, year, return_rate, _ in cursor.fetchall():
                if ts_code in result:
                    result[ts_code][year] = return_rate
            
            conn.close()
            
            # 检查是否有缺失数据
            missing_count = sum(
                1 for ts_code in ts_codes 
                for year in years 
                if result[ts_code][year] is None
            )
            
            if missing_count > 0 and fallback_to_realtime:
                # 🔥 对缺失数据进行实时计算补充
                missing_funds = {}
                for ts_code in ts_codes:
                    missing_years = [year for year in years if result[ts_code][year] is None]
                    if missing_years:
                        missing_funds[ts_code] = missing_years
                
                if missing_funds:
                    # 只对缺失的数据进行实时计算
                    print(f"[WARN] cache miss: {missing_count} items, falling back to realtime computation...")
                    missing_codes = list(missing_funds.keys())
                    missing_data = self.batch_calculate_year_returns(missing_codes, years)
                    
                    # 补充到结果中
                    for ts_code, year_returns in missing_data.items():
                        for year, return_rate in year_returns.items():
                            if result[ts_code][year] is None:
                                result[ts_code][year] = return_rate
            elif missing_count > 0 and not fallback_to_realtime:
                # 缓存缺失但未启用实时补充，仅记录调试信息
                pass # Removed print statement

            return result
            
        except Exception as e:
            conn.close()
            print(f"从缓存读取失败: {e}")
            # 降级到实时计算
            return self.batch_calculate_year_returns(ts_codes, years or ['2025', '2024', '2023'])
    
    def get_year_end_nav(self, ts_code: str) -> dict:
        """
        获取基金年末净值（每年12月的数据）
        
        参数:
            ts_code: 基金代码
        
        返回:
            {year: nav_value}
        """
        conn = self._connect()
        
        try:
            # 查询每年12月的净值数据
            query = """
                SELECT 
                    substr(nav_date, 1, 4) as year,
                    unit_nav
                FROM fund_nav
                WHERE ts_code = ?
                  AND substr(nav_date, 6, 2) = '12'
                  AND unit_nav IS NOT NULL
                ORDER BY nav_date DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(ts_code,))
            conn.close()
            
            if df.empty:
                return {}
            
            # 转换为字典 {year: nav}
            result = {}
            for _, row in df.iterrows():
                year = row['year']
                if year not in result:  # 取每年第一条（最后一个交易日）
                    result[year] = float(row['unit_nav'])
            
            return result
            
        except Exception as e:
            conn.close()
            print(f"获取年末净值失败: {e}")
            return {}
    
    # ============================================================
    # 1. 基金基础信息查询
    # ============================================================
    
    def get_fund_info(self, ts_code: str) -> Dict[str, Any]:
        """获取基金基本信息"""
        conn = self._connect()
        
        query = """
        SELECT * FROM fund_basic 
        WHERE ts_code = ?
        """
        
        df = pd.read_sql_query(query, conn, params=(ts_code,))
        conn.close()
        
        if df.empty:
            return None
        
        return df.iloc[0].to_dict()
    
    def search_funds(self, keyword: str, limit: int = 20) -> pd.DataFrame:
        """搜索基金（按名称或代码）"""
        conn = self._connect()
        
        query = """
        SELECT ts_code, name, fund_type, management, 
               found_date, list_date, status
        FROM fund_basic
        WHERE ts_code LIKE ? OR name LIKE ?
        LIMIT ?
        """
        
        pattern = f"%{keyword}%"
        df = pd.read_sql_query(query, conn, params=(pattern, pattern, limit))
        conn.close()
        
        return df
    
    # ============================================================
    # 2. 收益分析（最重要！）
    # ============================================================
    
    def calculate_returns(self, ts_code: str, periods: List[int] = None) -> Dict[str, float]:
        """
        计算基金收益率
        
        periods: 回溯期间（天数），默认 [7, 30, 90, 180, 365, 365*2, 365*3, 365*5, 365*10]
                 对应：近一周、近一月、近三月、近半年、近一年、近两年、近三年、近五年、近十年
        """
        if periods is None:
            periods = [7, 30, 90, 180, 365, 365*2, 365*3, 365*5, 365*10]
        
        conn = self._connect()
        
        # 获取净值数据（按日期倒序）
        query = """
        SELECT nav_date, unit_nav, accum_nav
        FROM fund_nav
        WHERE ts_code = ? AND unit_nav IS NOT NULL
        ORDER BY nav_date DESC
        LIMIT ?
        """
        
        max_period = max(periods) + 30  # 多取一些，确保有数据
        df = pd.read_sql_query(query, conn, params=(ts_code, max_period))
        conn.close()
        
        if df.empty or len(df) < 2:
            return {f"{p}天收益率": None for p in periods}
        
        df['nav_date'] = pd.to_datetime(df['nav_date'])
        latest_date = df['nav_date'].iloc[0]
        latest_nav = df['unit_nav'].iloc[0]
        
        returns = {}
        
        for period in periods:
            target_date = latest_date - timedelta(days=period)
            
            # 找到最接近目标日期的净值
            past_df = df[df['nav_date'] <= target_date]
            
            if not past_df.empty:
                past_nav = past_df.iloc[0]['unit_nav']
                ret = (latest_nav - past_nav) / past_nav * 100
                
                # 格式化期间名称
                if period == 7:
                    period_name = "近一周"
                elif period == 30:
                    period_name = "近一月"
                elif period == 90:
                    period_name = "近三月"
                elif period == 180:
                    period_name = "近半年"
                elif period == 365:
                    period_name = "近一年"
                elif period == 365*2:
                    period_name = "近两年"
                elif period == 365*3:
                    period_name = "近三年"
                elif period == 365*5:
                    period_name = "近五年"
                elif period == 365*10:
                    period_name = "近十年"
                else:
                    period_name = f"近{period}天"
                
                returns[period_name] = round(ret, 2)
            else:
                if period == 7:
                    period_name = "近一周"
                elif period == 30:
                    period_name = "近一月"
                elif period == 90:
                    period_name = "近三月"
                elif period == 180:
                    period_name = "近半年"
                elif period == 365:
                    period_name = "近一年"
                elif period == 365*2:
                    period_name = "近两年"
                elif period == 365*3:
                    period_name = "近三年"
                elif period == 365*5:
                    period_name = "近五年"
                elif period == 365*10:
                    period_name = "近十年"
                else:
                    period_name = f"近{period}天"
                returns[period_name] = None
        
        return returns
    
    def calculate_risk_metrics(self, ts_code: str, days: int = 365) -> Dict[str, float]:
        """
        计算风险指标
        
        返回：
        - volatility: 波动率（年化）
        - max_drawdown: 最大回撤
        - sharpe_ratio: 夏普比率（假设无风险利率3%）
        """
        conn = self._connect()
        
        query = """
        SELECT nav_date, unit_nav
        FROM fund_nav
        WHERE ts_code = ? AND unit_nav IS NOT NULL
        ORDER BY nav_date DESC
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(ts_code, days + 30))
        conn.close()
        
        if df.empty or len(df) < 30:
            return {
                "波动率": None,
                "最大回撤": None,
                "夏普比率": None
            }
        
        df = df.sort_values('nav_date')
        df['returns'] = df['unit_nav'].pct_change()
        
        # 波动率（年化）
        volatility = df['returns'].std() * np.sqrt(252) * 100  # 252个交易日
        
        # 最大回撤
        cummax = df['unit_nav'].cummax()
        drawdown = (df['unit_nav'] - cummax) / cummax * 100
        max_drawdown = drawdown.min()
        
        # 夏普比率
        annual_return = df['returns'].mean() * 252 * 100
        risk_free_rate = 3.0  # 假设无风险利率3%
        sharpe = (annual_return - risk_free_rate) / volatility if volatility != 0 else None
        
        return {
            "波动率": round(volatility, 2),
            "最大回撤": round(max_drawdown, 2),
            "夏普比率": round(sharpe, 2) if sharpe else None
        }
    
    # ============================================================
    # 3. 基金经理分析
    # ============================================================
    
    def get_fund_managers(self, ts_code: str) -> pd.DataFrame:
        """获取基金经理信息"""
        conn = self._connect()
        
        query = """
        SELECT name, gender, begin_date, end_date, resume
        FROM fund_manager
        WHERE ts_code = ?
        ORDER BY begin_date DESC
        """
        
        df = pd.read_sql_query(query, conn, params=(ts_code,))
        conn.close()
        
        return df
    
    def get_manager_experience(self, manager_name: str) -> Dict[str, Any]:
        """分析基金经理的管理经验"""
        conn = self._connect()
        
        query = """
        SELECT ts_code, begin_date, end_date
        FROM fund_manager
        WHERE name = ?
        ORDER BY begin_date
        """
        
        df = pd.read_sql_query(query, conn, params=(manager_name,))
        conn.close()
        
        if df.empty:
            return None
        
        # 计算管理年限
        df['begin_date'] = pd.to_datetime(df['begin_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])
        
        total_days = 0
        for _, row in df.iterrows():
            end = row['end_date'] if pd.notna(row['end_date']) else datetime.now()
            days = (end - row['begin_date']).days
            total_days += days
        
        return {
            "管理基金数量": len(df),
            "管理年限": round(total_days / 365, 1),
            "在管基金": len(df[df['end_date'].isna()])
        }
    
    # ============================================================
    # 4. 持仓分析
    # ============================================================
    
    def get_top_holdings(self, ts_code: str, limit: int = 10) -> pd.DataFrame:
        """获取基金前N大重仓股"""
        conn = self._connect()
        
        query = """
        SELECT ann_date, end_date, symbol, mkv, 
               stk_mkv_ratio, stk_float_ratio
        FROM fund_portfolio
        WHERE ts_code = ?
        ORDER BY ann_date DESC, stk_mkv_ratio DESC
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(ts_code, limit))
        conn.close()
        
        return df
    
    def analyze_portfolio_concentration(self, ts_code: str) -> Dict[str, Any]:
        """分析持仓集中度"""
        conn = self._connect()
        
        # 获取最新一期持仓
        query = """
        SELECT stk_mkv_ratio
        FROM fund_portfolio
        WHERE ts_code = ? 
        ORDER BY ann_date DESC, stk_mkv_ratio DESC
        LIMIT 50
        """
        
        df = pd.read_sql_query(query, conn, params=(ts_code,))
        conn.close()
        
        if df.empty:
            return None
        
        top5_ratio = df.head(5)['stk_mkv_ratio'].sum()
        top10_ratio = df.head(10)['stk_mkv_ratio'].sum()
        
        # 集中度评级
        if top5_ratio > 50:
            concentration = "高度集中"
        elif top5_ratio > 30:
            concentration = "中度集中"
        else:
            concentration = "分散"
        
        return {
            "前5大持仓占比": round(top5_ratio, 2),
            "前10大持仓占比": round(top10_ratio, 2),
            "集中度评级": concentration
        }
    
    # ============================================================
    # 5. 规模分析
    # ============================================================
    
    def get_fund_scale_trend(self, ts_code: str, periods: int = 12) -> pd.DataFrame:
        """获取基金规模变化趋势"""
        conn = self._connect()
        
        query = """
        SELECT trade_date, fd_share
        FROM fund_share
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(ts_code, periods))
        conn.close()
        
        return df
    
    def get_latest_scale(self, ts_code: str) -> Optional[float]:
        """获取最新规模（亿份）"""
        conn = self._connect()
        
        query = """
        SELECT fd_share
        FROM fund_share
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 1
        """
        
        df = pd.read_sql_query(query, conn, params=(ts_code,))
        conn.close()
        
        if df.empty:
            return None
        
        return round(df['fd_share'].iloc[0] / 100000000, 2)  # 转换为亿份
    
    # ============================================================
    # 6. 综合评分系统（重点！）
    # ============================================================
    
    def calculate_fund_score(self, ts_code: str) -> Dict[str, Any]:
        """
        计算基金综合评分（满分100分）
        
        新版评分标准（v2.1）：
        1. 收益得分（80分）：最近5年收益累加，>100%得满分80分，按比例计算，负值0分
                            不足5年按年扣减10%（每缺1年-10%）
        2. 风险得分（20分）：最近5年回撤累加，=0得满分20分，>40%得0分，按比例换算
                            不足5年按年扣减10%（每缺1年-10%）
        
        星级评定：>80分=5星，>70分=4星，>60分=3星，>50分=2星，其余=1星
        
        注意：不再查询成立日期，直接查询最近5年数据，有多少算多少
        """
        from datetime import datetime
        
        score_detail = {
            "总分": 0,
            "收益得分": 0,
            "风险得分": 0,
            "评级": "未评级",
            "数据年限": 0,
            "收益详情": {},
            "风险详情": {}
        }
        
        try:
            # 🔥 不再查询成立日期，直接查询最近5年（包括今年）
            current_year = datetime.now().year
            years_to_check = [str(year) for year in range(current_year, current_year - 5, -1)]
            
            # 例如：2025年查询 ['2025', '2024', '2023', '2022', '2021']
            
            # ============================================================
            # 1. 收益得分（80分）
            # ============================================================
            # 获取最近5年每年的收益率
            year_returns = self.batch_calculate_year_returns([ts_code], years_to_check)
            
            valid_returns = []
            if ts_code in year_returns:
                returns_data = year_returns[ts_code]
                
                for year in years_to_check:
                    if year in returns_data and returns_data[year] is not None:
                        valid_returns.append(returns_data[year])
                        score_detail["收益详情"][year] = returns_data[year]
            
            # 计算实际数据年限（有多少年数据）
            actual_years = len(valid_returns)
            score_detail["数据年限"] = actual_years
            
            if actual_years > 0:
                
                # 计算5年收益累加
                if valid_returns:
                    total_return = sum(valid_returns)
                    score_detail["收益详情"]["累计收益"] = round(total_return, 2)
                    
                    # 负值得0分
                    if total_return <= 0:
                        return_score = 0
                    else:
                        # >100%得80分，按比例计算
                        return_score = min(80, (total_return / 100) * 80)
                    
                    # 不足5年扣减：每缺1年减10%
                    missing_years = 5 - actual_years
                    if missing_years > 0:
                        penalty = missing_years * 0.15
                        return_score = return_score * (1 - penalty)
                    
                    score_detail["收益得分"] = max(0, round(return_score, 1))
            
            # ============================================================
            # 2. 风险得分（20分）- 基于年度收益稳定性
            # ============================================================
            # 🔥 新规则：根据每年的收益率评分
            # 年收益 > 20%: 4分
            # 年收益 > 0%:  3分
            # 年收益 > -10%: 2分
            # 年收益 <= -10%: 0分
            # 最多5年，累加得出最终风险分
            
            risk_points = []
            
            for year, year_return in score_detail["收益详情"].items():
                if year == "累计收益":
                    continue
                
                if year_return is not None:
                    # 根据收益率计算风险分
                    if year_return > 20:
                        points = 4
                    elif year_return > 0:
                        points = 3
                    elif year_return > -10:
                        points = 2
                    else:  # <= -10
                        points = 0
                    
                    risk_points.append(points)
                    score_detail["风险详情"][year + "风险分"] = points
            
            if risk_points:
                # 累加风险分
                total_risk_points = sum(risk_points)
                score_detail["风险详情"]["累计风险分"] = total_risk_points
                
                # 实际年数
                actual_years = len(risk_points)
                
                # 风险得分：最多5年 × 4分 = 20分
                risk_score = total_risk_points
                
                # 🔥 年限扣分：不足5年，每少1年减少20%
                missing_years = 5 - actual_years
                if missing_years > 0:
                    penalty = missing_years * 0.20  # 每年20%
                    risk_score = risk_score * (1 - penalty)
                
                score_detail["风险得分"] = max(0, round(risk_score, 1))
            
            # ============================================================
            # 3. 计算总分和评级
            # ============================================================
            score_detail["总分"] = round(
                score_detail["收益得分"] + score_detail["风险得分"], 
                1
            )
            
            # 星级评定（100分制）
            total = score_detail["总分"]
            if total > 80:
                score_detail["评级"] = "五星 ★★★★★"
            elif total > 70:
                score_detail["评级"] = "四星 ★★★★"
            elif total > 60:
                score_detail["评级"] = "三星 ★★★"
            elif total > 50:
                score_detail["评级"] = "二星 ★★"
            else:
                score_detail["评级"] = "一星 ★"
            
        except Exception as e:
            score_detail["错误"] = str(e)
            import traceback
            print(f"评分计算失败 {ts_code}: {e}")
            print(traceback.format_exc())
        
        return score_detail
    
    def batch_calculate_scores(
        self, 
        ts_codes: List[str],
        year_returns: Dict[str, Dict[str, Optional[float]]] = None
    ) -> Dict[str, Optional[float]]:
        """
        批量计算基金评分（优化版 - 与详情页保持一致）
        
        参数:
            ts_codes: 基金代码列表
            year_returns: 预先获取的年度收益数据 {ts_code: {year: return}}
        
        返回:
            {ts_code: score}
        """
        from datetime import datetime
        
        conn = self._connect()
        results = {}
        current_year = datetime.now().year
        years_to_check = [str(year) for year in range(current_year, current_year - 5, -1)]
        
        # 如果没有提供年度收益，批量获取
        if year_returns is None:
            year_returns = self.batch_get_cached_returns(ts_codes, years_to_check)
        
        # 批量计算每个基金的评分
        for ts_code in ts_codes:
            try:
                returns_data = year_returns.get(ts_code, {})
                valid_returns = []
                
                for year in years_to_check:
                    if year in returns_data and returns_data[year] is not None:
                        valid_returns.append(returns_data[year])
                
                actual_years = len(valid_returns)
                
                if actual_years == 0:
                    results[ts_code] = None
                    continue
                
                # 1. 收益得分（80分）
                total_return = sum(valid_returns)
                return_score = 0 if total_return <= 0 else min(80, (total_return / 100) * 80)
                missing_years = 5 - actual_years
                if missing_years > 0:
                    return_score *= (1 - missing_years * 0.15)
                return_score = max(0, round(return_score, 1))
                
                # 2. 风险得分（20分） - 基于年度收益稳定性
                # 🔥 新规则：根据每年的收益率评分
                # 年收益 > 20%: 4分
                # 年收益 > 0%:  3分
                # 年收益 > -10%: 2分
                # 年收益 <= -10%: 0分
                
                risk_points = []
                
                for year in years_to_check:
                    if year in returns_data and returns_data[year] is not None:
                        year_return = returns_data[year]
                        
                        # 根据收益率计算风险分
                        if year_return > 20:
                            points = 4
                        elif year_return > 0:
                            points = 3
                        elif year_return > -10:
                            points = 2
                        else:  # <= -10
                            points = 0
                        
                        risk_points.append(points)
                
                risk_score = 0
                if risk_points:
                    # 累加风险分（最多5年 × 4分 = 20分）
                    risk_score = sum(risk_points)
                    
                    # 🔥 年限扣分：不足5年，每少1年减少20%
                    actual_risk_years = len(risk_points)
                    missing_risk_years = 5 - actual_risk_years
                    if missing_risk_years > 0:
                        risk_score *= (1 - missing_risk_years * 0.20)
                    risk_score = max(0, round(risk_score, 1))
                
                # 3. 总分
                total_score = round(return_score + risk_score, 1)
                results[ts_code] = total_score
                
            except Exception as e:
                print(f"批量评分失败 {ts_code}: {e}")
                results[ts_code] = None
        
        conn.close()
        return results
    
    def _calculate_year_max_drawdown(self, ts_code: str, year: str) -> Optional[float]:
        """计算指定年份的最大回撤"""
        conn = self._connect()
        
        try:
            # 获取该年份的净值数据
            query = """
                SELECT nav_date, unit_nav
                FROM fund_nav
                WHERE ts_code = ?
                  AND substr(nav_date, 1, 4) = ?
                  AND unit_nav IS NOT NULL
                ORDER BY nav_date
            """
            
            df = pd.read_sql_query(query, conn, params=(ts_code, year))
            conn.close()
            
            if df.empty or len(df) < 2:
                return None
            
            # 计算最大回撤
            navs = df['unit_nav'].values
            peak = navs[0]
            max_drawdown = 0
            
            for nav in navs:
                if nav > peak:
                    peak = nav
                drawdown = (nav - peak) / peak * 100
                if drawdown < max_drawdown:
                    max_drawdown = drawdown
            
            return max_drawdown
            
        except Exception as e:
            conn.close()
            print(f"计算{year}年回撤失败: {e}")
            return None
    
    def check_gold_rating(self, ts_code: str, rating: int) -> bool:
        """
        检查是否符合金色评级
        
        条件：
        1. 普通评级 >= 4星
        2. 成立日期 >= 4年
        3. 基金净值累计增长 > 沪深300累计增长
        
        参数:
            ts_code: 基金代码
            rating: 普通星级评分（1-5）
        
        返回:
            True: 符合金色评级
            False: 不符合
        """
        from datetime import datetime
        
        # 条件1：评级必须 >= 4星
        if rating < 4:
            return False
        
        try:
            # 条件2：成立日期 >= 4年
            fund_info = self.get_fund_info(ts_code)
            if not fund_info:
                return False
            
            found_date = fund_info.get('found_date', '')
            if not found_date:
                return False
            
            # 解析成立日期
            try:
                found_year = int(found_date[:4])
                current_year = datetime.now().year
                fund_age = current_year - found_year
                
                if fund_age < 4:
                    return False
            except (ValueError, IndexError):
                return False
            
            # 条件3：基金净值累计增长 > 沪深300累计增长
            # 获取基金最早和最新净值
            conn = self._connect()
            
            query = """
                SELECT MIN(nav_date) as first_date, MAX(nav_date) as last_date
                FROM fund_nav
                WHERE ts_code = ? AND unit_nav IS NOT NULL
            """
            df = pd.read_sql_query(query, conn, params=(ts_code,))
            
            if df.empty or df['first_date'].iloc[0] is None:
                conn.close()
                return False
            
            first_date = df['first_date'].iloc[0]
            last_date = df['last_date'].iloc[0]
            
            # 获取首尾净值
            query_nav = """
                SELECT nav_date, unit_nav
                FROM fund_nav
                WHERE ts_code = ? AND nav_date IN (?, ?)
                ORDER BY nav_date
            """
            df_nav = pd.read_sql_query(query_nav, conn, params=(ts_code, first_date, last_date))
            conn.close()
            
            if len(df_nav) < 2:
                return False
            
            first_nav = df_nav['unit_nav'].iloc[0]
            last_nav = df_nav['unit_nav'].iloc[-1]
            
            # 基金累计涨幅
            fund_growth = (last_nav - first_nav) / first_nav * 100
            
            # 获取同期沪深300涨幅
            hs300_growth = self._get_hs300_growth(first_date, last_date)
            
            if hs300_growth is None:
                # 如果无法获取沪深300数据，保守不给金色评级
                return False
            
            # 基金涨幅必须 > 沪深300涨幅
            return fund_growth > hs300_growth
            
        except Exception as e:
            print(f"检查金色评级失败 {ts_code}: {e}")
            return False
    
    def _get_hs300_growth(self, start_date: str, end_date: str) -> Optional[float]:
        """
        获取沪深300指定期间的累计涨幅
        
        参数:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
        
        返回:
            涨幅百分比，如果获取失败返回 None
        """
        try:
            from lj_read import StockDataReaderV2
            from config import DATA_DIR
            
            db_path = DATA_DIR / 'astock.db.gz'
            if not db_path.exists():
                return None
            
            reader = StockDataReaderV2(str(db_path))
            df = reader.get_stock_data('000300', market='CN')
            
            if df.empty:
                return None
            
            # 筛选日期范围
            df_filtered = df[
                (df['date'] >= start_date) & 
                (df['date'] <= end_date)
            ].sort_values('date')
            
            if len(df_filtered) < 2:
                return None
            
            first_close = df_filtered['close'].iloc[0]
            last_close = df_filtered['close'].iloc[-1]
            
            growth = (last_close - first_close) / first_close * 100
            return growth
            
        except Exception as e:
            print(f"获取沪深300涨幅失败: {e}")
            return None
    
    # ============================================================
    # 7. 同类对比
    # ============================================================
    
    def compare_with_peers(self, ts_code: str, top_n: int = 10) -> pd.DataFrame:
        """与同类基金对比（按类型）"""
        conn = self._connect()
        
        # 获取基金类型
        fund_info = self.get_fund_info(ts_code)
        if not fund_info:
            return pd.DataFrame()
        
        fund_type = fund_info.get('fund_type')
        
        # 获取同类基金
        query = """
        SELECT ts_code, name
        FROM fund_basic
        WHERE fund_type = ? AND status = 'L'
        LIMIT 50
        """
        
        peers = pd.read_sql_query(query, conn, params=(fund_type,))
        conn.close()
        
        # 计算每个基金的一年收益率
        results = []
        for _, row in peers.iterrows():
            returns = self.calculate_returns(row['ts_code'], [365])
            if returns.get("近一年"):
                results.append({
                    "代码": row['ts_code'],
                    "名称": row['name'],
                    "近一年收益": returns["近一年"]
                })
        
        # 按收益率排序
        df_results = pd.DataFrame(results)
        if not df_results.empty:
            df_results = df_results.sort_values("近一年收益", ascending=False).head(top_n)
        
        return df_results
    
    # ============================================================
    # 8. 资金流向分析
    # ============================================================
    
    def get_fund_flow(self, ts_code: str) -> Dict[str, Any]:
        """
        获取基金份额变化（资金流向）
        
        参数:
            ts_code: 基金代码
        
        返回:
            包含资金流向信息的字典
        """
        conn = self._connect()
        
        try:
            # 获取最近30天的份额数据
            query = """
            SELECT trade_date, fd_share
            FROM fund_share
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT 31
            """
            df = pd.read_sql_query(query, conn, params=(ts_code,))
            conn.close()
            
            if df.empty or len(df) < 2:
                return {
                    "current_share": None,
                    "flow_7d": None,
                    "flow_30d": None,
                    "latest_date": None
                }
            
            # 当前份额
            current_share = df.iloc[0]['fd_share']
            latest_date = df.iloc[0]['trade_date']
            
            # 计算7日变化
            flow_7d = None
            if len(df) >= 7:
                share_7d_ago = df.iloc[6]['fd_share']
                flow_7d = current_share - share_7d_ago
            
            # 计算30日变化
            flow_30d = None
            if len(df) >= 30:
                share_30d_ago = df.iloc[29]['fd_share']
                flow_30d = current_share - share_30d_ago
            
            return {
                "current_share": round(current_share, 2) if current_share else None,
                "flow_7d": round(flow_7d, 2) if flow_7d is not None else None,
                "flow_30d": round(flow_30d, 2) if flow_30d is not None else None,
                "latest_date": latest_date
            }
            
        except Exception as e:
            conn.close()
            print(f"获取资金流向失败: {e}")
            return {
                "current_share": None,
                "flow_7d": None,
                "flow_30d": None,
                "latest_date": None
            }
    
    # ============================================================
    # 9. 生成完整报告
    # ============================================================
    
    def generate_report(self, ts_code: str) -> Dict[str, Any]:
        """生成基金完整分析报告"""
        
        report = {
            "基金代码": ts_code,
            "生成时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 基本信息
        info = self.get_fund_info(ts_code)
        if info:
            report["基本信息"] = {
                "基金名称": info.get('name'),
                "基金类型": info.get('fund_type'),
                "管理公司": info.get('management'),
                "成立日期": info.get('found_date'),
                "基金状态": info.get('status')
            }
        
        # 收益分析
        report["收益分析"] = self.calculate_returns(ts_code)
        
        # 风险分析
        report["风险分析"] = self.calculate_risk_metrics(ts_code)
        
        # 规模信息
        scale = self.get_latest_scale(ts_code)
        report["最新规模"] = f"{scale}亿份" if scale else "暂无数据"
        
        # 基金经理
        managers = self.get_fund_managers(ts_code)
        if not managers.empty:
            current_manager = managers.iloc[0]
            exp = self.get_manager_experience(current_manager['name'])
            report["基金经理"] = {
                "姓名": current_manager['name'],
                "任职时间": current_manager['begin_date'],
                "管理经验": exp
            }
        
        # 持仓分析
        concentration = self.analyze_portfolio_concentration(ts_code)
        report["持仓集中度"] = concentration
        
        # 综合评分
        report["综合评分"] = self.calculate_fund_score(ts_code)
        
        return report
    
    # ============================================================
    # 9. 年度排行榜
    # ============================================================
    
    def get_top_performers_by_year(self, year: str = "2025", top_n: int = 20) -> List[Dict[str, Any]]:
        """
        获取指定年度收益最高的前N名基金
        
        参数:
            year: 年份，默认2025
            top_n: 返回数量，默认20
        
        返回:
            基金列表，包含代码、名称、年度收益、评分等
        """
        conn = self._connect()
        
        # 获取所有在售基金
        query = """
        SELECT ts_code, name, fund_type, management
        FROM fund_basic
        WHERE status = 'L'
        """
        
        funds_df = pd.read_sql_query(query, conn)
        conn.close()
        
        if funds_df.empty:
            return []
        
        # 计算每只基金的年度收益
        results = []
        
        for _, fund in funds_df.iterrows():
            try:
                ts_code = fund['ts_code']
                
                # 获取年初和最新净值
                conn = self._connect()
                
                # 年初净值（当年1月1日或之后的第一个净值）
                start_date = f"{year}-01-01"
                query_start = """
                SELECT unit_nav, nav_date
                FROM fund_nav
                WHERE ts_code = ? AND nav_date >= ? AND unit_nav IS NOT NULL
                ORDER BY nav_date ASC
                LIMIT 1
                """
                
                start_nav_df = pd.read_sql_query(query_start, conn, params=(ts_code, start_date))
                
                # 最新净值
                query_latest = """
                SELECT unit_nav, nav_date
                FROM fund_nav
                WHERE ts_code = ? AND unit_nav IS NOT NULL
                ORDER BY nav_date DESC
                LIMIT 1
                """
                
                latest_nav_df = pd.read_sql_query(query_latest, conn, params=(ts_code,))
                conn.close()
                
                if start_nav_df.empty or latest_nav_df.empty:
                    continue
                
                start_nav = start_nav_df['unit_nav'].iloc[0]
                latest_nav = latest_nav_df['unit_nav'].iloc[0]
                latest_date = latest_nav_df['nav_date'].iloc[0]
                
                # 计算年度收益率
                year_return = (latest_nav - start_nav) / start_nav * 100
                
                # 获取评分
                score = self.calculate_fund_score(ts_code)
                
                results.append({
                    "排名": 0,  # 稍后填充
                    "代码": ts_code,
                    "名称": fund['name'],
                    "类型": fund['fund_type'],
                    "公司": fund['management'],
                    f"{year}年收益率": round(year_return, 2),
                    "最新净值": round(latest_nav, 4),
                    "更新日期": latest_date,
                    "综合评分": score.get('总分', 0),
                    "评级": score.get('评级', '未评级')
                })
                
            except Exception as e:
                # 跳过有问题的基金
                continue
        
        # 按年度收益率排序
        results_sorted = sorted(results, key=lambda x: x[f"{year}年收益率"], reverse=True)
        
        # 添加排名
        for idx, item in enumerate(results_sorted[:top_n], 1):
            item["排名"] = idx
        
        return results_sorted[:top_n]
    
    # ============================================================
    # 10. 筛选功能
    # ============================================================
    
    def get_filter_options(self) -> Dict[str, List[str]]:
        """
        获取筛选选项
        
        返回:
            包含公司、类型等选项的字典
        """
        conn = self._connect()
        
        # 获取基金公司列表
        query_companies = """
        SELECT DISTINCT management
        FROM fund_basic
        WHERE management IS NOT NULL AND management != ''
        ORDER BY management
        """
        companies_df = pd.read_sql_query(query_companies, conn)
        companies = companies_df['management'].tolist()
        
        # 获取基金类型
        query_types = """
        SELECT DISTINCT fund_type
        FROM fund_basic
        WHERE fund_type IS NOT NULL AND fund_type != ''
        ORDER BY fund_type
        """
        types_df = pd.read_sql_query(query_types, conn)
        fund_types = types_df['fund_type'].tolist()
        
        # 获取投资类型
        query_invest = """
        SELECT DISTINCT invest_type
        FROM fund_basic
        WHERE invest_type IS NOT NULL AND invest_type != ''
        ORDER BY invest_type
        """
        invest_df = pd.read_sql_query(query_invest, conn)
        invest_types = invest_df['invest_type'].tolist()
        
        conn.close()
        
        return {
            "companies": companies,
            "fund_types": fund_types,
            "invest_types": invest_types
        }
    
    def filter_funds(self, filters: Dict[str, str]) -> pd.DataFrame:
        """
        根据条件筛选基金
        
        参数:
            filters: 筛选条件字典
                - search: 代码或名称关键词
                - company: 基金公司
                - fund_type: 基金类型
                - invest_type: 投资类型
                - risk_level: 风险等级
                - status: 基金状态
        
        返回:
            符合条件的基金DataFrame（已按评分排序）
        """
        conn = self._connect()
        
        # 构建查询条件
        conditions = []
        params = []
        
        # 状态筛选
        if filters.get('status'):
            conditions.append("fb.status = ?")
            params.append(filters['status'])
        
        # 公司筛选
        if filters.get('company'):
            conditions.append("fb.management = ?")
            params.append(filters['company'])
        
        # 基金类型
        if filters.get('fund_type'):
            conditions.append("fb.fund_type = ?")
            params.append(filters['fund_type'])
        
        # 投资类型
        if filters.get('invest_type'):
            conditions.append("fb.invest_type = ?")
            params.append(filters['invest_type'])
        
        # 搜索关键词（代码或名称）
        if filters.get('search'):
            conditions.append("(fb.ts_code LIKE ? OR fb.name LIKE ?)")
            search_pattern = f"%{filters['search']}%"
            params.extend([search_pattern, search_pattern])
        
        # 构建 SQL - 关联缓存表获取最新评分
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT fb.ts_code, fb.name, fb.fund_type, fb.management, fb.invest_type, 
               fb.found_date, fb.list_date, fb.status,
               MAX(frc.computed_date) as cache_date
        FROM fund_basic fb
        LEFT JOIN (
            SELECT ts_code, MAX(computed_date) as computed_date
            FROM fund_returns_cache
            GROUP BY ts_code
        ) frc ON fb.ts_code = frc.ts_code
        WHERE {where_clause}
        GROUP BY fb.ts_code
        ORDER BY cache_date DESC NULLS LAST, fb.list_date DESC
        """
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # 风险等级筛选（基于基金类型进行简单分类）
        if filters.get('risk_level'):
            risk_level = filters['risk_level']
            if risk_level == 'low':
                # 低风险：货币型、债券型
                df = df[df['fund_type'].str.contains('货币|债券', na=False)]
            elif risk_level == 'medium':
                # 中风险：混合型
                df = df[df['fund_type'].str.contains('混合', na=False)]
            elif risk_level == 'high':
                # 高风险：股票型、指数型
                df = df[df['fund_type'].str.contains('股票|指数|ETF', na=False)]
        
        return df
    
    def calculate_year_return(self, ts_code: str, year: str = "2025") -> Optional[float]:
        """
        计算指定年度收益率
        
        参数:
            ts_code: 基金代码
            year: 年份
        
        返回:
            收益率（百分比）或 None
        """
        conn = self._connect()
        
        # 年初净值（该年1月1日或之后的第一个交易日）
        start_date = f"{year}-01-01"
        query_start = """
        SELECT unit_nav, nav_date
        FROM fund_nav
        WHERE ts_code = ? AND nav_date >= ? AND unit_nav IS NOT NULL
        ORDER BY nav_date ASC
        LIMIT 1
        """
        
        start_nav_df = pd.read_sql_query(query_start, conn, params=(ts_code, start_date))
        
        # 年末净值（该年12月31日或之前的最后一个交易日）
        # 如果是当前年份，则取最新净值
        from datetime import datetime
        current_year = datetime.now().year
        
        if int(year) >= current_year:
            # 当前年份或未来年份，取最新净值
            query_end = """
            SELECT unit_nav, nav_date
            FROM fund_nav
            WHERE ts_code = ? AND unit_nav IS NOT NULL
            ORDER BY nav_date DESC
            LIMIT 1
            """
            end_nav_df = pd.read_sql_query(query_end, conn, params=(ts_code,))
        else:
            # 历史年份，取该年度最后一个交易日净值
            end_date = f"{year}-12-31"
            query_end = """
            SELECT unit_nav, nav_date
            FROM fund_nav
            WHERE ts_code = ? AND nav_date <= ? AND unit_nav IS NOT NULL
            ORDER BY nav_date DESC
            LIMIT 1
            """
            end_nav_df = pd.read_sql_query(query_end, conn, params=(ts_code, end_date))
        
        conn.close()
        
        if start_nav_df.empty or end_nav_df.empty:
            return None
        
        start_nav = start_nav_df['unit_nav'].iloc[0]
        end_nav = end_nav_df['unit_nav'].iloc[0]
        
        # 计算收益率
        year_return = (end_nav - start_nav) / start_nav * 100
        
        return round(year_return, 2)
    
    def batch_calculate_year_returns(self, ts_codes: List[str], years: List[str] = ["2025", "2024", "2023"]) -> Dict[str, Dict[str, Optional[float]]]:
        """
        批量计算多个基金的多个年度收益率（大幅优化性能）
        
        参数:
            ts_codes: 基金代码列表
            years: 年份列表
        
        返回:
            {ts_code: {year: return_rate}}
        """
        from datetime import datetime
        current_year = datetime.now().year
        
        if not ts_codes:
            return {}
        
        conn = self._connect()
        result = {ts_code: {year: None for year in years} for ts_code in ts_codes}
        
        # 批量读取所有需要的净值数据
        ts_codes_placeholder = ','.join(['?' for _ in ts_codes])
        
        # 获取所有年初和年末的关键日期
        date_ranges = []
        for year in years:
            date_ranges.append(f"{year}-01-01")
            if int(year) < current_year:
                date_ranges.append(f"{year}-12-31")
        
        # 一次性读取所有相关净值数据
        query = f"""
        SELECT ts_code, nav_date, unit_nav
        FROM fund_nav
        WHERE ts_code IN ({ts_codes_placeholder})
          AND unit_nav IS NOT NULL
          AND nav_date >= '2020-01-01'
        ORDER BY ts_code, nav_date
        """
        
        df = pd.read_sql_query(query, conn, params=tuple(ts_codes))
        conn.close()
        
        if df.empty:
            return result
        
        df['nav_date'] = pd.to_datetime(df['nav_date'])
        
        # 按基金代码分组计算
        for ts_code in ts_codes:
            fund_data = df[df['ts_code'] == ts_code]
            
            if fund_data.empty:
                continue
            
            for year in years:
                try:
                    # 年初净值
                    start_date = pd.Timestamp(f"{year}-01-01")
                    start_data = fund_data[fund_data['nav_date'] >= start_date]
                    
                    if start_data.empty:
                        continue
                    
                    start_nav = start_data.iloc[0]['unit_nav']
                    
                    # 年末净值
                    if int(year) >= current_year:
                        # 当前年份，取最新净值
                        end_nav = fund_data.iloc[-1]['unit_nav']
                    else:
                        # 历史年份，取年底净值
                        end_date = pd.Timestamp(f"{year}-12-31")
                        end_data = fund_data[fund_data['nav_date'] <= end_date]
                        
                        if end_data.empty:
                            continue
                        
                        end_nav = end_data.iloc[-1]['unit_nav']
                    
                    # 计算收益率
                    year_return = (end_nav - start_nav) / start_nav * 100
                    result[ts_code][year] = round(year_return, 2)
                    
                except Exception as e:
                    print(f"计算 {ts_code} {year}年收益失败: {e}")
                    continue
        
        return result
    
    def calculate_period_return(self, ts_code: str, days: int) -> Optional[float]:
        """
        计算指定期间收益率（快速版本，用于批量计算）
        
        参数:
            ts_code: 基金代码
            days: 回溯天数
        
        返回:
            收益率（百分比）或 None
        """
        conn = self._connect()
        
        # 获取最新净值
        query_latest = """
        SELECT unit_nav, nav_date
        FROM fund_nav
        WHERE ts_code = ? AND unit_nav IS NOT NULL
        ORDER BY nav_date DESC
        LIMIT 1
        """
        
        latest_nav_df = pd.read_sql_query(query_latest, conn, params=(ts_code,))
        
        if latest_nav_df.empty:
            conn.close()
            return None
        
        latest_date = pd.to_datetime(latest_nav_df['nav_date'].iloc[0])
        latest_nav = latest_nav_df['unit_nav'].iloc[0]
        
        # 计算目标日期
        target_date = latest_date - timedelta(days=days)
        
        # 获取目标日期附近的净值
        query_past = """
        SELECT unit_nav, nav_date
        FROM fund_nav
        WHERE ts_code = ? AND nav_date <= ? AND unit_nav IS NOT NULL
        ORDER BY nav_date DESC
        LIMIT 1
        """
        
        past_nav_df = pd.read_sql_query(query_past, conn, params=(ts_code, target_date.strftime('%Y-%m-%d')))
        conn.close()
        
        if past_nav_df.empty:
            return None
        
        past_nav = past_nav_df['unit_nav'].iloc[0]
        
        # 计算收益率
        period_return = (latest_nav - past_nav) / past_nav * 100
        
        return round(period_return, 2)


