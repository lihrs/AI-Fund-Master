#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
基金数据读取工具
Fund Data Reader - 直接读取.gz压缩格式的数据库

功能：
1. 自动识别和解压.gz格式数据库
2. 提供多种便捷的查询方法
3. 支持导出为DataFrame、字典、CSV等格式
4. 可作为模块被其他软件调用

使用示例：
    # 命令行使用
    python read.py --list-tables
    python read.py --table fund_basic --limit 10
    python read.py --query "SELECT * FROM fund_basic WHERE market='E'"
    
    # 作为模块使用
    from read import FundDataReader
    reader = FundDataReader()
    df = reader.get_fund_basic(market='E')
"""

import gzip
import sqlite3
import tempfile
import shutil
import argparse
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
import pandas as pd
from contextlib import contextmanager


class FundDataReader:
    """基金数据读取器 - 支持直接读取.gz压缩格式"""
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        """
        初始化数据读取器
        
        Args:
            db_path: 数据库路径，可以是.db或.db.gz格式
                    如果为None，自动查找data/fund_data.db.gz或data/fund_data.db
        """
        if db_path is None:
            # 自动查找数据库
            gz_path = Path("data/aifm.db.gz")
            db_path_plain = Path("data/aifm.db")
            
            if gz_path.exists():
                db_path = gz_path
            elif db_path_plain.exists():
                db_path = db_path_plain
            else:
                raise FileNotFoundError(
                    "未找到数据库文件！\n"
                    "请确保以下文件之一存在：\n"
                    "  - data/aifm.db.gz\n"
                    "  - data/aifm.db"
                )
        
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")
        
        self.is_compressed = self.db_path.suffix == '.gz'
        self._temp_db = None
    
    @contextmanager
    def _get_connection(self):
        """
        获取数据库连接（上下文管理器）
        
        如果是.gz格式，自动解压到临时文件
        """
        if self.is_compressed:
            # 创建临时文件
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            temp_path = Path(temp_file.name)
            temp_file.close()
            
            try:
                # 解压数据库到临时文件
                with gzip.open(self.db_path, 'rb') as f_in:
                    with open(temp_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # 连接临时数据库
                conn = sqlite3.connect(str(temp_path))
                conn.row_factory = sqlite3.Row
                
                yield conn
                
            finally:
                # 关闭连接并删除临时文件
                if conn:
                    conn.close()
                if temp_path.exists():
                    temp_path.unlink()
        else:
            # 直接连接数据库
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        执行SQL查询
        
        Args:
            query: SQL查询语句
            params: 查询参数（可选）
            
        Returns:
            查询结果DataFrame
        """
        with self._get_connection() as conn:
            if params:
                return pd.read_sql_query(query, conn, params=params)
            else:
                return pd.read_sql_query(query, conn)
    
    def get_tables(self) -> List[str]:
        """获取所有表名"""
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        df = self.execute_query(query)
        return df['name'].tolist()
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        获取表的详细信息
        
        Args:
            table_name: 表名
            
        Returns:
            包含表结构和统计信息的字典
        """
        # 获取表结构
        schema_query = f"PRAGMA table_info({table_name})"
        schema_df = self.execute_query(schema_query)
        
        # 获取记录数
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        count = self.execute_query(count_query)['count'][0]
        
        return {
            'table_name': table_name,
            'columns': schema_df.to_dict('records'),
            'record_count': count
        }
    
    def read_table(self, table_name: str, limit: Optional[int] = None, 
                   offset: int = 0, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        读取整个表或部分数据
        
        Args:
            table_name: 表名
            limit: 限制返回的行数
            offset: 跳过的行数
            columns: 要读取的列名列表，None表示所有列
            
        Returns:
            数据DataFrame
        """
        col_str = ', '.join(columns) if columns else '*'
        query = f"SELECT {col_str} FROM {table_name}"
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        return self.execute_query(query)
    
    # ==================== 基金列表相关查询 ====================
    
    def get_fund_basic(self, ts_code: Optional[str] = None, 
                      market: Optional[str] = None,
                      status: Optional[str] = None,
                      fund_type: Optional[str] = None,
                      limit: Optional[int] = None) -> pd.DataFrame:
        """
        查询基金列表
        
        Args:
            ts_code: 基金代码
            market: 市场类型 (E场内, O场外)
            status: 状态 (L上市, D摘牌, I发行)
            fund_type: 基金类型
            limit: 限制返回数量
            
        Returns:
            基金列表DataFrame
        """
        query = "SELECT * FROM fund_basic WHERE 1=1"
        params = []
        
        if ts_code:
            query += " AND ts_code = ?"
            params.append(ts_code)
        if market:
            query += " AND market = ?"
            params.append(market)
        if status:
            query += " AND status = ?"
            params.append(status)
        if fund_type:
            query += " AND fund_type = ?"
            params.append(fund_type)
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute_query(query, tuple(params) if params else None)
    
    def search_fund_by_name(self, name: str) -> pd.DataFrame:
        """
        按名称搜索基金（模糊匹配）
        
        Args:
            name: 基金名称关键词
            
        Returns:
            匹配的基金列表
        """
        query = "SELECT * FROM fund_basic WHERE name LIKE ?"
        return self.execute_query(query, (f'%{name}%',))
    
    # ==================== 基金净值相关查询 ====================
    
    def get_fund_nav(self, ts_code: str, 
                    start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    limit: Optional[int] = None) -> pd.DataFrame:
        """
        查询基金净值
        
        Args:
            ts_code: 基金代码
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            limit: 限制返回数量
            
        Returns:
            净值数据DataFrame
        """
        query = "SELECT * FROM fund_nav WHERE ts_code = ?"
        params = [ts_code]
        
        if start_date:
            query += " AND nav_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND nav_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY nav_date DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute_query(query, tuple(params))
    
    def get_latest_nav(self, ts_code: Optional[str] = None, 
                      top_n: int = 1) -> pd.DataFrame:
        """
        获取最新净值
        
        Args:
            ts_code: 基金代码，None表示所有基金
            top_n: 每个基金返回最近N条记录
            
        Returns:
            最新净值DataFrame
        """
        if ts_code:
            query = """
                SELECT * FROM fund_nav 
                WHERE ts_code = ?
                ORDER BY nav_date DESC 
                LIMIT ?
            """
            return self.execute_query(query, (ts_code, top_n))
        else:
            # 获取所有基金的最新净值
            query = """
                SELECT f.* FROM fund_nav f
                INNER JOIN (
                    SELECT ts_code, MAX(nav_date) as max_date 
                    FROM fund_nav 
                    GROUP BY ts_code
                ) m ON f.ts_code = m.ts_code AND f.nav_date = m.max_date
            """
            return self.execute_query(query)
    
    # ==================== 基金经理相关查询 ====================
    
    def get_fund_manager(self, ts_code: Optional[str] = None,
                        name: Optional[str] = None,
                        current_only: bool = False) -> pd.DataFrame:
        """
        查询基金经理
        
        Args:
            ts_code: 基金代码
            name: 经理姓名（模糊匹配）
            current_only: 是否只查询在任经理
            
        Returns:
            基金经理DataFrame
        """
        query = "SELECT * FROM fund_manager WHERE 1=1"
        params = []
        
        if ts_code:
            query += " AND ts_code = ?"
            params.append(ts_code)
        if name:
            query += " AND name LIKE ?"
            params.append(f'%{name}%')
        if current_only:
            query += " AND (end_date IS NULL OR end_date = '')"
        
        query += " ORDER BY begin_date DESC"
        
        return self.execute_query(query, tuple(params) if params else None)
    
    # ==================== 基金规模相关查询 ====================
    
    def get_fund_share(self, ts_code: str,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> pd.DataFrame:
        """
        查询基金规模
        
        Args:
            ts_code: 基金代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            基金规模DataFrame
        """
        query = "SELECT * FROM fund_share WHERE ts_code = ?"
        params = [ts_code]
        
        if start_date:
            query += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND trade_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY trade_date DESC"
        
        return self.execute_query(query, tuple(params))
    
    # ==================== 基金分红相关查询 ====================
    
    def get_fund_div(self, ts_code: Optional[str] = None,
                    year: Optional[str] = None) -> pd.DataFrame:
        """
        查询基金分红
        
        Args:
            ts_code: 基金代码
            year: 年份 (YYYY)
            
        Returns:
            基金分红DataFrame
        """
        query = "SELECT * FROM fund_div WHERE 1=1"
        params = []
        
        if ts_code:
            query += " AND ts_code = ?"
            params.append(ts_code)
        if year:
            query += " AND ann_date LIKE ?"
            params.append(f'{year}%')
        
        query += " ORDER BY ann_date DESC"
        
        return self.execute_query(query, tuple(params) if params else None)
    
    # ==================== 基金持仓相关查询 ====================
    
    def get_fund_portfolio(self, ts_code: str,
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """
        查询基金持仓
        
        Args:
            ts_code: 基金代码
            end_date: 报告期结束日期
            
        Returns:
            基金持仓DataFrame
        """
        query = "SELECT * FROM fund_portfolio WHERE ts_code = ?"
        params = [ts_code]
        
        if end_date:
            query += " AND end_date = ?"
            params.append(end_date)
        
        query += " ORDER BY end_date DESC, mkv DESC"
        
        return self.execute_query(query, tuple(params))
    
    def get_latest_portfolio(self, ts_code: str) -> pd.DataFrame:
        """
        获取基金最新持仓
        
        Args:
            ts_code: 基金代码
            
        Returns:
            最新持仓DataFrame
        """
        query = """
            SELECT * FROM fund_portfolio 
            WHERE ts_code = ? AND end_date = (
                SELECT MAX(end_date) FROM fund_portfolio WHERE ts_code = ?
            )
            ORDER BY mkv DESC
        """
        return self.execute_query(query, (ts_code, ts_code))
    
    # ==================== 全量数据查询 ====================
    
    def get_all_fund_basic(self) -> pd.DataFrame:
        """
        获取所有基金基本信息
        
        Returns:
            所有基金基本信息DataFrame
        """
        query = "SELECT * FROM fund_basic ORDER BY ts_code"
        return self.execute_query(query)
    
    def get_all_fund_manager(self) -> pd.DataFrame:
        """
        获取所有基金经理信息
        
        Returns:
            所有基金经理信息DataFrame
        """
        query = "SELECT * FROM fund_manager ORDER BY ts_code, begin_date DESC"
        return self.execute_query(query)
    
    def get_all_fund_share(self) -> pd.DataFrame:
        """
        获取所有基金规模信息
        
        Returns:
            所有基金规模信息DataFrame
        """
        query = "SELECT * FROM fund_share ORDER BY ts_code, trade_date DESC"
        return self.execute_query(query)
    
    def get_all_fund_nav(self) -> pd.DataFrame:
        """
        获取所有基金净值信息
        
        Returns:
            所有基金净值信息DataFrame
        """
        query = "SELECT * FROM fund_nav ORDER BY ts_code, nav_date DESC"
        return self.execute_query(query)
    
    def get_all_fund_div(self) -> pd.DataFrame:
        """
        获取所有基金分红信息
        
        Returns:
            所有基金分红信息DataFrame
        """
        query = "SELECT * FROM fund_div ORDER BY ts_code, ann_date DESC"
        return self.execute_query(query)
    
    def get_all_fund_portfolio(self) -> pd.DataFrame:
        """
        获取所有基金持仓信息
        
        Returns:
            所有基金持仓信息DataFrame
        """
        query = "SELECT * FROM fund_portfolio ORDER BY ts_code, end_date DESC, mkv DESC"
        return self.execute_query(query)
    
    def get_all_data(self) -> Dict[str, pd.DataFrame]:
        """
        获取所有表的全部数据
        
        Returns:
            包含所有表数据的字典，key为表名，value为对应的DataFrame
        """
        all_data = {}
        
        # 获取所有表名
        tables = self.get_tables()
        
        # 排除元数据表
        data_tables = [t for t in tables if t != 'collection_metadata']
        
        for table in data_tables:
            try:
                all_data[table] = self.read_table(table)
            except Exception as e:
                print(f"警告: 读取表 {table} 失败: {e}")
                all_data[table] = pd.DataFrame()
        
        return all_data
    
    # ==================== 组合查询 ====================
    
    def get_fund_full_info(self, ts_code: str) -> Dict[str, Any]:
        """
        获取基金的完整信息（基本信息+最新净值+经理+最新持仓）
        
        Args:
            ts_code: 基金代码
            
        Returns:
            包含所有信息的字典
        """
        info = {}
        
        # 基本信息
        basic = self.get_fund_basic(ts_code=ts_code)
        info['basic'] = basic.to_dict('records')[0] if not basic.empty else None
        
        # 最新净值
        nav = self.get_latest_nav(ts_code=ts_code)
        info['latest_nav'] = nav.to_dict('records')[0] if not nav.empty else None
        
        # 当前基金经理
        manager = self.get_fund_manager(ts_code=ts_code, current_only=True)
        info['current_managers'] = manager.to_dict('records') if not manager.empty else []
        
        # 最新持仓
        portfolio = self.get_latest_portfolio(ts_code=ts_code)
        info['latest_portfolio'] = portfolio.to_dict('records') if not portfolio.empty else []
        
        return info
    
    def compare_funds(self, ts_codes: List[str]) -> pd.DataFrame:
        """
        对比多个基金的基本信息和最新净值
        
        Args:
            ts_codes: 基金代码列表
            
        Returns:
            对比结果DataFrame
        """
        codes_str = ','.join([f"'{code}'" for code in ts_codes])
        query = f"""
            SELECT 
                b.ts_code,
                b.name,
                b.fund_type,
                b.management,
                n.nav_date,
                n.unit_nav,
                n.accum_nav
            FROM fund_basic b
            LEFT JOIN (
                SELECT * FROM fund_nav
                WHERE (ts_code, nav_date) IN (
                    SELECT ts_code, MAX(nav_date) 
                    FROM fund_nav 
                    GROUP BY ts_code
                )
            ) n ON b.ts_code = n.ts_code
            WHERE b.ts_code IN ({codes_str})
        """
        return self.execute_query(query)
    
    # ==================== 统计分析 ====================
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        stats = {}
        
        tables = self.get_tables()
        stats['tables'] = {}
        
        for table in tables:
            info = self.get_table_info(table)
            stats['tables'][table] = {
                'record_count': info['record_count'],
                'columns': len(info['columns'])
            }
        
        return stats
    
    # ==================== 数据导出 ====================
    
    def export_to_csv(self, table_name: str, output_path: str,
                     query: Optional[str] = None, **kwargs):
        """
        导出数据到CSV
        
        Args:
            table_name: 表名
            output_path: 输出文件路径
            query: 自定义查询语句（可选）
            **kwargs: 传递给pandas.to_csv的其他参数
        """
        if query:
            df = self.execute_query(query)
        else:
            df = self.read_table(table_name)
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig', **kwargs)
        print(f"已导出 {len(df)} 条记录到: {output_path}")
    
    def export_to_excel(self, output_path: str, tables: Optional[List[str]] = None):
        """
        导出数据到Excel（多个表作为不同的sheet）
        
        Args:
            output_path: 输出文件路径
            tables: 要导出的表名列表，None表示所有表
        """
        if tables is None:
            tables = self.get_tables()
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for table in tables:
                df = self.read_table(table)
                # Excel sheet名称长度限制为31个字符
                sheet_name = table[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"已导出表 {table} ({len(df)} 条记录)")
        
        print(f"Excel文件已保存到: {output_path}")


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description='基金数据读取工具 - 支持直接读取.gz压缩格式',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 列出所有表
  python read.py --list-tables
  
  # 查看表信息
  python read.py --table-info fund_basic
  
  # 读取表数据
  python read.py --table fund_basic --limit 10
  
  # 执行自定义查询
  python read.py --query "SELECT * FROM fund_basic WHERE market='E'" --limit 20
  
  # 查询特定基金
  python read.py --fund-code 159915.SZ
  
  # 搜索基金
  python read.py --search 沪深300
  
  # 导出数据
  python read.py --table fund_basic --export fund_basic.csv
  
  # 获取所有表的全量数据（仅显示统计信息）
  python read.py --all-data
  
  # 导出所有数据到Excel
  python read.py --export-all all_fund_data.xlsx
        """
    )
    
    parser.add_argument('--db', type=str, help='数据库文件路径（.db或.db.gz）')
    parser.add_argument('--list-tables', action='store_true', help='列出所有表')
    parser.add_argument('--table-info', type=str, help='查看表信息')
    parser.add_argument('--table', type=str, help='读取表数据')
    parser.add_argument('--query', type=str, help='执行自定义SQL查询')
    parser.add_argument('--limit', type=int, help='限制返回行数')
    parser.add_argument('--fund-code', type=str, help='查询特定基金代码')
    parser.add_argument('--search', type=str, help='搜索基金名称')
    parser.add_argument('--export', type=str, help='导出到CSV文件')
    parser.add_argument('-stats', action='store_true', help='显示统计信息')
    parser.add_argument('--all-data', action='store_true', help='获取所有表的全量数据')
    parser.add_argument('-export-all', type=str, help='导出所有数据到Excel文件')
    
    args = parser.parse_args()
    
    try:
        # 创建读取器
        reader = FundDataReader(args.db)
        print(f"数据库路径: {reader.db_path}")
        print(f"压缩格式: {'是' if reader.is_compressed else '否'}")
        print()
        
        # 执行操作
        if args.list_tables:
            tables = reader.get_tables()
            print("数据库表列表:")
            for i, table in enumerate(tables, 1):
                info = reader.get_table_info(table)
                print(f"  {i}. {table} ({info['record_count']:,} 条记录)")
        
        elif args.table_info:
            info = reader.get_table_info(args.table_info)
            print(f"表名: {info['table_name']}")
            print(f"记录数: {info['record_count']:,}")
            print("\n字段信息:")
            for col in info['columns']:
                print(f"  {col['name']:20} {col['type']:10} {'NOT NULL' if col['notnull'] else ''}")
        
        elif args.table:
            df = reader.read_table(args.table, limit=args.limit)
            print(f"表 {args.table} 的数据:")
            print(df)
            
            if args.export:
                reader.export_to_csv(args.table, args.export)
        
        elif args.query:
            df = reader.execute_query(args.query)
            if args.limit:
                df = df.head(args.limit)
            print("查询结果:")
            print(df)
            
            if args.export:
                df.to_csv(args.export, index=False, encoding='utf-8-sig')
                print(f"已导出到: {args.export}")
        
        elif args.fund_code:
            print(f"基金代码: {args.fund_code}")
            info = reader.get_fund_full_info(args.fund_code)
            
            if info['basic']:
                print("\n基本信息:")
                for key, value in info['basic'].items():
                    print(f"  {key}: {value}")
            
            if info['latest_nav']:
                print("\n最新净值:")
                for key, value in info['latest_nav'].items():
                    print(f"  {key}: {value}")
            
            if info['current_managers']:
                print("\n当前基金经理:")
                for mgr in info['current_managers']:
                    print(f"  {mgr['name']} (从 {mgr['begin_date']})")
        
        elif args.search:
            df = reader.search_fund_by_name(args.search)
            print(f"搜索 '{args.search}' 的结果:")
            print(df[['ts_code', 'name', 'fund_type', 'management']])
        
        elif args.stats:
            stats = reader.get_statistics()
            print("数据库统计信息:")
            for table, info in stats['tables'].items():
                print(f"  {table}: {info['record_count']:,} 条记录, {info['columns']} 个字段")
        
        elif args.all_data:
            print("获取所有表的全量数据...")
            all_data = reader.get_all_data()
            print("\n数据概览:")
            for table_name, df in all_data.items():
                print(f"  {table_name}: {len(df):,} 条记录")
            
            if args.export_all:
                reader.export_to_excel(args.export_all, list(all_data.keys()))
        
        elif args.export_all:
            print("导出所有数据到Excel...")
            reader.export_to_excel(args.export_all)
        
        else:
            # 无参数时显示数据库内容统计
            print("=" * 80)
            print("数据库内容概览")
            print("=" * 80)
            print(f"数据库路径: {reader.db_path}")
            print(f"压缩格式: {'是' if reader.is_compressed else '否'}\n")
            
            tables = reader.get_tables()
            if not tables:
                print("数据库为空，无数据表。")
            else:
                print(f"共 {len(tables)} 个数据表:\n")
                
                for table in tables:
                    if table == 'collection_metadata':
                        continue
                    
                    try:
                        info = reader.get_table_info(table)
                        count = info['record_count']
                        
                        # 获取日期范围
                        date_range = "N/A"
                        date_field = None
                        
                        if table == 'fund_nav':
                            date_field = 'nav_date'
                        elif table == 'fund_share':
                            date_field = 'trade_date'
                        elif table == 'fund_div':
                            date_field = 'ann_date'
                        elif table == 'fund_portfolio':
                            date_field = 'end_date'
                        elif table == 'fund_manager':
                            date_field = 'begin_date'
                        
                        if date_field and count > 0:
                            query = f"SELECT MIN({date_field}) as min_date, MAX({date_field}) as max_date FROM {table}"
                            try:
                                result = reader.execute_query(query)
                                if not result.empty:
                                    min_date = result['min_date'][0]
                                    max_date = result['max_date'][0]
                                    if min_date and max_date:
                                        date_range = f"{min_date} ~ {max_date}"
                            except:
                                pass
                        
                        print(f"  [{table}]")
                        print(f"    记录数: {count:,} 条")
                        print(f"    日期范围: {date_range}")
                        print()
                        
                    except Exception as e:
                        print(f"  [{table}]: 读取失败 ({e})\n")
                
                print("=" * 80)
                print("使用 -stats 查看详细统计信息")
                print("使用 --help 查看所有可用命令")
                print("=" * 80)
    
    except Exception as e:
        print(f"错误: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())

