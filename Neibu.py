import asyncio
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
import pandas as pd
import argparse
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Any, Tuple, Callable, Union, TypedDict
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from functools import lru_cache

import requests

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler(),
    ],
)

# 常量定义
DATE_FORMAT = "%Y%m%d"
EXCEL_DATE_FORMAT = "yyyy-mm-dd"
HEADER_FORMAT = {
    "bold": True,
    "bg_color": "#4F81BD",
    "font_color": "#FFFFFF",
    "border": 1,
    "align": "center",
    "valign": "vcenter",
}
CONTENT_FORMAT = {
    "border": 1,
    "align": "center",
    "valign": "vcenter",
    "num_format": "0.00",
}
ALTERNATE_ROW_FORMAT = {
    "bg_color": "#DCE6F1",
    "border": 1,
    "align": "center",
    "valign": "vcenter",
    "num_format": "0.00",
}

@dataclass
class PowerThreshold:
    """战力阈值配置"""
    min_power: float
    max_power: float
    group_name: str
    kill_threshold: int
    death_threshold: int
    power_growth_threshold: float = 2000000  # 每日战力增长阈值

# 战力分组配置（单位：亿）
POWER_GROUPS = [
    PowerThreshold(150000000, 200000000, "1.5亿-2亿", 400, 800, 2000000),
    PowerThreshold(200000000, 250000000, "2亿-2.5亿", 600, 1000, 2500000),
    PowerThreshold(250000000, 300000000, "2.5亿-3亿", 800, 1200, 3000000),
    PowerThreshold(300000000, 350000000, "3亿-3.5亿", 1000, 1500, 3500000),
    PowerThreshold(350000000, 400000000, "3.5亿-4亿", 1200, 1800, 4000000),
    PowerThreshold(400000000, 450000000, "4亿-4.5亿", 1500, 2000, 4500000),
    PowerThreshold(450000000, 500000000, "4.5亿-5亿", 1800, 2200, 5000000),
    PowerThreshold(500000000, float('inf'), "5亿以上", 2000, 2500, 5500000),
]

@lru_cache(maxsize=1000)
def get_power_group(max_power: float) -> str:
    """根据战力值返回战力组别（使用缓存优化）"""
    for group in POWER_GROUPS:
        if group.min_power <= max_power < group.max_power:
            return group.group_name
    return "5亿以上"

@lru_cache(maxsize=1000)
def get_thresholds(max_power: float) -> Tuple[int, int, float]:
    """获取击杀、死亡和战力增长阈值（使用缓存优化）"""
    for group in POWER_GROUPS:
        if group.min_power <= max_power < group.max_power:
            return group.kill_threshold, group.death_threshold, group.power_growth_threshold
    return 2000, 2500, 5500000

def load_json(file_path: str) -> Dict:
    """加载 JSON 文件并返回数据"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"文件未找到: {file_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"文件格式错误: {file_path} 不是有效的 JSON 文件")
        raise

def validate_date(date_value: str) -> Optional[datetime]:
    """验证日期字符串是否有效"""
    date_str = str(date_value)
    if len(date_str) != 8:
        return None
    try:
        return datetime.strptime(date_str, DATE_FORMAT)
    except ValueError:
        return None

def calculate_daily_stats(entry: Dict, prev_entry: Optional[Dict] = None) -> Dict:
    """计算每日统计信息（累计值差值归属于当前日期，负数置0）"""
    day = entry["day"]
    valid_date = validate_date(day)
    # 默认当天击杀和死亡为0
    daily_kills = 0
    daily_deaths = 0
    # 优先使用c_sumkill/c_die
    if entry.get("c_sumkill", 0) > 0 or entry.get("c_die", 0) > 0:
        daily_kills = entry.get("c_sumkill", 0)
        daily_deaths = entry.get("c_die", 0)
    elif prev_entry is not None:
        # 用累计值做差，负数置0
        daily_kills = max(0, entry.get("sumkill", 0) - prev_entry.get("sumkill", 0))
        daily_deaths = max(0, entry.get("die", 0) - prev_entry.get("die", 0))
    daily_kill_ratio = daily_kills / daily_deaths if daily_deaths > 0 else float('inf')
    max_power = entry.get("maxpower", 0)
    power_growth = entry.get("power_growth", 0)
    tech_power = entry.get("powers", {}).get("tech", 0)
    battle_score = min(100, (daily_kills / 200 + daily_deaths / 300) * 50)
    return {
        "日期": valid_date,
        "当天击杀": daily_kills,
        "当天死亡": daily_deaths,
        "当天击杀比": daily_kill_ratio,
        "当天最高战力": max_power,
        "战力增长": power_growth,
        "战斗活跃度": battle_score,
        "科技战力": tech_power
    }

def calculate_summary_stats(daily_stats: List[Dict], max_power: float) -> Dict:
    """计算总体统计信息"""
    if not daily_stats:
        return {
            "赛季总击杀数": 0,
            "赛季总被击杀数": 0,
            "赛季击杀/被击杀比": float('inf'),
            "活跃天数": 0,
            "战力组别": get_power_group(max_power),
            "平均战斗活跃度": 0,
            "科技战力": 0  # 添加科技战力
        }

    # 使用numpy进行向量化计算
    kills = np.array([stat["当天击杀"] for stat in daily_stats])
    deaths = np.array([stat["当天死亡"] for stat in daily_stats])
    power_growths = np.array([stat.get("战力增长", 0) for stat in daily_stats])
    battle_scores = np.array([stat.get("战斗活跃度", 0) for stat in daily_stats])
    tech_powers = np.array([stat.get("科技战力", 0) for stat in daily_stats])  # 获取科技战力数组
    
    total_kills = np.sum(kills)
    total_deaths = np.sum(deaths)
    total_days = len(daily_stats)
    
    kill_threshold, death_threshold, power_growth_threshold = get_thresholds(max_power)
    
    # 活跃度计算（基于多个维度）
    kill_active = kills >= (kill_threshold * 0.3)  # 击杀达到阈值30%
    death_active = deaths >= (death_threshold * 0.3)  # 死亡达到阈值30%
    power_active = power_growths >= (power_growth_threshold * 0.5)  # 战力增长达到阈值50%
    battle_active = battle_scores >= 30  # 战斗活跃度达到30分
    
    # 综合活跃天数（至少满足两个条件）
    active_conditions = np.vstack([kill_active, death_active, power_active, battle_active])
    active_days = np.sum(np.sum(active_conditions, axis=0) >= 2)
    
    # 计算平均战斗活跃度
    avg_battle_score = np.mean(battle_scores)
    
    kill_death_ratio = total_kills / total_deaths if total_deaths > 0 else float('inf')

    return {
        "赛季总击杀数": int(total_kills),
        "赛季总被击杀数": int(total_deaths),
        "赛季击杀/被击杀比": kill_death_ratio,
        "活跃天数": int(active_days),
        "战力组别": get_power_group(max_power),
        "平均战斗活跃度": float(avg_battle_score),
        "高活跃天数": int(np.sum(battle_scores >= 60)),  # 战斗活跃度>=60的天数
        "中活跃天数": int(np.sum((battle_scores >= 30) & (battle_scores < 60))),  # 战斗活跃度30-60的天数
        "低活跃天数": int(np.sum(battle_scores < 30)),  # 战斗活跃度<30的天数
        "科技战力": int(np.max(tech_powers))  # 添加科技战力（取最大值）
    }

def process_user_data(user_data: Dict, start_timestamp: int, end_timestamp: int) -> Tuple[Dict, List[Dict], str, float]:
    """处理单个用户的数据，累计值差值归属于当前日期，负数置0"""
    daily_stats = []
    latest_nick = "Unknown"
    latest_day = 0
    max_power = 0
    end_max_power = 0
    data_list = user_data["Data"]
    data_list = sorted(data_list, key=lambda x: x["day"])  # 按日期升序
    for idx in range(len(data_list)):
        entry = data_list[idx]
        prev_entry = data_list[idx-1] if idx > 0 else None
        if "nick" in entry and entry["day"] > latest_day:
            latest_nick = entry["nick"]
            latest_day = entry["day"]
            max_power = entry.get("maxpower", 0)
        # 只统计在时间范围内的
        if validate_date(entry["day"]) and start_timestamp <= entry["day"] <= end_timestamp:
            daily_stat = calculate_daily_stats(entry, prev_entry)
            daily_stats.append(daily_stat)
            if entry["day"] <= end_timestamp:
                end_max_power = max(end_max_power, entry.get("maxpower", 0))
    if daily_stats:
        summary_stats = calculate_summary_stats(daily_stats, max_power)
        return summary_stats, daily_stats, latest_nick, end_max_power
    return None, [], latest_nick, end_max_power

def calculate_guild_stats(results: Dict) -> Dict:
    """计算公会总体统计信息"""
    guild_stats = {}
    
    for user, stats in results.items():
        guild_name = stats.get("公会名称", "未知公会")
        if guild_name not in guild_stats:
            guild_stats[guild_name] = {
                "成员数": 0,
                "总击杀数": 0,
                "总被击杀数": 0,
                "平均击杀比": 0,
                "总活跃天数": 0,
                "平均活跃度": 0,
                "高活跃成员数": 0,
                "中活跃成员数": 0,
                "低活跃成员数": 0,
                "平均战力": 0,
                "最高战力": 0,
                "战力分布": defaultdict(int)
            }
        
        guild_stats[guild_name]["成员数"] += 1
        guild_stats[guild_name]["总击杀数"] += stats["赛季总击杀数"]
        guild_stats[guild_name]["总被击杀数"] += stats["赛季总被击杀数"]
        guild_stats[guild_name]["总活跃天数"] += stats["活跃天数"]
        guild_stats[guild_name]["平均活跃度"] += stats["平均战斗活跃度"]
        guild_stats[guild_name]["平均战力"] += stats["结束时的最高战力"]
        guild_stats[guild_name]["最高战力"] = max(
            guild_stats[guild_name]["最高战力"],
            stats["结束时的最高战力"]
        )
        
        # 统计活跃度分布
        if stats["平均战斗活跃度"] >= 60:
            guild_stats[guild_name]["高活跃成员数"] += 1
        elif stats["平均战斗活跃度"] >= 30:
            guild_stats[guild_name]["中活跃成员数"] += 1
        else:
            guild_stats[guild_name]["低活跃成员数"] += 1
        
        # 统计战力分布
        power_group = stats["战力组别"]
        guild_stats[guild_name]["战力分布"][power_group] += 1
    
    # 计算平均值
    for guild_name in guild_stats:
        member_count = guild_stats[guild_name]["成员数"]
        if member_count > 0:
            guild_stats[guild_name]["平均击杀比"] = (
                guild_stats[guild_name]["总击杀数"] /
                guild_stats[guild_name]["总被击杀数"]
                if guild_stats[guild_name]["总被击杀数"] > 0
                else float('inf')
            )
            guild_stats[guild_name]["平均活跃度"] /= member_count
            guild_stats[guild_name]["平均战力"] /= member_count
    
    return guild_stats

def calculate_kills(data: Dict, start_date: str, end_date: str) -> Tuple[Dict, Dict]:
    """计算指定时间范围内的击杀与被击杀数据"""
    start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").strftime(DATE_FORMAT))
    end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").strftime(DATE_FORMAT))

    results = defaultdict(
        lambda: {
            "赛季总击杀数": 0,
            "赛季总被击杀数": 0,
            "赛季击杀/被击杀比": 0,
            "活跃天数": 0,
            "每日统计": [],
            "结束时的最高战力": 0,
            "公会名称": "未知公会"
        }
    )

    power_group_stats = defaultdict(lambda: defaultdict(list))

    # 使用线程池并行处理用户数据
    with ThreadPoolExecutor() as executor:
        futures = []
        for pid, info in data.items():
            if info["Code"] == 0 and "Data" in info:
                futures.append(
                    executor.submit(process_user_data, info, start_timestamp, end_timestamp)
                )

        for future in futures:
            summary_stats, daily_stats, latest_nick, end_max_power = future.result()
            if summary_stats:
                results[latest_nick] = {
                    **summary_stats,
                    "每日统计": daily_stats,
                    "最新战力": max(stat["当天最高战力"] for stat in daily_stats),
                    "结束时的最高战力": end_max_power,
                }

                # 更新战力分组统计
                power_group = get_power_group(end_max_power)
                for stat in daily_stats:
                    power_group_stats[power_group][latest_nick].append({
                        "日期": stat["日期"],
                        "当天击杀": stat["当天击杀"],
                        "当天死亡": stat["当天死亡"],
                    })

    return results, power_group_stats

class ExcelExporter:
    """Excel 导出工具类"""

    def __init__(self, output_file: str):
        self.output_file = output_file
        self.workbook = None
        self.header_format = None
        self.content_format = None
        self.alternate_row_format = None

    def _init_formats(self):
        """初始化 Excel 样式"""
        self.header_format = self.workbook.add_format(HEADER_FORMAT)
        self.content_format = self.workbook.add_format(CONTENT_FORMAT)
        self.alternate_row_format = self.workbook.add_format(ALTERNATE_ROW_FORMAT)

    def export(self, results: Dict, power_group_stats: Dict, compare: bool = True):
        """导出数据到 Excel"""
        main_table, daily_stats_table, power_group_tables = self._prepare_data(results, power_group_stats)

        with pd.ExcelWriter(self.output_file, engine="xlsxwriter", engine_kwargs={'options': {'nan_inf_to_errors': True}}) as writer:
            self.workbook = writer.book
            self._init_formats()

            self._export_main_table(writer, main_table)
            self._export_daily_stats_table(writer, daily_stats_table)
            self._export_power_group_tables(writer, power_group_tables, results)
            self._add_charts(writer, main_table, daily_stats_table)

    def _prepare_data(self, results: Dict, power_group_stats: Dict) -> Tuple[List[Dict], List[Dict], Dict]:
        """准备数据"""
        main_table = []
        daily_stats_table = []

        for user, stats in results.items():
            main_table.append({
                "用户": user,
                "赛季总击杀数": stats["赛季总击杀数"],
                "赛季总被击杀数": stats["赛季总被击杀数"],
                "赛季击杀/被击杀比": stats["赛季击杀/被击杀比"],
                "活跃天数": stats["活跃天数"],
                "结束时的最高战力": stats["结束时的最高战力"],
                "战力组别": stats["战力组别"],
                "平均战斗活跃度": stats["平均战斗活跃度"],
                "科技战力": stats["科技战力"]  # 添加科技战力
            })
            daily_stats_table.extend([
                {
                    "用户": user,
                    "日期": stat["日期"],
                    "当天击杀": stat["当天击杀"],
                    "当天死亡": stat["当天死亡"],
                    "当天击杀比": stat["当天击杀比"],
                    "当天最高战力": stat["当天最高战力"],
                    "科技战力": stat["科技战力"]  # 添加科技战力
                }
                for stat in stats["每日统计"]
            ])

        power_group_tables = {
            group: group_data
            for group, group_data in power_group_stats.items()
        }

        return main_table, daily_stats_table, power_group_tables

    def _export_main_table(self, writer, main_table: List[Dict]):
        """导出主表数据"""
        main_df = pd.DataFrame(main_table)
        main_df.to_excel(writer, index=False, sheet_name="用户总览")
        worksheet_main = writer.sheets["用户总览"]

        for col_num, value in enumerate(main_df.columns.values):
            worksheet_main.write(0, col_num, value, self.header_format)

        for row_num in range(1, len(main_df) + 1):
            for col_num in range(len(main_df.columns)):
                cell_format = (
                    self.alternate_row_format if row_num % 2 == 0 else self.content_format
                )
                worksheet_main.write(row_num, col_num, main_df.iloc[row_num - 1, col_num], cell_format)

        worksheet_main.freeze_panes(1, 0)
        worksheet_main.autofit()

    def _export_daily_stats_table(self, writer, daily_stats_table: List[Dict]):
        """导出每日统计表数据"""
        daily_stats_df = pd.DataFrame(daily_stats_table)
        daily_stats_df.to_excel(writer, index=False, sheet_name="每日统计")
        worksheet_daily = writer.sheets["每日统计"]

        date_format = self.workbook.add_format({**CONTENT_FORMAT, "num_format": EXCEL_DATE_FORMAT})

        for col_num, value in enumerate(daily_stats_df.columns.values):
            worksheet_daily.write(0, col_num, value, self.header_format)

        current_user = None
        start_row = 1
        for row_num in range(1, len(daily_stats_df) + 1):
            user = daily_stats_df.iloc[row_num - 1, 0]
            if user != current_user:
                if current_user is not None and row_num - 1 > start_row:
                    worksheet_daily.merge_range(
                        start_row, 0, row_num - 1, 0, current_user, self.content_format
                    )
                current_user = user
                start_row = row_num
            for col_num in range(1, len(daily_stats_df.columns)):
                cell_format = (
                    date_format
                    if daily_stats_df.columns[col_num] == "日期"
                    else (self.alternate_row_format if row_num % 2 == 0 else self.content_format)
                )
                worksheet_daily.write(
                    row_num, col_num, daily_stats_df.iloc[row_num - 1, col_num], cell_format
                )

        if current_user is not None and len(daily_stats_df) > start_row:
            worksheet_daily.merge_range(
                start_row, 0, len(daily_stats_df), 0, current_user, self.content_format
            )

        worksheet_daily.freeze_panes(1, 0)
        worksheet_daily.autofit()

    def _export_power_group_tables(self, writer, power_group_tables: Dict, results: Dict):
        """导出战力分组表"""
        for power_group, group_data in power_group_tables.items():
            group_table = []
            for user, stats in group_data.items():
                for stat in stats:
                    # 修正日期格式为字符串
                    date_value = stat["日期"]
                    if hasattr(date_value, 'strftime'):
                        date_str = date_value.strftime('%Y-%m-%d')
                    else:
                        # 兼容字符串或数字类型
                        try:
                            date_str = str(date_value)
                            if len(date_str) == 8 and date_str.isdigit():
                                date_str = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
                        except Exception:
                            pass
                    group_table.append({
                        "用户": user,
                        "日期": date_str,
                        "当天击杀": stat["当天击杀"],
                        "当天死亡": stat["当天死亡"],
                        "赛季总击杀": "",
                        "赛季总死亡": "",
                        "活跃天数": "",
                    })
                
                if user in results:
                    group_table[-1]["赛季总击杀"] = results[user]["赛季总击杀数"]
                    group_table[-1]["赛季总死亡"] = results[user]["赛季总被击杀数"]
                    group_table[-1]["活跃天数"] = results[user]["活跃天数"]

            group_df = pd.DataFrame(group_table)
            group_df.to_excel(writer, index=False, sheet_name=power_group)
            worksheet_group = writer.sheets[power_group]

            for col_num, value in enumerate(group_df.columns.values):
                worksheet_group.write(0, col_num, value, self.header_format)

            current_user = None
            start_row = 1
            for row_num in range(1, len(group_df) + 1):
                user = group_df.iloc[row_num - 1, 0]
                if user != current_user:
                    if current_user is not None and row_num - 1 > start_row:
                        worksheet_group.merge_range(
                            start_row, 0, row_num - 1, 0, current_user, self.content_format
                        )
                    current_user = user
                    start_row = row_num
                for col_num in range(1, len(group_df.columns)):
                    cell_format = (
                        self.alternate_row_format if row_num % 2 == 0 else self.content_format
                    )
                    worksheet_group.write(row_num, col_num, group_df.iloc[row_num - 1, col_num], cell_format)

            if current_user is not None and len(group_df) > start_row:
                worksheet_group.merge_range(
                    start_row, 0, len(group_df), 0, current_user, self.content_format
                )

            worksheet_group.freeze_panes(1, 0)
            worksheet_group.autofit()

    def _add_charts(self, writer, main_table: List[Dict], daily_stats_table: List[Dict]):
        """在 Excel 中添加图表"""
        workbook = writer.book
        main_df = pd.DataFrame(main_table)
        daily_stats_df = pd.DataFrame(daily_stats_table)

        # 1. 用户击杀数柱状图
        chart1 = workbook.add_chart({"type": "column"})
        chart1.add_series({
            "name": "用户总览!$B$1",
            "categories": f"=用户总览!$A$2:$A${len(main_df) + 1}",
            "values": f"=用户总览!$B$2:$B${len(main_df) + 1}",
        })
        chart1.set_title({"name": "用户击杀数分布"})
        chart1.set_x_axis({"name": "用户"})
        chart1.set_y_axis({"name": "击杀数"})
        writer.sheets["用户总览"].insert_chart("G2", chart1)

        # 2. 每日击杀与被击杀趋势图
        chart2 = workbook.add_chart({"type": "line"})
        chart2.add_series({
            "name": "每日统计!$C$1",
            "categories": f"=每日统计!$B$2:$B${len(daily_stats_df) + 1}",
            "values": f"=每日统计!$C$2:$C${len(daily_stats_df) + 1}",
        })
        chart2.add_series({
            "name": "每日统计!$D$1",
            "categories": f"=每日统计!$B$2:$B${len(daily_stats_df) + 1}",
            "values": f"=每日统计!$D$2:$D${len(daily_stats_df) + 1}",
        })
        chart2.set_title({"name": "每日击杀与被击杀趋势"})
        chart2.set_x_axis({"name": "日期"})
        chart2.set_y_axis({"name": "数量"})
        writer.sheets["每日统计"].insert_chart("G2", chart2)

        # 3. 击杀比分布直方图
        chart3 = workbook.add_chart({"type": "column"})
        chart3.add_series({
            "name": "用户总览!$D$1",
            "categories": f"=用户总览!$A$2:$A${len(main_df) + 1}",
            "values": f"=用户总览!$D$2:$D${len(main_df) + 1}",
        })
        chart3.set_title({"name": "用户击杀比分布"})
        chart3.set_x_axis({"name": "用户"})
        chart3.set_y_axis({"name": "击杀比"})
        writer.sheets["用户总览"].insert_chart("G20", chart3)

        # 4. 战力与击杀效率散点图
        chart4 = workbook.add_chart({"type": "scatter"})
        chart4.add_series({
            "name": "用户总览!$F$1",
            "categories": f"=用户总览!$F$2:$F${len(main_df) + 1}",
            "values": f"=用户总览!$B$2:$B${len(main_df) + 1}",
        })
        chart4.set_title({"name": "战力与击杀效率关系"})
        chart4.set_x_axis({"name": "最高战力"})
        chart4.set_y_axis({"name": "击杀数"})
        writer.sheets["用户总览"].insert_chart("G38", chart4)

        # 5. 活跃度分析图
        chart5 = workbook.add_chart({"type": "column"})
        chart5.add_series({
            "name": "用户总览!$E$1",
            "categories": f"=用户总览!$A$2:$A${len(main_df) + 1}",
            "values": f"=用户总览!$E$2:$E${len(main_df) + 1}",
        })
        chart5.set_title({"name": "用户活跃度分析"})
        chart5.set_x_axis({"name": "用户"})
        chart5.set_y_axis({"name": "活跃天数"})
        writer.sheets["用户总览"].insert_chart("G56", chart5)

        # 6. 战力分布图
        chart6 = workbook.add_chart({"type": "column"})
        chart6.add_series({
            "name": "用户总览!$F$1",
            "categories": f"=用户总览!$A$2:$A${len(main_df) + 1}",
            "values": f"=用户总览!$F$2:$F${len(main_df) + 1}",
        })
        chart6.set_title({"name": "用户战力分布"})
        chart6.set_x_axis({"name": "用户"})
        chart6.set_y_axis({"name": "最高战力"})
        writer.sheets["用户总览"].insert_chart("G74", chart6)


# --- 赛季战场 API（横向对比八个联盟）---
# 线上 JSON 约定（与 https://yx.dmzgame.com/warpath/KvkList 等接口一致）：
#   KvkList     → Data: KvkListItem[]
#   ServerList  → Data: { "legend1"|"legend2"|"gold2"|"gold3"|...: str[] }
#   ServerDetail→ Data: ServerDetailRow[]
WARPATH_API_BASE = "https://yx.dmzgame.com/warpath"
KVK_REQUEST_TIMEOUT = 30


class KvkListItem(TypedDict):
    start_day: int
    end_day: int
    kvkname: str


class ServerDetailRow(TypedDict, total=False):
    """单场 server 下各联盟一条；字段以线上为准，允许增减。"""

    id: int
    start_day: int
    end_day: int
    kvkname: str
    grade: str
    kind: str
    server: str
    gid: int
    wid: int
    sname: str
    fname: str
    owner: str
    power: int
    kil: int
    di: int
    start_power: int
    start_kil: int
    c_power: int
    c_kil: int
    created_at: str
    updated_at: str


def _warpath_ssl_verify() -> bool:
    """若本机证书链不完整导致握手失败，可设置环境变量 WARPATH_SSL_VERIFY=0。"""
    v = os.environ.get("WARPATH_SSL_VERIFY", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _warpath_request_json(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """请求战火公开 API，成功时返回 Data 字段。"""
    url = f"{WARPATH_API_BASE.rstrip('/')}/{path.lstrip('/')}"
    verify = _warpath_ssl_verify()
    try:
        resp = requests.get(
            url, params=params or {}, timeout=KVK_REQUEST_TIMEOUT, verify=verify
        )
    except requests.exceptions.SSLError:
        if verify:
            logging.warning(
                "HTTPS 证书校验失败，已使用 verify=False 重试；"
                "也可设置环境变量 WARPATH_SSL_VERIFY=0 关闭校验。"
            )
            try:
                import urllib3

                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass
            resp = requests.get(
                url, params=params or {}, timeout=KVK_REQUEST_TIMEOUT, verify=False
            )
        else:
            raise
    resp.raise_for_status()
    body = resp.json()
    if body.get("Code") != 0:
        raise RuntimeError(body.get("Message") or f"API 返回 Code={body.get('Code')}")
    return body.get("Data")


def fetch_kvk_season_list() -> List[KvkListItem]:
    """赛季列表，对应 KvkList。"""
    data = _warpath_request_json("KvkList")
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


def fetch_kvk_server_list(end_day: int) -> Dict[str, List[str]]:
    """某赛季各场次服务器编号，对应 ServerList?end_day=。"""
    data = _warpath_request_json("ServerList", {"end_day": end_day})
    if not isinstance(data, dict):
        return {}
    out: Dict[str, List[str]] = {}
    for k, v in data.items():
        if isinstance(v, list):
            out[str(k)] = [str(x) for x in v]
    return out


def fetch_kvk_server_detail(end_day: int, server: str) -> List[ServerDetailRow]:
    """单场服务器内联盟明细，对应 ServerDetail。"""
    data = _warpath_request_json(
        "ServerDetail", {"end_day": int(end_day), "server": str(server).strip()}
    )
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


def _alliance_column_label(row: Dict[str, Any], idx: int) -> str:
    base = (row.get("sname") or row.get("fname") or f"联盟{idx + 1}") or ""
    base = str(base).strip() or f"联盟{idx + 1}"
    return base[:31]


def _kill_death_ratio(row: Dict[str, Any]) -> Union[float, str]:
    di = row.get("di")
    kil = row.get("kil")
    try:
        di_n = int(di) if di is not None else 0
        kil_n = int(kil) if kil is not None else 0
    except (TypeError, ValueError):
        return ""
    if di_n <= 0:
        return ""
    return round(kil_n / di_n, 4)


def build_kvk_horizontal_compare_rows(
    detail_rows: List[Dict[str, Any]], max_alliances: int = 8
) -> Tuple[List[str], List[List[Any]]]:
    """
    按当前战力降序取前 max_alliances 个联盟，构造「指标 × 联盟」横向表。
    返回 (表头, 数据行)，表头第一列为「指标」，其余为各联盟简称。
    """
    sorted_rows = sorted(
        detail_rows,
        key=lambda r: int(r.get("power") or 0),
        reverse=True,
    )[:max_alliances]

    raw_labels = [_alliance_column_label(r, i) for i, r in enumerate(sorted_rows)]
    col_labels = _dedupe_sheet_labels(raw_labels)
    header = ["指标"] + col_labels

    def col_vals(getter: Callable[[Dict[str, Any]], Any]) -> List[Any]:
        return [getter(r) for r in sorted_rows]

    metric_rows: List[List[Any]] = [
        ["全名"] + col_vals(lambda r: r.get("fname", "")),
        ["简称"] + col_vals(lambda r: r.get("sname", "")),
        ["盟主"] + col_vals(lambda r: r.get("owner", "")),
        ["当前战力"] + col_vals(lambda r: int(r.get("power") or 0)),
        ["赛季击杀"] + col_vals(lambda r: int(r.get("kil") or 0)),
        ["赛季死亡(DI)"] + col_vals(lambda r: int(r.get("di") or 0)),
        ["初始战力"] + col_vals(lambda r: int(r.get("start_power") or 0)),
        ["初始击杀"] + col_vals(lambda r: int(r.get("start_kil") or 0)),
        ["战力变化"] + col_vals(lambda r: int(r.get("c_power") or 0)),
        ["击杀变化"] + col_vals(lambda r: int(r.get("c_kil") or 0)),
        ["击杀/死亡比"] + col_vals(_kill_death_ratio),
        ["档位"] + col_vals(lambda r: r.get("grade", "")),
        ["场次类型"] + col_vals(lambda r: r.get("kind", "")),
        ["服务器"] + col_vals(lambda r: r.get("server", "")),
        ["GID"] + col_vals(_safe_int_field("gid")),
    ]
    return header, metric_rows


def _dedupe_sheet_labels(labels: List[str]) -> List[str]:
    """避免简称重复导致 DataFrame 列名冲突。"""
    seen: Dict[str, int] = {}
    out: List[str] = []
    for lb in labels:
        key = lb
        n = seen.get(key, 0) + 1
        seen[key] = n
        if n == 1:
            out.append(lb)
        else:
            suffix = f"_{n}"
            base = lb[: max(0, 31 - len(suffix))] + suffix
            out.append(base[:31])
    return out


def _safe_int_field(field: str) -> Callable[[Dict[str, Any]], Any]:
    def _getter(r: Dict[str, Any]) -> Any:
        v = r.get(field)
        if v is None or v == "":
            return ""
        try:
            return int(v)
        except (TypeError, ValueError):
            return v

    return _getter


def kvk_int_ymd_to_iso(ymd: Union[int, str]) -> str:
    """YYYYMMDD 整数或字符串 → calculate_kills 用的 YYYY-MM-DD。"""
    s = f"{int(ymd):08d}"
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def kvk_guild_roster_day_candidates(start_ymd: int, end_ymd: int) -> List[str]:
    """
    guild_member 的 day 参数在赛季末日可能没有快照；依次尝试：
    末日、首日、赛季中点、末日前若干天、今天（去重保序）。
    """
    from datetime import timedelta

    seen: set = set()
    out: List[str] = []

    def add(ym: Union[int, str]) -> None:
        s = f"{int(ym):08d}"
        if s not in seen:
            seen.add(s)
            out.append(s)

    add(end_ymd)
    add(start_ymd)
    try:
        sd = datetime.strptime(f"{int(start_ymd):08d}", "%Y%m%d")
        ed = datetime.strptime(f"{int(end_ymd):08d}", "%Y%m%d")
        if ed >= sd:
            mid = sd + (ed - sd) / 2
            add(mid.strftime("%Y%m%d"))
        for i in (1, 2, 3, 5, 7, 10, 14):
            d = ed - timedelta(days=i)
            if d >= sd:
                add(d.strftime("%Y%m%d"))
    except (TypeError, ValueError, OSError):
        pass
    add(datetime.now().strftime("%Y%m%d"))
    return out


def _kvk_trimmed_mean_one_min_one_max(arr: List[float]) -> Union[float, str]:
    """
    各指标独立：去掉一个最小值和一个最大值后取算术平均。
    成员数 ≤ 2 时无法再各去一个，退化为全体平均；空列表返回 ""。
    """
    if not arr:
        return ""
    if len(arr) <= 2:
        return float(np.mean(arr))
    s = sorted(arr)
    trimmed = s[1:-1]
    return float(np.mean(trimmed))


def kvk_aggregate_member_averages(member_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    将 calculate_kills 得到的「每名成员一条」统计，按指标分别做「去一高一低后平均」。

    口径（与「联盟明细」里 ServerDetail 的官方字段不是同一套数）：
    - 击杀、被击杀、活跃天数、科技战力：各成员数值排序后去掉一个最小、一个最大，再 np.mean。
    - 成员数 ≤ 2 时上述指标无法去极值，对该指标退化为全体平均。
    - 击杀比：仅对有限数成员收集后同样去一高一低再平均；一般不等于「平均击杀÷平均被击杀」。
    - 「赛季总击杀/总被击杀」= 各成员赛季总击杀数、总被击杀数的加总（pid 口径，非 ServerDetail）。
    """
    if not member_results:
        return {
            "成员数": 0,
            "赛季总击杀": "",
            "赛季总被击杀": "",
            "成员平均赛季击杀": "",
            "成员平均赛季被击杀": "",
            "成员平均击杀比": "",
            "成员平均活跃天数": "",
            "成员平均科技战力": "",
        }
    kills: List[float] = []
    deaths: List[float] = []
    ratios: List[float] = []
    active: List[float] = []
    tech: List[float] = []
    for st in member_results.values():
        kills.append(float(st.get("赛季总击杀数") or 0))
        deaths.append(float(st.get("赛季总被击杀数") or 0))
        r = st.get("赛季击杀/被击杀比")
        if isinstance(r, (int, float)) and np.isfinite(r):
            ratios.append(float(r))
        active.append(float(st.get("活跃天数") or 0))
        tech.append(float(st.get("科技战力") or 0))
    n = len(member_results)
    sum_kills = int(np.sum(kills)) if kills else 0
    sum_deaths = int(np.sum(deaths)) if deaths else 0

    return {
        "成员数": n,
        "赛季总击杀": sum_kills,
        "赛季总被击杀": sum_deaths,
        "成员平均赛季击杀": _kvk_trimmed_mean_one_min_one_max(kills),
        "成员平均赛季被击杀": _kvk_trimmed_mean_one_min_one_max(deaths),
        "成员平均击杀比": _kvk_trimmed_mean_one_min_one_max(ratios) if ratios else "",
        "成员平均活跃天数": _kvk_trimmed_mean_one_min_one_max(active),
        "成员平均科技战力": _kvk_trimmed_mean_one_min_one_max(tech),
    }


async def _kvk_async_fetch_guild_member_avg(
    gid: int,
    start_ymd: int,
    end_ymd: int,
    max_concurrent: int,
) -> Optional[Dict[str, Any]]:
    """
    用 gid 拉 guild_member（赛季末日成员快照）+ 各成员 pid_detail，
    再按赛季起止调用 calculate_kills，返回成员均值字典；失败返回 None。
    """
    from data01 import GuildDataFetcher

    tmp = Path(tempfile.mkdtemp(prefix="kvk_guild_"))
    try:
        gdir = tmp / "guild"
        pdir = tmp / "pid"
        gdir.mkdir(parents=True)
        pdir.mkdir(parents=True)
        s_iso = kvk_int_ymd_to_iso(start_ymd)
        e_iso = kvk_int_ymd_to_iso(end_ymd)
        day_candidates = kvk_guild_roster_day_candidates(start_ymd, end_ymd)
        async with GuildDataFetcher(max_concurrent=max_concurrent) as fetcher:
            fetcher.output_dir = gdir
            fetcher.pid_data_dir = pdir
            fetcher.max_retries = 3
            fetcher.retry_delay = 2
            gd, day_used = await fetcher.fetch_guild_member_with_day_fallback(
                int(gid), day_candidates
            )
            if not gd or gd.get("Code") != 0 or not day_used:
                logging.warning(
                    "guild_member 无有效成员 gid=%s 已试日期=%s",
                    gid,
                    ",".join(day_candidates[:12]) + ("…" if len(day_candidates) > 12 else ""),
                )
                return None
            pids = fetcher.extract_pids(gd, warn_if_empty=False)
            if not pids:
                logging.warning("公会 %s 无成员 PID（回退日后仍为空）", gid)
                return None
            gnick = "Unknown"
            if gd.get("Data"):
                gnick = str(gd["Data"][0].get("gnick") or "Unknown")
            pid_dict = await fetcher.fetch_pid_details(pids, pdir, gnick, int(gid))
        if not pid_dict:
            return None
        results, _ = calculate_kills(pid_dict, s_iso, e_iso)
        return kvk_aggregate_member_averages(results)
    except Exception as e:
        logging.warning("成员均线 gid=%s: %s", gid, e, exc_info=True)
        return None
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


KVK_MEMBER_AVG_SHEET_COLUMNS = [
    "简称",
    "全称",
    "GID",
    "成员数",
    "赛季总击杀",
    "赛季总被击杀",
    "成员平均赛季击杀",
    "成员平均赛季被击杀",
    "成员平均击杀比",
    "成员平均活跃天数",
    "成员平均科技战力",
    "备注",
]


async def kvk_build_member_avg_rows(
    detail_rows: List[Dict[str, Any]],
    start_ymd: int,
    end_ymd: int,
    max_concurrent: int,
    guild_parallel: int = 2,
) -> List[Dict[str, Any]]:
    """对 ServerDetail 中每个联盟并行（受限）拉成员并算场均。"""
    sem = asyncio.Semaphore(max(1, guild_parallel))
    ordered = sorted(
        detail_rows,
        key=lambda x: int(x.get("power") or 0),
        reverse=True,
    )

    async def _one(r: Dict[str, Any]) -> Dict[str, Any]:
        gid = int(r.get("gid") or 0)
        base = {
            "简称": r.get("sname", ""),
            "全称": r.get("fname", ""),
            "GID": gid,
        }
        if not gid:
            base.update(
                {
                    "成员数": "",
                    "赛季总击杀": "",
                    "赛季总被击杀": "",
                    "成员平均赛季击杀": "",
                    "成员平均赛季被击杀": "",
                    "成员平均击杀比": "",
                    "成员平均活跃天数": "",
                    "成员平均科技战力": "",
                    "备注": "无 GID",
                }
            )
            return base
        async with sem:
            avg = await _kvk_async_fetch_guild_member_avg(
                gid, start_ymd, end_ymd, max_concurrent=max_concurrent
            )
        if avg is None:
            base.update(
                {
                    "成员数": "",
                    "赛季总击杀": "",
                    "赛季总被击杀": "",
                    "成员平均赛季击杀": "",
                    "成员平均赛季被击杀": "",
                    "成员平均击杀比": "",
                    "成员平均活跃天数": "",
                    "成员平均科技战力": "",
                    "备注": "成员数据拉取或计算失败",
                }
            )
        else:
            base.update(avg)
            base["备注"] = ""
        return base

    return list(await asyncio.gather(*[_one(r) for r in ordered]))


def _write_df_with_header_style(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
    sheet_name: str,
    header_fmt: Any,
    content_fmt: Any,
    alt_fmt: Any,
    freeze_col: int = 1,
) -> None:
    """与现有导出风格一致：首行表头加粗着色，内容斑马纹。"""
    df.to_excel(writer, index=False, sheet_name=sheet_name)
    ws = writer.sheets[sheet_name]
    for c, name in enumerate(df.columns):
        ws.write(0, c, name, header_fmt)
    for r in range(len(df)):
        row_fmt = alt_fmt if (r + 1) % 2 == 0 else content_fmt
        for c in range(len(df.columns)):
            ws.write(r + 1, c, df.iloc[r, c], row_fmt)
    ws.freeze_panes(1, freeze_col)
    ws.autofit()


def export_kvk_battlefield_excel(
    end_day: int,
    server: str,
    output_file: str,
    max_alliances: int = 8,
    member_avg: bool = True,
    max_concurrent: int = 8,
) -> None:
    """拉取单场 ServerDetail，导出横向对比 + 原始明细；可选按 gid 拉成员算场均。"""
    detail = fetch_kvk_server_detail(end_day, server)
    if not detail:
        logging.warning("ServerDetail 无数据，仍将生成带说明的 Excel")

    header, metric_rows = build_kvk_horizontal_compare_rows(detail, max_alliances=max_alliances)
    horiz_df = pd.DataFrame(metric_rows, columns=header)

    meta = {
        "end_day": end_day,
        "server": server,
        "kvkname": detail[0].get("kvkname", "") if detail else "",
        "start_day": detail[0].get("start_day", "") if detail else "",
        "联盟数量(原始)": len(detail),
        "对比联盟数": min(len(detail), max_alliances),
        "成员场均分析": "是" if member_avg else "否",
        "成员均值算法说明": (
            "联盟成员均值表：对每名成员用 pid_detail+calculate_kills 得到赛季指标后，"
            "「赛季总击杀/总被击杀」为成员口径加总；各「成员平均*」为单列去一高一低后平均；成员≤2 不去极值。"
            "击杀比仅含有限数。联盟明细为 ServerDetail 联盟级 kil 等，与成员加总不可直接对比。"
        ),
    }
    meta_df = pd.DataFrame([meta])

    detail_columns = [
        "id",
        "sname",
        "fname",
        "owner",
        "power",
        "kil",
        "di",
        "start_power",
        "start_kil",
        "c_power",
        "c_kil",
        "grade",
        "kind",
        "server",
        "gid",
        "wid",
        "kvkname",
        "start_day",
        "end_day",
        "created_at",
        "updated_at",
    ]
    detail_records = []
    for r in sorted(detail, key=lambda x: int(x.get("power") or 0), reverse=True):
        detail_records.append({k: r.get(k, "") for k in detail_columns})
    detail_df = pd.DataFrame(detail_records)
    rename_map = {
        "id": "记录ID",
        "sname": "简称",
        "fname": "全名",
        "owner": "盟主",
        "power": "当前战力",
        "kil": "赛季击杀",
        "di": "赛季死亡(DI)",
        "start_power": "初始战力",
        "start_kil": "初始击杀",
        "c_power": "战力变化",
        "c_kil": "击杀变化",
        "grade": "档位",
        "kind": "场次类型",
        "server": "服务器",
        "gid": "GID",
        "wid": "WID",
        "kvkname": "赛季战场名",
        "start_day": "开始日",
        "end_day": "结束日",
        "created_at": "创建时间",
        "updated_at": "更新时间",
    }
    detail_cn = detail_df.rename(columns=rename_map)

    with pd.ExcelWriter(output_file, engine="xlsxwriter", engine_kwargs={"options": {"nan_inf_to_errors": True}}) as writer:
        wb = writer.book
        h_fmt = wb.add_format(HEADER_FORMAT)
        c_fmt = wb.add_format(CONTENT_FORMAT)
        alt_fmt = wb.add_format(ALTERNATE_ROW_FORMAT)
        int_fmt = wb.add_format({**CONTENT_FORMAT, "num_format": "0"})

        meta_df.to_excel(writer, index=False, sheet_name="场次说明")
        mws = writer.sheets["场次说明"]
        for c in range(len(meta_df.columns)):
            mws.write(0, c, meta_df.columns[c], h_fmt)
            mws.write(1, c, meta_df.iloc[0, c], c_fmt)
        mws.autofit()

        _write_df_with_header_style(writer, horiz_df, "联盟横向对比", h_fmt, c_fmt, alt_fmt)

        detail_cn.to_excel(writer, index=False, sheet_name="联盟明细")
        dws = writer.sheets["联盟明细"]
        for c, name in enumerate(detail_cn.columns):
            dws.write(0, c, name, h_fmt)
        num_cols = {
            "记录ID",
            "当前战力",
            "赛季击杀",
            "赛季死亡(DI)",
            "初始战力",
            "初始击杀",
            "战力变化",
            "击杀变化",
            "GID",
            "WID",
            "开始日",
            "结束日",
        }
        for r in range(len(detail_cn)):
            row_fmt = alt_fmt if (r + 1) % 2 == 0 else c_fmt
            num_row_fmt = wb.add_format({**ALTERNATE_ROW_FORMAT, "num_format": "0"}) if (r + 1) % 2 == 0 else int_fmt
            for c, col_name in enumerate(detail_cn.columns):
                val = detail_cn.iloc[r, c]
                fmt = num_row_fmt if col_name in num_cols else row_fmt
                dws.write(r + 1, c, val, fmt)
        dws.freeze_panes(1, 0)
        dws.autofit()

        if member_avg and detail:
            try:
                sd = int(detail[0].get("start_day"))
                ed = int(detail[0].get("end_day"))
            except (TypeError, ValueError, KeyError, IndexError):
                sd, ed = 0, 0
            if sd and ed:
                logging.info(
                    "按 gid 拉取 guild_member + pid_detail，计算成员场均（赛季 %s～%s）…",
                    sd,
                    ed,
                )
                member_rows = asyncio.run(
                    kvk_build_member_avg_rows(
                        detail, sd, ed, max_concurrent=max_concurrent
                    )
                )
                member_df = pd.DataFrame(member_rows)
                for c in KVK_MEMBER_AVG_SHEET_COLUMNS:
                    if c not in member_df.columns:
                        member_df[c] = ""
                member_df = member_df[KVK_MEMBER_AVG_SHEET_COLUMNS]
                _write_df_with_header_style(
                    writer, member_df, "联盟成员均值", h_fmt, c_fmt, alt_fmt
                )

    logging.info(f"赛季战场分析已导出: {output_file}")


def export_kvk_all_servers_excel(
    end_day: int,
    output_file: str,
    max_alliances: int = 8,
) -> None:
    """同一 end_day 下所有场次服务器各导出一张「联盟横向对比」子表（多 sheet）。"""
    server_map = fetch_kvk_server_list(end_day)
    codes: List[Tuple[str, str]] = []
    for kind, srv_list in sorted(server_map.items()):
        for s in srv_list:
            codes.append((kind, s))

    with pd.ExcelWriter(output_file, engine="xlsxwriter", engine_kwargs={"options": {"nan_inf_to_errors": True}}) as writer:
        wb = writer.book
        h_fmt = wb.add_format(HEADER_FORMAT)
        c_fmt = wb.add_format(CONTENT_FORMAT)
        alt_fmt = wb.add_format(ALTERNATE_ROW_FORMAT)

        summary_rows = []
        for kind, server in codes:
            detail = fetch_kvk_server_detail(end_day, server)
            header, metric_rows = build_kvk_horizontal_compare_rows(detail, max_alliances=max_alliances)
            horiz_df = pd.DataFrame(metric_rows, columns=header)
            safe_name = f"{kind}_{server}"[:31]
            _write_df_with_header_style(writer, horiz_df, safe_name, h_fmt, c_fmt, alt_fmt)
            summary_rows.append(
                {
                    "场次类型": kind,
                    "服务器": server,
                    "联盟数": len(detail),
                    "战场名": detail[0].get("kvkname", "") if detail else "",
                }
            )

        sum_df = pd.DataFrame(summary_rows or [{"场次类型": "", "服务器": "", "联盟数": 0, "战场名": ""}])
        sum_df.to_excel(writer, index=False, sheet_name="场次索引")
        sws = writer.sheets["场次索引"]
        for c, name in enumerate(sum_df.columns):
            sws.write(0, c, name, h_fmt)
        for r in range(len(sum_df)):
            rf = alt_fmt if (r + 1) % 2 == 0 else c_fmt
            for c in range(len(sum_df.columns)):
                sws.write(r + 1, c, sum_df.iloc[r, c], rf)
        sws.freeze_panes(1, 0)
        sws.autofit()

    logging.info(f"全场次赛季战场分析已导出: {output_file} (共 {len(codes)} 个服务器)")


def main_kvk_cli(args: argparse.Namespace) -> None:
    if getattr(args, "list_seasons", False):
        seasons = fetch_kvk_season_list()
        for s in seasons:
            print(
                f"{s.get('start_day')} -> {s.get('end_day')}  {s.get('kvkname', '')}"
            )
        if args.output:
            pd.DataFrame(seasons).to_excel(args.output, index=False)
            logging.info(f"赛季列表已写入 {args.output}")
        return

    if getattr(args, "list_servers", False):
        if not args.end_day:
            raise SystemExit("查看场次列表需要 --end-day")
        sm = fetch_kvk_server_list(args.end_day)
        for kind, lst in sorted(sm.items()):
            print(f"{kind}: {', '.join(lst)}")
        if args.output:
            rows = [{"场次类型": k, "服务器": s} for k, v in sorted(sm.items()) for s in v]
            pd.DataFrame(rows).to_excel(args.output, index=False)
            logging.info(f"场次服务器列表已写入 {args.output}")
        return

    if not args.output:
        raise SystemExit("请指定 -o/--output")

    if args.all_servers:
        if not args.end_day:
            raise SystemExit("--all-servers 需要 --end-day")
        export_kvk_all_servers_excel(args.end_day, args.output)
        return

    if not args.end_day:
        raise SystemExit("单场对比需要 --end-day")
    if not args.server:
        raise SystemExit("单场对比需要 --server，或改用 --all-servers")
    export_kvk_battlefield_excel(
        args.end_day,
        args.server,
        args.output,
        member_avg=not args.no_member_avg,
        max_concurrent=args.max_concurrent,
    )


def main(file_path: str, start_date: str, end_date: str, output_file: str, compare: bool = True):
    """主函数"""
    try:
        logging.info("开始加载 JSON 数据...")
        data = load_json(file_path)
        logging.info("开始计算击杀数据...")
        results, power_group_stats = calculate_kills(data, start_date, end_date)
        logging.info("开始导出到 Excel...")
        exporter = ExcelExporter(output_file)
        exporter.export(results, power_group_stats, compare)
        logging.info(f"数据已成功导出到 {output_file}")
    except Exception as e:
        logging.error(f"发生错误: {e}", exc_info=True)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "kvk":
        kvk_parser = argparse.ArgumentParser(
            description="赛季战场分析：调用 KvkList / ServerList / ServerDetail，横向对比八个联盟"
        )
        kvk_parser.add_argument(
            "--end-day",
            type=int,
            default=None,
            help="赛季结束日 YYYYMMDD，与官网 ServerList 参数一致",
        )
        kvk_parser.add_argument(
            "--server",
            type=str,
            default=None,
            help="场次服务器编号，如 0200",
        )
        kvk_parser.add_argument(
            "-o",
            "--output",
            type=str,
            default=None,
            help="输出 Excel 路径",
        )
        kvk_parser.add_argument(
            "--list-seasons",
            action="store_true",
            help="仅列出赛季（可选配合 -o 导出赛季表）",
        )
        kvk_parser.add_argument(
            "--list-servers",
            action="store_true",
            help="列出某 end_day 下各档位服务器编号（需 --end-day）",
        )
        kvk_parser.add_argument(
            "--all-servers",
            action="store_true",
            help="导出该 end_day 下所有场次服务器的横向对比（多 sheet）",
        )
        kvk_parser.add_argument(
            "--no-member-avg",
            action="store_true",
            help="不拉取各联盟成员数据，跳过「联盟成员均值」sheet（默认可省略）",
        )
        kvk_parser.add_argument(
            "--max-concurrent",
            type=int,
            default=8,
            help="拉取 pid_detail 时的最大并发（默认 8）",
        )
        kvk_args = kvk_parser.parse_args(sys.argv[2:])
        main_kvk_cli(kvk_args)
    else:
        parser = argparse.ArgumentParser(description="处理击杀与被击杀数据并导出到 Excel 文件")
        parser.add_argument("file_path", type=str, help="JSON 数据文件路径")
        parser.add_argument("start_date", type=str, help="开始日期，格式为 YYYY-MM-DD")
        parser.add_argument("end_date", type=str, help="结束日期，格式为 YYYY-MM-DD")
        parser.add_argument("output_file", type=str, help="输出 Excel 文件路径")

        args = parser.parse_args()
        main(args.file_path, args.start_date, args.end_date, args.output_file)