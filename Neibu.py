import json
from datetime import datetime
import pandas as pd
import argparse
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from functools import lru_cache

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

def calculate_daily_stats(entry: Dict, start_timestamp: int, end_timestamp: int) -> Optional[Dict]:
    """计算每日统计信息"""
    day = entry["day"]
    valid_date = validate_date(day)
    if valid_date and start_timestamp <= day <= end_timestamp:
        daily_kills = entry.get("c_sumkill", 0)
        daily_deaths = entry.get("c_die", 0)
        daily_kill_ratio = daily_kills / daily_deaths if daily_deaths > 0 else float('inf')
        max_power = entry.get("maxpower", 0)
        power_growth = entry.get("power_growth", 0)  # 每日战力增长
        
        # 计算战斗活跃度得分（0-100）
        battle_score = min(100, (daily_kills / 200 + daily_deaths / 300) * 50)
        
        return {
            "日期": valid_date,
            "当天击杀": daily_kills,
            "当天死亡": daily_deaths,
            "当天击杀比": daily_kill_ratio,
            "当天最高战力": max_power,
            "战力增长": power_growth,
            "战斗活跃度": battle_score
        }
    return None

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
        }

    # 使用numpy进行向量化计算
    kills = np.array([stat["当天击杀"] for stat in daily_stats])
    deaths = np.array([stat["当天死亡"] for stat in daily_stats])
    power_growths = np.array([stat.get("战力增长", 0) for stat in daily_stats])
    battle_scores = np.array([stat.get("战斗活跃度", 0) for stat in daily_stats])
    
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
    }

def process_user_data(user_data: Dict, start_timestamp: int, end_timestamp: int) -> Tuple[Dict, List[Dict], str, float]:
    """处理单个用户的数据"""
    daily_stats = []
    latest_nick = "Unknown"
    latest_day = 0
    max_power = 0
    end_max_power = 0

    for entry in user_data["Data"]:
        if "nick" in entry and entry["day"] > latest_day:
            latest_nick = entry["nick"]
            latest_day = entry["day"]
            max_power = entry.get("maxpower", 0)

        daily_stat = calculate_daily_stats(entry, start_timestamp, end_timestamp)
        if daily_stat:
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
                "平均战斗活跃度": stats["平均战斗活跃度"]
            })
            daily_stats_table.extend([
                {
                    "用户": user,
                    "日期": stat["日期"],
                    "当天击杀": stat["当天击杀"],
                    "当天死亡": stat["当天死亡"],
                    "当天击杀比": stat["当天击杀比"],
                    "当天最高战力": stat["当天最高战力"],
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
                    group_table.append({
                        "用户": user,
                        "日期": stat["日期"],
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
    parser = argparse.ArgumentParser(description="处理击杀与被击杀数据并导出到 Excel 文件")
    parser.add_argument("file_path", type=str, help="JSON 数据文件路径")
    parser.add_argument("start_date", type=str, help="开始日期，格式为 YYYY-MM-DD")
    parser.add_argument("end_date", type=str, help="结束日期，格式为 YYYY-MM-DD")
    parser.add_argument("output_file", type=str, help="输出 Excel 文件路径")

    args = parser.parse_args()
    main(args.file_path, args.start_date, args.end_date, args.output_file)