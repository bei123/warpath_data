import json
from datetime import datetime
import pandas as pd
import argparse
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Any


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


def load_json(file_path: str) -> Dict:
    """加载 JSON 文件并返回数据。

    Args:
        file_path (str): JSON 文件路径。

    Returns:
        Dict: 解析后的 JSON 数据。

    Raises:
        FileNotFoundError: 如果文件未找到。
        json.JSONDecodeError: 如果文件格式错误。
    """
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
    """验证日期字符串是否有效。

    Args:
        date_value (str): 日期字符串。

    Returns:
        Optional[datetime]: 如果日期有效，返回 datetime 对象；否则返回 None。
    """
    date_str = str(date_value)
    if len(date_str) != 8:
        return None
    try:
        return datetime.strptime(date_str, DATE_FORMAT)
    except ValueError:
        return None


def calculate_daily_stats(entry: Dict, start_timestamp: int, end_timestamp: int) -> Optional[Dict]:
    """计算每日统计信息。

    Args:
        entry (Dict): 单条数据记录。
        start_timestamp (int): 开始日期的时间戳。
        end_timestamp (int): 结束日期的时间戳。

    Returns:
        Optional[Dict]: 如果日期在范围内，返回每日统计信息；否则返回 None。
    """
    day = entry["day"]
    valid_date = validate_date(day)
    if valid_date and start_timestamp <= day <= end_timestamp:
        daily_kills = entry.get("c_sumkill", 0)
        daily_deaths = entry.get("c_die", 0)
        daily_kill_ratio = daily_kills / daily_deaths if daily_deaths > 0 else "Infinity"
        max_power = entry.get("maxpower", 0)  
        return {
            "日期": valid_date,
            "当天击杀": daily_kills,
            "当天死亡": daily_deaths,
            "当天击杀比": daily_kill_ratio,
            "当天最高战力": max_power,  
        }
    return None


def get_power_group(max_power: float) -> str:
    """根据战力值返回战力组别。

    Args:
        max_power (float): 用户的最新战力值。

    Returns:
        str: 战力组别。
    """
    if max_power < 150000000:
        return "1亿-1.5亿"
    elif 150000000 <= max_power < 180000000:
        return "1.5亿-1.8亿"
    elif 180000000 <= max_power < 200000000:
        return "1.8亿-2亿"
    elif 200000000 <= max_power < 240000000:
        return "2亿-2.4亿"
    elif 240000000 <= max_power < 270000000:
        return "2.4亿-2.7亿"
    elif 270000000 <= max_power < 300000000:
        return "2.7亿-3亿"
    elif 300000000 <= max_power < 350000000:
        return "3亿-3.5亿"
    else:
        return "3.5亿及以上"


def calculate_summary_stats(daily_stats: List[Dict], max_power: float) -> Dict:
    """计算总体统计信息。

    Args:
        daily_stats (List[Dict]): 每日统计信息列表。
        max_power (float): 用户的最新战力值。

    Returns:
        Dict: 总体统计信息。
    """
    # 计算总击杀数
    total_kills = sum(stat["当天击杀"] for stat in daily_stats)
    # 计算总死亡数
    total_deaths = sum(stat["当天死亡"] for stat in daily_stats)
    # 计算总天数
    total_days = len(daily_stats)
    # 计算平均击杀数
    avg_kills = total_kills / total_days if total_days > 0 else 0
    # 计算平均死亡数
    avg_deaths = total_deaths / total_days if total_days > 0 else 0

    # 根据战力值确定活跃天数阈值
    if max_power < 150000000:
        power_group = "1亿-1.5亿"
        kill_threshold = 500
        death_threshold = 1500
    elif 150000000 <= max_power < 180000000:
        power_group = "1.5亿-1.8亿"
        kill_threshold = 600
        death_threshold = 1200
    elif 180000000 <= max_power < 200000000:
        power_group = "1.8亿-2亿"
        kill_threshold = 800
        death_threshold = 1500
    elif 200000000 <= max_power < 240000000:
        power_group = "2亿-2.4亿"
        kill_threshold = 1200
        death_threshold = 800
    elif 240000000 <= max_power < 270000000:
        power_group = "2.4亿-2.7亿"
        kill_threshold = 1800
        death_threshold = 800
    elif 270000000 <= max_power < 300000000:
        power_group = "2.7亿-3亿"
        kill_threshold = 2500
        death_threshold = 400
    elif 300000000 <= max_power < 350000000:
        power_group = "3亿-3.5亿"
        kill_threshold = 3500
        death_threshold = 2500
    else:
        power_group = "3.5亿及以上"
        kill_threshold = 4000
        death_threshold = 2000

    # 计算活跃天数
    active_days = sum(
        1 for stat in daily_stats
        if stat["当天击杀"] > kill_threshold or stat["当天死亡"] > death_threshold
    )

    # 计算击杀/被击杀比
    kill_death_ratio = total_kills / total_deaths if total_deaths > 0 else "Infinity"

    # 返回总体统计信息
    return {
        "赛季总击杀数": total_kills,
        "赛季总被击杀数": total_deaths,
        "赛季击杀/被击杀比": kill_death_ratio,
        "活跃天数": active_days,
        "战力组别": power_group,
    }


def calculate_kills(data: Dict, start_date: str, end_date: str) -> Dict:
    """计算指定时间范围内的击杀与被击杀数据。

    Args:
        data (Dict): 原始数据。
        start_date (str): 开始日期，格式为 YYYY-MM-DD。
        end_date (str): 结束日期，格式为 YYYY-MM-DD。

    Returns:
        Dict: 计算结果。
    """
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
        }
    )

    
    power_group_stats = defaultdict(lambda: defaultdict(list))

    for pid, info in data.items():
        if info["Code"] == 0 and "Data" in info:
            daily_stats = []
            latest_nick = "Unknown"  
            latest_day = 0  
            max_power = 0  
            end_max_power = 0  

            for entry in info["Data"]:
                
                if "nick" in entry and entry["day"] > latest_day:
                    latest_nick = entry["nick"]
                    latest_day = entry["day"]
                    max_power = entry.get("maxpower", 0)  

                # 计算每日统计
                daily_stat = calculate_daily_stats(entry, start_timestamp, end_timestamp)
                if daily_stat:
                    daily_stats.append(daily_stat)
                    # 更新时间段结束时的最高战力
                    if entry["day"] <= end_timestamp:
                        end_max_power = max(end_max_power, entry.get("maxpower", 0))

                    # 新增：按战力分组存储每个用户的每日击杀和死亡数据
                    power_group = get_power_group(max_power)
                    date_str = datetime.strptime(str(entry["day"]), DATE_FORMAT).strftime("%Y-%m-%d")
                    power_group_stats[power_group][latest_nick].append(
                        {
                            "日期": date_str,
                            "当天击杀": entry.get("c_sumkill", 0),
                            "当天死亡": entry.get("c_die", 0),
                        }
                    )

            if daily_stats:
                
                summary_stats = calculate_summary_stats(daily_stats, max_power)
                user_id = latest_nick  
                results[user_id] = {
                    **summary_stats,
                    "每日统计": daily_stats,
                    "最新战力": max_power,  
                    "结束时的最高战力": end_max_power,  
                }

    return results, power_group_stats  


class ExcelExporter:
    """Excel 导出工具类。"""

    def __init__(self, output_file: str):
        self.output_file = output_file
        self.workbook = None
        self.header_format = None
        self.content_format = None
        self.alternate_row_format = None

    def _init_formats(self):
        """初始化 Excel 样式。"""
        self.header_format = self.workbook.add_format(HEADER_FORMAT)
        self.content_format = self.workbook.add_format(CONTENT_FORMAT)
        self.alternate_row_format = self.workbook.add_format(ALTERNATE_ROW_FORMAT)

    def export(self, results: Dict, power_group_stats: Dict):
        """导出数据到 Excel。

        Args:
            results (Dict): 用户计算结果。
            power_group_stats (Dict): 战力分组统计结果。
        """
        main_table, daily_stats_table, power_group_tables = self._prepare_data(results, power_group_stats)

        with pd.ExcelWriter(self.output_file, engine="xlsxwriter") as writer:
            self.workbook = writer.book
            self._init_formats()

            self._export_main_table(writer, main_table)
            self._export_daily_stats_table(writer, daily_stats_table)

            
            self._export_power_group_tables(writer, power_group_tables, results)

            
            self._add_charts(writer, main_table, daily_stats_table)

    def _prepare_data(self, results: Dict, power_group_stats: Dict) -> (List[Dict], List[Dict], Dict):
        """准备数据。

        Args:
            results (Dict): 用户计算结果。
            power_group_stats (Dict): 战力分组统计结果。

        Returns:
            Tuple[List[Dict], List[Dict], Dict]: 主表、每日统计表和战力分组表数据。
        """
        main_table = []
        daily_stats_table = []

        for user, stats in results.items():
            main_table.append(
                {
                    "用户": user,
                    "赛季总击杀数": stats["赛季总击杀数"],
                    "赛季总被击杀数": stats["赛季总被击杀数"],
                    "赛季击杀/被击杀比": stats["赛季击杀/被击杀比"],
                    "活跃天数": stats["活跃天数"],
                    "结束时的最高战力": stats["结束时的最高战力"],  
                }
            )
            for daily_stat in stats["每日统计"]:
                daily_stats_table.append(
                    {
                        "用户": user,
                        "日期": daily_stat["日期"],
                        "当天击杀": daily_stat["当天击杀"],
                        "当天死亡": daily_stat["当天死亡"],
                        "当天击杀比": daily_stat["当天击杀比"],
                        "当天最高战力": daily_stat["当天最高战力"],  
                    }
                )

        
        power_group_tables = {}
        for power_group, group_data in power_group_stats.items():
            power_group_tables[power_group] = group_data

        return main_table, daily_stats_table, power_group_tables

    def _export_main_table(self, writer, main_table: List[Dict]):
        """导出主表数据。

        Args:
            writer: Excel writer 对象。
            main_table (List[Dict]): 主表数据。
        """
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
        """导出每日统计表数据。

        Args:
            writer: Excel writer 对象。
            daily_stats_table (List[Dict]): 每日统计表数据。
        """
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
                if current_user is not None:
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

        if current_user is not None:
            worksheet_daily.merge_range(
                start_row, 0, len(daily_stats_df), 0, current_user, self.content_format
            )

        worksheet_daily.freeze_panes(1, 0)
        worksheet_daily.autofit()

    def _export_power_group_tables(self, writer, power_group_tables: Dict, results: Dict):
        """导出战力分组表，并对用户名单元格进行合并处理。

        Args:
            writer: Excel writer 对象。
            power_group_tables (Dict): 战力分组数据。
            results (Dict): 用户计算结果。
        """
        for power_group, group_data in power_group_tables.items():
            group_table = []
            for user, stats in group_data.items():
                # 添加每日数据
                for stat in stats:
                    group_table.append(
                        {
                            "用户": user,
                            "日期": stat["日期"],
                            "当天击杀": stat["当天击杀"],
                            "当天死亡": stat["当天死亡"],
                            "赛季总击杀": "",  
                            "赛季总死亡": "", 
                            "活跃天数": "",  
                        }
                    )
               
                if user in results:
                    group_table[-1]["赛季总击杀"] = results[user]["赛季总击杀数"]
                    group_table[-1]["赛季总死亡"] = results[user]["赛季总被击杀数"]
                    group_table[-1]["活跃天数"] = results[user]["活跃天数"]

            group_df = pd.DataFrame(group_table)
            group_df.to_excel(writer, index=False, sheet_name=power_group)
            worksheet_group = writer.sheets[power_group]

            # 设置表头格式
            for col_num, value in enumerate(group_df.columns.values):
                worksheet_group.write(0, col_num, value, self.header_format)

            # 合并用户名单元格
            current_user = None
            start_row = 1
            for row_num in range(1, len(group_df) + 1):
                user = group_df.iloc[row_num - 1, 0]
                if user != current_user:
                    if current_user is not None:
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

            # 合并最后一个用户的单元格
            if current_user is not None:
                worksheet_group.merge_range(
                    start_row, 0, len(group_df), 0, current_user, self.content_format
                )

            worksheet_group.freeze_panes(1, 0)
            worksheet_group.autofit()

    def _add_charts(self, writer, main_table: List[Dict], daily_stats_table: List[Dict]):
        """在 Excel 中添加图表。

        Args:
            writer: Excel writer 对象。
            main_table (List[Dict]): 主表数据。
            daily_stats_table (List[Dict]): 每日统计表数据。
        """
        workbook = writer.book
        main_df = pd.DataFrame(main_table)
        daily_stats_df = pd.DataFrame(daily_stats_table)

        # 1. 用户击杀数柱状图
        chart1 = workbook.add_chart({"type": "column"})
        chart1.add_series({
            "name": "用户总览!$B$1",
            "categories": "=用户总览!$A$2:$A${}".format(len(main_df) + 1),
            "values": "=用户总览!$B$2:$B${}".format(len(main_df) + 1),
        })
        chart1.set_title({"name": "用户击杀数"})
        chart1.set_x_axis({"name": "用户"})
        chart1.set_y_axis({"name": "击杀数"})
        writer.sheets["用户总览"].insert_chart("G2", chart1)

        # 2. 每日击杀与被击杀趋势图
        chart2 = workbook.add_chart({"type": "line"})
        chart2.add_series({
            "name": "每日统计!$C$1",
            "categories": "=每日统计!$B$2:$B${}".format(len(daily_stats_df) + 1),
            "values": "=每日统计!$C$2:$C${}".format(len(daily_stats_df) + 1),
        })
        chart2.add_series({
            "name": "每日统计!$D$1",
            "categories": "=每日统计!$B$2:$B${}".format(len(daily_stats_df) + 1),
            "values": "=每日统计!$D$2:$D${}".format(len(daily_stats_df) + 1),
        })
        chart2.set_title({"name": "每日击杀与被击杀趋势"})
        chart2.set_x_axis({"name": "日期"})
        chart2.set_y_axis({"name": "数量"})
        writer.sheets["每日统计"].insert_chart("G2", chart2)

        # 3. 击杀比饼图
        chart3 = workbook.add_chart({"type": "pie"})
        chart3.add_series({
            "name": "用户总览!$D$1",
            "categories": "=用户总览!$A$2:$A${}".format(len(main_df) + 1),
            "values": "=用户总览!$D$2:$D${}".format(len(main_df) + 1),
        })
        chart3.set_title({"name": "用户击杀比分布"})
        writer.sheets["用户总览"].insert_chart("G20", chart3)

        # 4. 活跃天数散点图
        chart4 = workbook.add_chart({"type": "scatter"})
        chart4.add_series({
            "name": "用户总览!$E$1",
            "categories": "=用户总览!$B$2:$B${}".format(len(main_df) + 1),
            "values": "=用户总览!$E$2:$E${}".format(len(main_df) + 1),
        })
        chart4.set_title({"name": "活跃天数与击杀数关系"})
        chart4.set_x_axis({"name": "击杀数"})
        chart4.set_y_axis({"name": "活跃天数"})
        writer.sheets["用户总览"].insert_chart("G38", chart4)

        # 5. 用户结束时最高战力柱状图
        chart5 = workbook.add_chart({"type": "column"})
        chart5.add_series({
            "name": "用户总览!$F$1",
            "categories": "=用户总览!$A$2:$A${}".format(len(main_df) + 1),
            "values": "=用户总览!$F$2:$F${}".format(len(main_df) + 1),
        })
        chart5.set_title({"name": "用户结束时最高战力"})
        chart5.set_x_axis({"name": "用户"})
        chart5.set_y_axis({"name": "最高战力"})
        writer.sheets["用户总览"].insert_chart("G56", chart5)


def main(file_path: str, start_date: str, end_date: str, output_file: str):
    """主函数。

    Args:
        file_path (str): JSON 数据文件路径。
        start_date (str): 开始日期，格式为 YYYY-MM-DD。
        end_date (str): 结束日期，格式为 YYYY-MM-DD。
        output_file (str): 输出 Excel 文件路径。
    """
    try:
        logging.info("开始加载 JSON 数据...")
        data = load_json(file_path)
        logging.info("开始计算击杀数据...")
        results, power_group_stats = calculate_kills(data, start_date, end_date) 
        logging.info("开始导出到 Excel...")
        exporter = ExcelExporter(output_file)
        exporter.export(results, power_group_stats)  
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