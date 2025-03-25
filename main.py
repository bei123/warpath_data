import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
import logging
import asyncio
from data01 import GuildDataFetcher, get_date_range
from Neibu import main as process_data

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('warpath_data.log'),
        logging.StreamHandler()
    ]
)

def convert_date_format(date_str: str, from_format: str = "%Y%m%d", to_format: str = "%Y-%m-%d") -> str:
    """转换日期格式
    
    Args:
        date_str (str): 日期字符串
        from_format (str): 输入日期格式
        to_format (str): 输出日期格式
        
    Returns:
        str: 转换后的日期字符串
    """
    try:
        date_obj = datetime.strptime(date_str, from_format)
        return date_obj.strftime(to_format)
    except ValueError as e:
        logging.error(f"日期格式转换错误: {e}")
        raise

def parse_guild_ids(gids_str: str) -> List[int]:
    """解析公会ID列表"""
    try:
        return [int(gid.strip()) for gid in gids_str.split(',')]
    except ValueError as e:
        logging.error(f"公会ID格式错误: {e}")
        raise

class WarpathDataProcessor:
    """战火数据处理主程序"""
    
    def __init__(self, output_dir: Optional[str] = None, max_concurrent: int = 10, max_retries: int = 3, retry_delay: int = 2):
        """
        初始化数据处理器
        
        Args:
            output_dir (Optional[str]): 输出目录路径，如果为None则使用默认路径
            max_concurrent (int): 最大并发请求数
            max_retries (int): 最大重试次数
            retry_delay (int): 重试延迟（秒）
        """
        self.output_dir = Path(output_dir) if output_dir else Path("warpath_data")
        self.output_dir.mkdir(exist_ok=True)
        self.guild_data_dir = self.output_dir / "guild_data"
        self.pid_data_dir = self.output_dir / "pid_data"
        self.report_dir = self.output_dir / "reports"
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # 创建必要的目录
        for dir_path in [self.guild_data_dir, self.pid_data_dir, self.report_dir]:
            dir_path.mkdir(exist_ok=True)
    
    async def collect_guild_data(self, gid: int, current_date: str) -> Dict:
        """收集单个公会数据"""
        async with GuildDataFetcher(max_concurrent=self.max_concurrent) as fetcher:
            fetcher.output_dir = self.guild_data_dir
            fetcher.max_retries = self.max_retries
            fetcher.retry_delay = self.retry_delay
            
            logging.info(f"开始收集公会 {gid} 在 {current_date} 的数据...")
            result = await fetcher.process_guild_data(gid, current_date)
            
            if result["success"]:
                logging.info(f"成功获取数据，找到 {result['pid_count']} 个PID")
            else:
                logging.error(f"获取数据失败: {result['message']}")
            
            return result

    async def collect_multiple_guilds_data(self, gids: List[int], current_date: str) -> Dict[int, Dict]:
        """并发收集多个公会数据"""
        tasks = [self.collect_guild_data(gid, current_date) for gid in gids]
        results = await asyncio.gather(*tasks)
        return {gid: result for gid, result in zip(gids, results)}
    
    async def collect_pid_details(self, results: List[Dict]) -> Dict:
        """收集PID详细信息"""
        all_pids = set()
        for result in results:
            if result["success"]:
                all_pids.update(result["pids"])
        
        logging.info(f"开始获取 {len(all_pids)} 个PID的详细信息...")
        async with GuildDataFetcher(max_concurrent=self.max_concurrent) as fetcher:
            fetcher.max_retries = self.max_retries
            fetcher.retry_delay = self.retry_delay
            return await fetcher.fetch_pid_details(list(all_pids), self.pid_data_dir)
    
    def process_data(self, start_date: str, end_date: str, output_file: str, compare: bool = True, guild_id: Optional[int] = None):
        """处理数据并生成报告"""
        logging.info("开始处理数据...")
        try:
            # 转换日期格式
            formatted_start_date = convert_date_format(start_date)
            formatted_end_date = convert_date_format(end_date)
            
            # 根据是否指定公会ID选择数据文件
            if guild_id is not None:
                pid_file = self.pid_data_dir / f"guild_{guild_id}_pids_data.json"
            else:
                pid_file = self.pid_data_dir / "hi20pids_data.json"
            
            process_data(
                str(pid_file),
                formatted_start_date,
                formatted_end_date,
                output_file,
                compare
            )
            logging.info(f"数据处理完成，报告已保存到: {output_file}")
        except Exception as e:
            logging.error(f"数据处理失败: {e}")
            raise
    
    async def run_single_guild(self, gid: int, current_date: str, start_date: str, end_date: str):
        """运行单个公会的数据处理流程"""
        try:
            # 1. 收集公会数据
            guild_results = await self.collect_guild_data(gid, current_date)
            
            # 保存公会数据汇总报告
            report_file = self.report_dir / f"guild_report_{gid}_{current_date}.json"
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(guild_results, f, ensure_ascii=False, indent=4)
            logging.info(f"公会数据汇总报告已保存到: {report_file}")
            
            # 2. 收集PID详细信息
            pid_data = await self.collect_pid_details([guild_results])
            
            # 3. 处理数据并生成最终报告
            output_file = self.report_dir / f"final_report_{gid}_{current_date}.xlsx"
            self.process_data(start_date, end_date, str(output_file), compare=False, guild_id=gid)
            
            logging.info(f"公会 {gid} 的数据处理完成！")
            
        except Exception as e:
            logging.error(f"处理公会 {gid} 数据时发生错误: {e}", exc_info=True)
            raise

    async def run_multiple_guilds(self, gids: List[int], current_date: str, start_date: str, end_date: str, compare: bool = True):
        """运行多个公会的数据处理流程"""
        try:
            # 1. 收集所有公会数据
            guild_results = await self.collect_multiple_guilds_data(gids, current_date)
            
            # 保存公会数据汇总报告
            report_file = self.report_dir / f"guilds_report_{current_date}.json"
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(guild_results, f, ensure_ascii=False, indent=4)
            logging.info(f"多公会数据汇总报告已保存到: {report_file}")
            
            # 2. 收集所有PID详细信息
            pid_data = await self.collect_pid_details(list(guild_results.values()))
            
            # 3. 为每个公会生成独立的报告
            for gid in gids:
                # 筛选该公会的PID数据
                guild_pid_data = {
                    pid: data for pid, data in pid_data.items()
                    if pid in guild_results[gid].get("pids", [])
                }
                
                # 保存该公会的PID数据
                guild_pid_file = self.pid_data_dir / f"guild_{gid}_pids_data.json"
                with open(guild_pid_file, "w", encoding="utf-8") as f:
                    json.dump(guild_pid_data, f, ensure_ascii=False, indent=4)
                
                # 处理数据并生成该公会的报告
                output_file = self.report_dir / f"final_report_guild_{gid}_{current_date}.xlsx"
                self.process_data(start_date, end_date, str(output_file), compare=False, guild_id=gid)
                logging.info(f"公会 {gid} 的报告已生成: {output_file}")
            
            logging.info("所有公会的数据处理完成！")
            
        except Exception as e:
            logging.error(f"处理多公会数据时发生错误: {e}", exc_info=True)
            raise

async def main_async():
    parser = argparse.ArgumentParser(description="战火数据处理工具")
    parser.add_argument("--gid", type=str, help="单个公会ID")
    parser.add_argument("--gids", type=str, help="多个公会ID，用逗号分隔")
    parser.add_argument("--current-date", type=str, help="当前日期 (YYYYMMDD格式)，默认为今天")
    parser.add_argument("--start-date", type=str, help="开始日期 (YYYYMMDD格式)，默认为今天")
    parser.add_argument("--end-date", type=str, help="结束日期 (YYYYMMDD格式)，默认为今天")
    parser.add_argument("--output-dir", type=str, default="warpath_data", help="输出目录，默认为'warpath_data'")
    parser.add_argument("--max-concurrent", type=int, default=10, help="最大并发请求数")
    parser.add_argument("--max-retries", type=int, default=3, help="最大重试次数")
    parser.add_argument("--retry-delay", type=int, default=2, help="重试延迟（秒）")
    parser.add_argument("--compare", type=bool, default=True, help="是否生成公会间对比报告")
    
    args = parser.parse_args()
    
    # 验证参数
    if not args.gid and not args.gids:
        parser.error("必须提供 --gid 或 --gids 参数")
    if args.gid and args.gids:
        parser.error("不能同时提供 --gid 和 --gids 参数")
    
    # 设置默认日期
    today = datetime.now()
    if not args.current_date:
        args.current_date = today.strftime("%Y%m%d")
    if not args.start_date:
        args.start_date = today.strftime("%Y%m%d")
    if not args.end_date:
        args.end_date = today.strftime("%Y%m%d")
    
    # 创建处理器
    processor = WarpathDataProcessor(
        args.output_dir,
        args.max_concurrent,
        args.max_retries,
        args.retry_delay
    )
    
    # 根据参数选择运行模式
    if args.gid:
        await processor.run_single_guild(
            int(args.gid),
            args.current_date,
            args.start_date,
            args.end_date
        )
    else:
        gids = parse_guild_ids(args.gids)
        await processor.run_multiple_guilds(
            gids,
            args.current_date,
            args.start_date,
            args.end_date,
            args.compare
        )

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main() 