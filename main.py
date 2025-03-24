import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
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
    
    async def collect_guild_data(self, gid: int, current_date: str) -> List[Dict]:
        """收集公会数据
        
        Args:
            gid (int): 公会ID
            current_date (str): 当前日期 (YYYYMMDD格式)
        """
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
            
            return [result]
    
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
    
    def process_data(self, start_date: str, end_date: str, output_file: str):
        """处理数据并生成报告"""
        logging.info("开始处理数据...")
        try:
            # 转换日期格式
            formatted_start_date = convert_date_format(start_date)
            formatted_end_date = convert_date_format(end_date)
            
            process_data(
                str(self.pid_data_dir / "hi20pids_data.json"),
                formatted_start_date,
                formatted_end_date,
                output_file
            )
            logging.info(f"数据处理完成，报告已保存到: {output_file}")
        except Exception as e:
            logging.error(f"数据处理失败: {e}")
            raise
    
    async def run(self, gid: int, current_date: str, start_date: str, end_date: str):
        """运行完整的数据处理流程"""
        try:
            # 1. 收集公会数据（使用当天数据）
            guild_results = await self.collect_guild_data(gid, current_date)
            
            # 保存公会数据汇总报告
            report_file = self.report_dir / f"guild_report_{gid}_{current_date}.json"
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(guild_results, f, ensure_ascii=False, indent=4)
            logging.info(f"公会数据汇总报告已保存到: {report_file}")
            
            # 2. 收集PID详细信息
            pid_data = await self.collect_pid_details(guild_results)
            
            # 3. 处理数据并生成最终报告
            output_file = self.report_dir / f"final_report_{gid}_{current_date}.xlsx"
            self.process_data(start_date, end_date, str(output_file))
            
            logging.info("所有数据处理完成！")
            
        except Exception as e:
            logging.error(f"处理过程中发生错误: {e}", exc_info=True)
            raise

async def main_async():
    parser = argparse.ArgumentParser(description="战火数据处理工具")
    parser.add_argument("--gid", type=int, required=True, help="公会ID")
    parser.add_argument("--current-date", type=str, help="当前日期 (YYYYMMDD格式)，默认为今天")
    parser.add_argument("--start-date", type=str, help="开始日期 (YYYYMMDD格式)，默认为今天")
    parser.add_argument("--end-date", type=str, help="结束日期 (YYYYMMDD格式)，默认为今天")
    parser.add_argument("--output-dir", type=str, default="warpath_data", help="输出目录，默认为'warpath_data'")
    parser.add_argument("--max-concurrent", type=int, default=10, help="最大并发请求数")
    parser.add_argument("--max-retries", type=int, default=3, help="最大重试次数")
    parser.add_argument("--retry-delay", type=int, default=2, help="重试延迟（秒）")
    
    args = parser.parse_args()
    
    # 设置默认日期
    today = datetime.now()
    if not args.current_date:
        args.current_date = today.strftime("%Y%m%d")
    if not args.start_date:
        args.start_date = today.strftime("%Y%m%d")
    if not args.end_date:
        args.end_date = today.strftime("%Y%m%d")
    
    # 创建处理器并运行
    processor = WarpathDataProcessor(
        args.output_dir,
        args.max_concurrent,
        args.max_retries,
        args.retry_delay
    )
    await processor.run(args.gid, args.current_date, args.start_date, args.end_date)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main() 