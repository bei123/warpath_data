import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import asyncio
from datetime import datetime
from pathlib import Path
from main import WarpathDataProcessor
import logging
import sys

class WarpathDataGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("战火公会数据分析工具")
        self.root.geometry("800x600")
        
        # 创建主框架
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 创建输入区域
        self.create_input_area()
        
        # 创建输出区域
        self.create_output_area()
        
        # 创建按钮区域
        self.create_button_area()
        
        # 创建日志区域
        self.create_log_area()
        
        # 设置日志处理器
        self.setup_logging()
        
    def create_input_area(self):
        # 公会ID输入
        ttk.Label(self.main_frame, text="公会ID:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.gid_var = tk.StringVar()
        self.gid_entry = ttk.Entry(self.main_frame, textvariable=self.gid_var, width=30)
        self.gid_entry.grid(row=0, column=1, sticky=tk.W, pady=5)
        
        # 日期选择
        ttk.Label(self.main_frame, text="当前日期:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.current_date_var = tk.StringVar(value=datetime.now().strftime("%Y%m%d"))
        self.current_date_entry = ttk.Entry(self.main_frame, textvariable=self.current_date_var, width=30)
        self.current_date_entry.grid(row=1, column=1, sticky=tk.W, pady=5)
        
        ttk.Label(self.main_frame, text="开始日期:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.start_date_var = tk.StringVar(value=datetime.now().strftime("%Y%m%d"))
        self.start_date_entry = ttk.Entry(self.main_frame, textvariable=self.start_date_var, width=30)
        self.start_date_entry.grid(row=2, column=1, sticky=tk.W, pady=5)
        
        ttk.Label(self.main_frame, text="结束日期:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.end_date_var = tk.StringVar(value=datetime.now().strftime("%Y%m%d"))
        self.end_date_entry = ttk.Entry(self.main_frame, textvariable=self.end_date_var, width=30)
        self.end_date_entry.grid(row=3, column=1, sticky=tk.W, pady=5)
        
        # 输出目录选择
        ttk.Label(self.main_frame, text="输出目录:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.output_dir_var = tk.StringVar(value="warpath_data")
        self.output_dir_entry = ttk.Entry(self.main_frame, textvariable=self.output_dir_var, width=30)
        self.output_dir_entry.grid(row=4, column=1, sticky=tk.W, pady=5)
        ttk.Button(self.main_frame, text="浏览", command=self.browse_output_dir).grid(row=4, column=2, sticky=tk.W, pady=5)
        
        # 并发设置
        ttk.Label(self.main_frame, text="最大并发数:").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.max_concurrent_var = tk.StringVar(value="10")
        self.max_concurrent_entry = ttk.Entry(self.main_frame, textvariable=self.max_concurrent_var, width=30)
        self.max_concurrent_entry.grid(row=5, column=1, sticky=tk.W, pady=5)
        
        # 重试设置
        ttk.Label(self.main_frame, text="最大重试次数:").grid(row=6, column=0, sticky=tk.W, pady=5)
        self.max_retries_var = tk.StringVar(value="3")
        self.max_retries_entry = ttk.Entry(self.main_frame, textvariable=self.max_retries_var, width=30)
        self.max_retries_entry.grid(row=6, column=1, sticky=tk.W, pady=5)
        
        ttk.Label(self.main_frame, text="重试延迟(秒):").grid(row=7, column=0, sticky=tk.W, pady=5)
        self.retry_delay_var = tk.StringVar(value="2")
        self.retry_delay_entry = ttk.Entry(self.main_frame, textvariable=self.retry_delay_var, width=30)
        self.retry_delay_entry.grid(row=7, column=1, sticky=tk.W, pady=5)
        
    def create_output_area(self):
        # 创建输出文本框
        self.output_text = tk.Text(self.main_frame, height=10, width=70)
        self.output_text.grid(row=8, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)
        
    def create_button_area(self):
        # 创建按钮框架
        button_frame = ttk.Frame(self.main_frame)
        button_frame.grid(row=9, column=0, columnspan=3, pady=10)
        
        # 添加按钮
        ttk.Button(button_frame, text="开始分析", command=self.start_analysis).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="清除日志", command=self.clear_log).pack(side=tk.LEFT, padx=5)
        
    def create_log_area(self):
        # 创建日志文本框
        self.log_text = tk.Text(self.main_frame, height=10, width=70)
        self.log_text.grid(row=10, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)
        
    def setup_logging(self):
        # 创建自定义日志处理器
        class TextHandler(logging.Handler):
            def __init__(self, text_widget):
                logging.Handler.__init__(self)
                self.text_widget = text_widget
                
            def emit(self, record):
                msg = self.format(record)
                def append():
                    self.text_widget.configure(state='normal')
                    self.text_widget.insert(tk.END, msg + '\n')
                    self.text_widget.configure(state='disabled')
                    self.text_widget.see(tk.END)
                self.text_widget.after(0, append)
        
        # 配置日志处理器
        text_handler = TextHandler(self.log_text)
        text_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(text_handler)
        
    def browse_output_dir(self):
        directory = filedialog.askdirectory()
        if directory:
            self.output_dir_var.set(directory)
            
    def clear_log(self):
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')
        
    def validate_inputs(self):
        try:
            # 验证公会ID
            gid = self.gid_var.get().strip()
            if not gid:
                messagebox.showerror("错误", "请输入公会ID")
                return False
                
            # 验证日期格式
            date_format = "%Y%m%d"
            try:
                datetime.strptime(self.current_date_var.get(), date_format)
                datetime.strptime(self.start_date_var.get(), date_format)
                datetime.strptime(self.end_date_var.get(), date_format)
            except ValueError:
                messagebox.showerror("错误", "日期格式错误，请使用YYYYMMDD格式")
                return False
                
            # 验证数值输入
            try:
                int(self.max_concurrent_var.get())
                int(self.max_retries_var.get())
                int(self.retry_delay_var.get())
            except ValueError:
                messagebox.showerror("错误", "并发数、重试次数和延迟必须是整数")
                return False
                
            return True
        except Exception as e:
            messagebox.showerror("错误", f"输入验证失败: {str(e)}")
            return False
            
    async def run_analysis(self):
        try:
            # 创建处理器
            processor = WarpathDataProcessor(
                self.output_dir_var.get(),
                int(self.max_concurrent_var.get()),
                int(self.max_retries_var.get()),
                int(self.retry_delay_var.get())
            )
            
            # 运行分析
            await processor.run_single_guild(
                int(self.gid_var.get()),
                self.current_date_var.get(),
                self.start_date_var.get(),
                self.end_date_var.get()
            )
            
            messagebox.showinfo("成功", "数据分析完成！")
        except Exception as e:
            messagebox.showerror("错误", f"分析过程中发生错误: {str(e)}")
            
    def start_analysis(self):
        if not self.validate_inputs():
            return
            
        # 禁用按钮
        for widget in self.main_frame.winfo_children():
            if isinstance(widget, ttk.Button):
                widget.configure(state='disabled')
                
        # 清除输出
        self.output_text.configure(state='normal')
        self.output_text.delete(1.0, tk.END)
        self.output_text.configure(state='disabled')
        
        # 运行异步任务
        asyncio.run(self.run_analysis())
        
        # 重新启用按钮
        for widget in self.main_frame.winfo_children():
            if isinstance(widget, ttk.Button):
                widget.configure(state='normal')

def main():
    root = tk.Tk()
    app = WarpathDataGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main() 