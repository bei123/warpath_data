import sys
import asyncio
from datetime import datetime
from pathlib import Path
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                           QTextEdit, QFrame, QFileDialog, QMessageBox,
                           QGroupBox, QGridLayout, QSpinBox, QScrollArea,
                           QProgressBar)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QPalette, QColor
from main import WarpathDataProcessor
import logging

class LogHandler(logging.Handler):
    def __init__(self, signal):
        super().__init__()
        self.signal = signal

    def emit(self, record):
        try:
            msg = self.format(record)
            self.signal.emit(msg)
        except Exception:
            self.handleError(record)

class AnalysisThread(QThread):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    log = pyqtSignal(str)
    progress = pyqtSignal(str)

    def __init__(self, params):
        super().__init__()
        self.params = params
        self.is_running = True

    def stop(self):
        self.is_running = False

    def run(self):
        try:
            # 创建事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # 创建处理器
            processor = WarpathDataProcessor(
                self.params['output_dir'],
                self.params['max_concurrent'],
                self.params['max_retries'],
                self.params['retry_delay']
            )
            
            # 运行分析
            loop.run_until_complete(processor.run_single_guild(
                self.params['gid'],
                self.params['current_date'],
                self.params['start_date'],
                self.params['end_date']
            ))
            
            loop.close()
            self.finished.emit()
        except Exception as e:
            self.error.emit(str(e))

class ModernButton(QPushButton):
    def __init__(self, text, parent=None):
        super().__init__(text, parent)
        self.setMinimumHeight(40)
        self.setCursor(Qt.PointingHandCursor)

class ModernLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumHeight(35)

class ModernTextEdit(QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setMinimumHeight(150)

class WarpathDataGUI(QMainWindow):
    # 定义信号
    log_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("战火公会数据分析工具")
        self.setMinimumSize(1000, 800)
        
        # 创建主窗口部件
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        
        # 创建主布局
        main_layout = QVBoxLayout(main_widget)
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(20, 20, 20, 20)
        
        # 创建输入区域
        input_group = self.create_input_group()
        main_layout.addWidget(input_group)
        
        # 创建日志区域
        log_group = self.create_log_group()
        main_layout.addWidget(log_group)
        
        # 创建进度显示区域
        progress_group = self.create_progress_group()
        main_layout.addWidget(progress_group)
        
        # 连接信号
        self.log_signal.connect(self.log_message)
        
        # 设置样式
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QGroupBox {
                background-color: white;
                border-radius: 8px;
                border: 1px solid #ddd;
                margin-top: 1em;
                padding-top: 1em;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 3px;
                color: #333;
                font-weight: bold;
            }
            QLabel {
                color: #333;
            }
            QLineEdit, QSpinBox {
                padding: 5px;
                border: 1px solid #ddd;
                border-radius: 4px;
                background-color: white;
            }
            QLineEdit:focus, QSpinBox:focus {
                border: 2px solid #4F81BD;
            }
            QPushButton {
                background-color: #4F81BD;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2F5F9E;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
            QTextEdit {
                border: 1px solid #ddd;
                border-radius: 4px;
                background-color: white;
                padding: 5px;
            }
            QProgressBar {
                border: 1px solid #ddd;
                border-radius: 4px;
                text-align: center;
                background-color: #f0f0f0;
            }
            QProgressBar::chunk {
                background-color: #4F81BD;
                border-radius: 3px;
            }
        """)
        
        # 设置日志处理器
        self.setup_logging()
        
        # 初始化分析线程
        self.analysis_thread = None

    def create_input_group(self):
        group = QGroupBox("数据输入")
        layout = QGridLayout()
        
        # 公会ID输入
        layout.addWidget(QLabel("公会ID:"), 0, 0)
        self.gid_edit = ModernLineEdit()
        layout.addWidget(self.gid_edit, 0, 1)
        
        # 日期选择
        today = datetime.now().strftime("%Y%m%d")
        layout.addWidget(QLabel("当前日期:"), 1, 0)
        self.current_date_edit = ModernLineEdit()
        self.current_date_edit.setText(today)
        layout.addWidget(self.current_date_edit, 1, 1)
        
        layout.addWidget(QLabel("开始日期:"), 2, 0)
        self.start_date_edit = ModernLineEdit()
        self.start_date_edit.setText(today)
        layout.addWidget(self.start_date_edit, 2, 1)
        
        layout.addWidget(QLabel("结束日期:"), 3, 0)
        self.end_date_edit = ModernLineEdit()
        self.end_date_edit.setText(today)
        layout.addWidget(self.end_date_edit, 3, 1)
        
        # 输出目录选择
        layout.addWidget(QLabel("输出目录:"), 4, 0)
        self.output_dir_edit = ModernLineEdit()
        self.output_dir_edit.setText("warpath_data")
        layout.addWidget(self.output_dir_edit, 4, 1)
        
        browse_btn = ModernButton("浏览")
        browse_btn.clicked.connect(self.browse_output_dir)
        layout.addWidget(browse_btn, 4, 2)
        
        # 并发设置
        layout.addWidget(QLabel("最大并发数:"), 5, 0)
        self.max_concurrent_spin = QSpinBox()
        self.max_concurrent_spin.setRange(1, 50)
        self.max_concurrent_spin.setValue(10)
        layout.addWidget(self.max_concurrent_spin, 5, 1)
        
        # 重试设置
        layout.addWidget(QLabel("最大重试次数:"), 6, 0)
        self.max_retries_spin = QSpinBox()
        self.max_retries_spin.setRange(1, 10)
        self.max_retries_spin.setValue(3)
        layout.addWidget(self.max_retries_spin, 6, 1)
        
        layout.addWidget(QLabel("重试延迟(秒):"), 7, 0)
        self.retry_delay_spin = QSpinBox()
        self.retry_delay_spin.setRange(1, 10)
        self.retry_delay_spin.setValue(2)
        layout.addWidget(self.retry_delay_spin, 7, 1)
        
        # 按钮区域
        button_layout = QHBoxLayout()
        self.start_btn = ModernButton("开始分析")
        self.start_btn.clicked.connect(self.start_analysis)
        self.clear_btn = ModernButton("清除日志")
        self.clear_btn.clicked.connect(self.clear_log)
        
        button_layout.addWidget(self.start_btn)
        button_layout.addWidget(self.clear_btn)
        layout.addLayout(button_layout, 8, 0, 1, 3)
        
        group.setLayout(layout)
        return group
        
    def create_log_group(self):
        group = QGroupBox("运行日志")
        layout = QVBoxLayout()
        
        self.log_text = ModernTextEdit()
        layout.addWidget(self.log_text)
        
        group.setLayout(layout)
        return group

    def create_progress_group(self):
        group = QGroupBox("进度")
        layout = QVBoxLayout()
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # 设置为循环模式
        self.progress_bar.hide()
        layout.addWidget(self.progress_bar)
        
        self.progress_label = QLabel("")
        layout.addWidget(self.progress_label)
        
        group.setLayout(layout)
        return group
        
    def setup_logging(self):
        # 清除现有的处理器
        logger = logging.getLogger()
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            
        # 配置日志处理器
        log_handler = LogHandler(self.log_signal)
        log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(log_handler)
        
    def log_message(self, message):
        self.log_text.append(message)
        self.log_text.verticalScrollBar().setValue(
            self.log_text.verticalScrollBar().maximum()
        )
        
    def browse_output_dir(self):
        directory = QFileDialog.getExistingDirectory(self, "选择输出目录")
        if directory:
            self.output_dir_edit.setText(directory)
            
    def clear_log(self):
        self.log_text.clear()
        
    def validate_inputs(self):
        try:
            # 验证公会ID
            gid = self.gid_edit.text().strip()
            if not gid:
                QMessageBox.warning(self, "错误", "请输入公会ID")
                return False
                
            # 验证日期格式
            date_format = "%Y%m%d"
            try:
                datetime.strptime(self.current_date_edit.text(), date_format)
                datetime.strptime(self.start_date_edit.text(), date_format)
                datetime.strptime(self.end_date_edit.text(), date_format)
            except ValueError:
                QMessageBox.warning(self, "错误", "日期格式错误，请使用YYYYMMDD格式")
                return False
                
            return True
        except Exception as e:
            QMessageBox.warning(self, "错误", f"输入验证失败: {str(e)}")
            return False
            
    def start_analysis(self):
        if not self.validate_inputs():
            return
            
        # 禁用按钮
        self.start_btn.setEnabled(False)
        self.clear_btn.setEnabled(False)
        self.progress_bar.show()
        self.progress_label.setText("正在分析数据...")
        
        # 准备参数
        params = {
            'gid': int(self.gid_edit.text()),
            'current_date': self.current_date_edit.text(),
            'start_date': self.start_date_edit.text(),
            'end_date': self.end_date_edit.text(),
            'output_dir': self.output_dir_edit.text(),
            'max_concurrent': self.max_concurrent_spin.value(),
            'max_retries': self.max_retries_spin.value(),
            'retry_delay': self.retry_delay_spin.value()
        }
        
        # 创建并启动分析线程
        self.analysis_thread = AnalysisThread(params)
        self.analysis_thread.finished.connect(self.analysis_finished)
        self.analysis_thread.error.connect(self.analysis_error)
        self.analysis_thread.progress.connect(self.update_progress)
        self.analysis_thread.start()
        
    def update_progress(self, message):
        self.progress_label.setText(message)
        
    def analysis_finished(self):
        # 重新启用按钮
        self.start_btn.setEnabled(True)
        self.clear_btn.setEnabled(True)
        self.progress_bar.hide()
        self.progress_label.setText("分析完成！")
        QMessageBox.information(self, "成功", "数据分析完成！")
        
    def analysis_error(self, error_msg):
        # 重新启用按钮
        self.start_btn.setEnabled(True)
        self.clear_btn.setEnabled(True)
        self.progress_bar.hide()
        self.progress_label.setText("分析出错！")
        QMessageBox.critical(self, "错误", f"分析过程中发生错误: {error_msg}")

    def closeEvent(self, event):
        if self.analysis_thread and self.analysis_thread.isRunning():
            self.analysis_thread.stop()
            self.analysis_thread.wait()
        event.accept()

def main():
    app = QApplication(sys.argv)
    window = WarpathDataGUI()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main() 