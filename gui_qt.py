import sys
import asyncio
from datetime import datetime
from pathlib import Path
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                           QTextEdit, QFrame, QFileDialog, QMessageBox,
                           QGroupBox, QGridLayout, QSpinBox, QScrollArea,
                           QProgressBar, QCheckBox, QDesktopWidget, QDateEdit)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QSize, QDate
from PyQt5.QtGui import QFont, QPalette, QColor, QScreen
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
        
        # 获取屏幕信息
        screen = QApplication.primaryScreen()
        screen_geometry = screen.geometry()
        screen_width = screen_geometry.width()
        screen_height = screen_geometry.height()
        
        # 计算窗口大小（屏幕大小的60%）
        window_width = int(screen_width * 0.6)
        window_height = int(screen_height * 0.6)
        
        # 设置窗口大小
        self.resize(window_width, window_height)
        
        # 计算基础字体大小
        base_font_size = max(10, int(min(screen_width, screen_height) * 0.01))
        
        # 创建主窗口部件
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        
        # 创建主布局
        main_layout = QVBoxLayout(main_widget)
        main_layout.setSpacing(int(window_height * 0.02))
        main_layout.setContentsMargins(
            int(window_width * 0.02),
            int(window_height * 0.02),
            int(window_width * 0.02),
            int(window_height * 0.02)
        )
        
        # 创建标题
        title_label = QLabel("战火公会数据分析工具")
        title_label.setStyleSheet(f"""
            QLabel {{
                font-size: {int(base_font_size * 2)}px;
                font-weight: bold;
                color: #2c3e50;
                padding: {int(window_height * 0.01)}px;
                border-bottom: 2px solid #3498db;
            }}
        """)
        main_layout.addWidget(title_label)
        
        # 创建输入区域
        input_group = self.create_input_group(base_font_size, window_width, window_height)
        main_layout.addWidget(input_group)
        
        # 创建日志区域
        log_group = self.create_log_group(base_font_size, window_width, window_height)
        main_layout.addWidget(log_group)
        
        # 创建进度显示区域
        progress_group = self.create_progress_group(base_font_size, window_width, window_height)
        main_layout.addWidget(progress_group)
        
        # 连接信号
        self.log_signal.connect(self.log_message)
        
        # 设置样式
        self.setStyleSheet(f"""
            QMainWindow {{
                background-color: #f8f9fa;
            }}
            QGroupBox {{
                background-color: white;
                border-radius: {int(base_font_size * 0.8)}px;
                border: 2px solid #e9ecef;
                margin-top: {int(base_font_size * 1.2)}px;
                padding: {int(base_font_size * 1.2)}px;
                font-size: {int(base_font_size * 1.2)}px;
                font-weight: bold;
                color: #2c3e50;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: {int(base_font_size * 1.2)}px;
                padding: 0 {int(base_font_size * 0.4)}px;
                color: #2c3e50;
                font-weight: bold;
            }}
            QLabel {{
                color: #2c3e50;
                font-size: {base_font_size}px;
            }}
            QLineEdit, QSpinBox {{
                padding: {int(base_font_size * 0.6)}px;
                border: 2px solid #e9ecef;
                border-radius: {int(base_font_size * 0.5)}px;
                background-color: white;
                font-size: {base_font_size}px;
                min-height: {int(base_font_size * 2.5)}px;
            }}
            QLineEdit:focus, QSpinBox:focus {{
                border: 2px solid #3498db;
            }}
            QLineEdit::placeholder {{
                color: #adb5bd;
            }}
            QPushButton {{
                background-color: #3498db;
                color: white;
                border: none;
                border-radius: {int(base_font_size * 0.5)}px;
                padding: {int(base_font_size * 0.8)}px {int(base_font_size * 1.5)}px;
                font-weight: bold;
                font-size: {base_font_size}px;
                min-height: {int(base_font_size * 2.5)}px;
            }}
            QPushButton:hover {{
                background-color: #2980b9;
            }}
            QPushButton:pressed {{
                background-color: #2472a4;
            }}
            QPushButton:disabled {{
                background-color: #bdc3c7;
            }}
            QTextEdit {{
                border: 2px solid #e9ecef;
                border-radius: {int(base_font_size * 0.5)}px;
                background-color: white;
                padding: {int(base_font_size * 0.8)}px;
                font-family: 'Consolas', monospace;
                font-size: {int(base_font_size * 0.9)}px;
                line-height: 1.5;
            }}
            QProgressBar {{
                border: 2px solid #e9ecef;
                border-radius: {int(base_font_size * 0.5)}px;
                text-align: center;
                background-color: #f8f9fa;
                height: {int(base_font_size * 1.5)}px;
            }}
            QProgressBar::chunk {{
                background-color: #3498db;
                border-radius: {int(base_font_size * 0.3)}px;
            }}
            QCheckBox {{
                font-size: {base_font_size}px;
                color: #2c3e50;
            }}
            QCheckBox::indicator {{
                width: {int(base_font_size * 1.4)}px;
                height: {int(base_font_size * 1.4)}px;
                border-radius: {int(base_font_size * 0.3)}px;
                border: 2px solid #e9ecef;
            }}
            QCheckBox::indicator:checked {{
                background-color: #3498db;
                border: 2px solid #3498db;
            }}
            QCheckBox::indicator:unchecked:hover {{
                border: 2px solid #3498db;
            }}
            QScrollBar:vertical {{
                border: none;
                background-color: #f8f9fa;
                width: {int(base_font_size * 0.8)}px;
                margin: 0;
            }}
            QScrollBar::handle:vertical {{
                background-color: #cbd5e0;
                border-radius: {int(base_font_size * 0.4)}px;
                min-height: {int(base_font_size * 1.5)}px;
            }}
            QScrollBar::handle:vertical:hover {{
                background-color: #a0aec0;
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                height: 0;
            }}
        """)
        
        # 设置日志处理器
        self.setup_logging()
        
        # 初始化分析线程
        self.analysis_thread = None

    def create_input_group(self, base_font_size, window_width, window_height):
        group = QGroupBox("输入参数")
        layout = QGridLayout()
        layout.setSpacing(int(window_height * 0.015))
        layout.setContentsMargins(
            int(window_width * 0.015),
            int(window_height * 0.015),
            int(window_width * 0.015),
            int(window_height * 0.015)
        )
        
        # 公会ID输入
        layout.addWidget(QLabel("公会ID:"), 0, 0)
        self.gid_edit = QLineEdit()
        self.gid_edit.setPlaceholderText("单个公会ID")
        self.gid_edit.textChanged.connect(self.on_input_changed)
        layout.addWidget(self.gid_edit, 0, 1)
        
        # 多公会ID输入
        layout.addWidget(QLabel("多公会ID:"), 1, 0)
        self.gids_edit = QLineEdit()
        self.gids_edit.setPlaceholderText("多个公会ID，用逗号分隔")
        self.gids_edit.textChanged.connect(self.on_input_changed)
        layout.addWidget(self.gids_edit, 1, 1)
        
        # 当前日期
        layout.addWidget(QLabel("当前日期:"), 2, 0)
        self.current_date_edit = QDateEdit()
        self.current_date_edit.setCalendarPopup(True)
        self.current_date_edit.setDisplayFormat("yyyyMMdd")
        self.current_date_edit.setDate(QDate.currentDate())
        layout.addWidget(self.current_date_edit, 2, 1)
        
        # 开始日期
        layout.addWidget(QLabel("开始日期:"), 3, 0)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDisplayFormat("yyyyMMdd")
        self.start_date_edit.setDate(QDate.currentDate())
        layout.addWidget(self.start_date_edit, 3, 1)
        
        # 结束日期
        layout.addWidget(QLabel("结束日期:"), 4, 0)
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDisplayFormat("yyyyMMdd")
        self.end_date_edit.setDate(QDate.currentDate())
        layout.addWidget(self.end_date_edit, 4, 1)
        
        # 输出目录
        layout.addWidget(QLabel("输出目录:"), 5, 0)
        output_layout = QHBoxLayout()
        output_layout.setSpacing(10)
        self.output_dir_edit = QLineEdit()
        self.output_dir_edit.setText("warpath_data")
        output_layout.addWidget(self.output_dir_edit)
        browse_btn = QPushButton("浏览")
        browse_btn.setMinimumWidth(80)
        browse_btn.clicked.connect(self.browse_output_dir)
        output_layout.addWidget(browse_btn)
        layout.addLayout(output_layout, 5, 1)
        
        # 最大并发数
        layout.addWidget(QLabel("最大并发数:"), 6, 0)
        self.max_concurrent_spin = QSpinBox()
        self.max_concurrent_spin.setRange(1, 20)
        self.max_concurrent_spin.setValue(10)
        layout.addWidget(self.max_concurrent_spin, 6, 1)
        
        # 最大重试次数
        layout.addWidget(QLabel("最大重试次数:"), 7, 0)
        self.max_retries_spin = QSpinBox()
        self.max_retries_spin.setRange(1, 10)
        self.max_retries_spin.setValue(3)
        layout.addWidget(self.max_retries_spin, 7, 1)
        
        # 重试延迟
        layout.addWidget(QLabel("重试延迟(秒):"), 8, 0)
        self.retry_delay_spin = QSpinBox()
        self.retry_delay_spin.setRange(1, 10)
        self.retry_delay_spin.setValue(2)
        layout.addWidget(self.retry_delay_spin, 8, 1)
        
        # 生成对比报告选项
        layout.addWidget(QLabel("生成对比报告:"), 9, 0)
        self.compare_checkbox = QCheckBox()
        self.compare_checkbox.setChecked(True)
        self.compare_checkbox.setEnabled(False)
        layout.addWidget(self.compare_checkbox, 9, 1)
        
        # 按钮区域
        button_layout = QHBoxLayout()
        button_layout.setSpacing(int(window_width * 0.01))
        self.start_btn = QPushButton("开始分析")
        self.start_btn.setMinimumWidth(int(window_width * 0.1))
        self.start_btn.clicked.connect(self.start_analysis)
        button_layout.addWidget(self.start_btn)
        
        self.clear_btn = QPushButton("清除日志")
        self.clear_btn.setMinimumWidth(int(window_width * 0.1))
        self.clear_btn.clicked.connect(self.clear_log)
        button_layout.addWidget(self.clear_btn)
        
        layout.addLayout(button_layout, 10, 0, 1, 2)
        
        group.setLayout(layout)
        return group
        
    def create_log_group(self, base_font_size, window_width, window_height):
        group = QGroupBox("运行日志")
        layout = QVBoxLayout()
        layout.setSpacing(int(window_height * 0.01))
        layout.setContentsMargins(
            int(window_width * 0.015),
            int(window_height * 0.015),
            int(window_width * 0.015),
            int(window_height * 0.015)
        )
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(int(window_height * 0.3))
        layout.addWidget(self.log_text)
        
        group.setLayout(layout)
        return group

    def create_progress_group(self, base_font_size, window_width, window_height):
        group = QGroupBox("进度")
        layout = QVBoxLayout()
        layout.setSpacing(int(window_height * 0.01))
        layout.setContentsMargins(
            int(window_width * 0.015),
            int(window_height * 0.015),
            int(window_width * 0.015),
            int(window_height * 0.015)
        )
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # 设置为循环模式
        self.progress_bar.hide()
        self.progress_bar.setMinimumHeight(int(base_font_size * 1.5))
        layout.addWidget(self.progress_bar)
        
        self.progress_label = QLabel("")
        self.progress_label.setStyleSheet(f"""
            QLabel {{
                color: #2c3e50;
                font-size: {base_font_size}px;
                font-weight: bold;
            }}
        """)
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
        
    def on_input_changed(self):
        gid = self.gid_edit.text().strip()
        gids = self.gids_edit.text().strip()
        
        if gid and gids:
            self.gids_edit.clear()
            gids = ""
        
        self.compare_checkbox.setEnabled(bool(gids))
        
        if not self.compare_checkbox.isEnabled():
            self.compare_checkbox.setChecked(False)

    def validate_inputs(self):
        try:
            # 验证公会ID
            gid = self.gid_edit.text().strip()
            gids = self.gids_edit.text().strip()
            
            if not gid and not gids:
                QMessageBox.warning(self, "错误", "请输入公会ID或多公会ID")
                return False
                
            if gid and gids:
                QMessageBox.warning(self, "错误", "不能同时输入单个公会ID和多公会ID")
                return False
                
            # 验证日期
            current_date = self.current_date_edit.date().toString("yyyyMMdd")
            start_date = self.start_date_edit.date().toString("yyyyMMdd")
            end_date = self.end_date_edit.date().toString("yyyyMMdd")
            
            if start_date > end_date:
                QMessageBox.warning(self, "错误", "开始日期不能晚于结束日期")
                return False
                
            # 验证多公会ID格式
            if gids:
                try:
                    gid_list = [int(gid.strip()) for gid in gids.split(',')]
                    if len(gid_list) < 2:
                        QMessageBox.warning(self, "错误", "多公会分析需要至少输入两个公会ID")
                        return False
                except ValueError:
                    QMessageBox.warning(self, "错误", "多公会ID格式错误，请使用数字并用逗号分隔")
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
            'current_date': self.current_date_edit.date().toString("yyyyMMdd"),
            'start_date': self.start_date_edit.date().toString("yyyyMMdd"),
            'end_date': self.end_date_edit.date().toString("yyyyMMdd"),
            'output_dir': self.output_dir_edit.text(),
            'max_concurrent': self.max_concurrent_spin.value(),
            'max_retries': self.max_retries_spin.value(),
            'retry_delay': self.retry_delay_spin.value(),
            'compare': self.compare_checkbox.isChecked()
        }
        
        # 根据输入选择分析模式
        if self.gid_edit.text().strip():
            params['gid'] = int(self.gid_edit.text())
            self.analysis_thread = SingleGuildAnalysisThread(params)
        else:
            params['gids'] = [int(gid.strip()) for gid in self.gids_edit.text().split(',')]
            self.analysis_thread = MultipleGuildsAnalysisThread(params)
        
        # 连接信号
        self.analysis_thread.finished.connect(self.analysis_finished)
        self.analysis_thread.error.connect(self.analysis_error)
        self.analysis_thread.progress.connect(self.update_progress)
        
        # 启动分析线程
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

class SingleGuildAnalysisThread(QThread):
    finished = pyqtSignal()
    error = pyqtSignal(str)
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

class MultipleGuildsAnalysisThread(QThread):
    finished = pyqtSignal()
    error = pyqtSignal(str)
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
            loop.run_until_complete(processor.run_multiple_guilds(
                self.params['gids'],
                self.params['current_date'],
                self.params['start_date'],
                self.params['end_date'],
                self.params['compare']
            ))
            
            loop.close()
            self.finished.emit()
        except Exception as e:
            self.error.emit(str(e))

def main():
    app = QApplication(sys.argv)
    window = WarpathDataGUI()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main() 