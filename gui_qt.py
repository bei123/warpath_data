import sys
import asyncio
from datetime import datetime
from pathlib import Path
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                           QTextEdit, QFrame, QFileDialog, QMessageBox,
                           QGroupBox, QGridLayout, QSpinBox, QScrollArea,
                           QProgressBar, QCheckBox, QDesktopWidget, QDateEdit,
                           QRadioButton)
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
        
        # 创建模式选择区域
        mode_group = self.create_mode_group(base_font_size, window_width, window_height)
        main_layout.addWidget(mode_group)
        
        # 创建输入区域
        self.input_group = self.create_input_group(base_font_size, window_width, window_height)
        main_layout.addWidget(self.input_group)
        
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
                padding: {int(base_font_size * 0.6)}px;
                background-color: white;
                font-size: {base_font_size}px;
            }}
            QProgressBar {{
                border: 2px solid #e9ecef;
                border-radius: {int(base_font_size * 0.5)}px;
                text-align: center;
                background-color: white;
                font-size: {base_font_size}px;
            }}
            QProgressBar::chunk {{
                background-color: #3498db;
                border-radius: {int(base_font_size * 0.5)}px;
            }}
            QRadioButton {{
                font-size: {base_font_size}px;
                color: #2c3e50;
            }}
            QRadioButton::indicator {{
                width: {int(base_font_size * 1.2)}px;
                height: {int(base_font_size * 1.2)}px;
            }}
        """)
        
        # 初始化变量
        self.analysis_thread = None
        self.current_mode = "single"  # 默认模式
        self.setup_logging()
        
        # 设置默认日期为今天
        today = datetime.now()
        self.date_edit.setDate(QDate(today.year, today.month, today.day))

    def create_mode_group(self, base_font_size, window_width, window_height):
        """创建模式选择组"""
        group = QGroupBox("选择分析模式")
        layout = QHBoxLayout()
        
        # 创建单选按钮
        self.single_mode_radio = QRadioButton("单个公会分析")
        self.multiple_mode_radio = QRadioButton("多个公会分析")
        self.all_guilds_radio = QRadioButton("全服联盟分析")
        
        # 设置默认选中
        self.single_mode_radio.setChecked(True)
        
        # 连接信号
        self.single_mode_radio.toggled.connect(lambda: self.on_mode_changed("single"))
        self.multiple_mode_radio.toggled.connect(lambda: self.on_mode_changed("multiple"))
        self.all_guilds_radio.toggled.connect(lambda: self.on_mode_changed("all_guilds"))
        
        # 添加到布局
        layout.addWidget(self.single_mode_radio)
        layout.addWidget(self.multiple_mode_radio)
        layout.addWidget(self.all_guilds_radio)
        layout.addStretch()
        
        group.setLayout(layout)
        return group

    def create_input_group(self, base_font_size, window_width, window_height):
        """创建输入区域"""
        group = QGroupBox("输入参数")
        layout = QGridLayout()
        
        # 当前日期
        date_label = QLabel("当前日期:")
        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDisplayFormat("yyyyMMdd")
        layout.addWidget(date_label, 0, 0)
        layout.addWidget(self.date_edit, 0, 1)
        
        # 开始日期（仅单个公会分析显示）
        start_label = QLabel("开始日期:")
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDisplayFormat("yyyyMMdd")
        layout.addWidget(start_label, 1, 0)
        layout.addWidget(self.start_date_edit, 1, 1)
        
        # 结束日期（仅单个公会分析显示）
        end_label = QLabel("结束日期:")
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDisplayFormat("yyyyMMdd")
        layout.addWidget(end_label, 2, 0)
        layout.addWidget(self.end_date_edit, 2, 1)
        
        # 公会ID输入
        gid_label = QLabel("公会ID:")
        self.gid_input = ModernLineEdit()
        self.gid_input.setPlaceholderText("输入单个公会ID")
        layout.addWidget(gid_label, 3, 0)
        layout.addWidget(self.gid_input, 3, 1)
        
        # 多公会ID输入
        gids_label = QLabel("多个公会ID:")
        self.gids_input = ModernLineEdit()
        self.gids_input.setPlaceholderText("输入多个公会ID，用逗号分隔")
        self.gids_input.setVisible(False)
        layout.addWidget(gids_label, 4, 0)
        layout.addWidget(self.gids_input, 4, 1)
        
        # 输出目录选择
        output_label = QLabel("输出目录:")
        self.output_input = ModernLineEdit()
        self.output_input.setPlaceholderText("选择输出目录")
        self.browse_button = ModernButton("浏览")
        self.browse_button.clicked.connect(self.browse_output_dir)
        layout.addWidget(output_label, 5, 0)
        layout.addWidget(self.output_input, 5, 1)
        layout.addWidget(self.browse_button, 5, 2)
        
        # 开始按钮
        self.start_button = ModernButton("开始分析")
        self.start_button.clicked.connect(self.start_analysis)
        layout.addWidget(self.start_button, 6, 0, 1, 3)
        
        group.setLayout(layout)
        return group

    def on_mode_changed(self, mode):
        """处理模式切换"""
        self.current_mode = mode
        # 单个/多公会分析都显示日期区间
        show_dates = (mode == "single" or mode == "multiple")
        self.start_date_edit.setVisible(show_dates)
        self.end_date_edit.setVisible(show_dates)
        self.gid_input.setVisible(mode == "single")
        self.gids_input.setVisible(mode == "multiple")
        # 更新输入框提示文本
        if mode == "single":
            self.gid_input.setPlaceholderText("输入单个公会ID")
        elif mode == "multiple":
            self.gids_input.setPlaceholderText("输入多个公会ID，用逗号分隔")
        # 更新开始按钮文本
        if mode == "all_guilds":
            self.start_button.setText("获取全服联盟数据")
        else:
            self.start_button.setText("开始分析")

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
            self.output_input.setText(directory)
            
    def clear_log(self):
        self.log_text.clear()
        
    def start_analysis(self):
        """开始分析"""
        if not self.validate_inputs():
            return
            
        # 禁用开始按钮
        self.start_button.setEnabled(False)
        
        # 获取参数
        params = {
            'output_dir': self.output_input.text(),
            'max_concurrent': 10,
            'max_retries': 3,
            'retry_delay': 2,
            'current_date': self.date_edit.date().toString("yyyyMMdd")
        }
        
        # 根据模式创建不同的线程
        if self.current_mode == "single":
            params['gid'] = int(self.gid_input.text())
            params['start_date'] = self.start_date_edit.date().toString("yyyyMMdd")
            params['end_date'] = self.end_date_edit.date().toString("yyyyMMdd")
            self.analysis_thread = SingleGuildAnalysisThread(params)
        elif self.current_mode == "multiple":
            params['gids'] = [int(gid.strip()) for gid in self.gids_input.text().split(',')]
            params['start_date'] = self.start_date_edit.date().toString("yyyyMMdd")
            params['end_date'] = self.end_date_edit.date().toString("yyyyMMdd")
            params['compare'] = False  # 默认不生成对比报告，如需对比可改为True
            self.analysis_thread = MultipleGuildsAnalysisThread(params)
        else:  # all_guilds
            params.update({
                'wid': 0.1,  # 固定为0.1
                'ccid': 0,   # 固定为0
                'rank': 'power',  # 固定为power
                'is_benfu': 1,    # 固定为1
                'is_quanfu': 1    # 固定为1
            })
            self.analysis_thread = AllGuildsAnalysisThread(params)
        
        # 连接信号
        self.analysis_thread.finished.connect(self.analysis_finished)
        self.analysis_thread.error.connect(self.analysis_error)
        self.analysis_thread.progress.connect(self.update_progress)
        
        # 启动线程
        self.analysis_thread.start()

    def validate_inputs(self):
        """验证输入"""
        if not self.output_input.text():
            QMessageBox.warning(self, "警告", "请选择输出目录")
            return False
            
        if self.current_mode == "single":
            if not self.gid_input.text():
                QMessageBox.warning(self, "警告", "请输入公会ID")
                return False
            try:
                int(self.gid_input.text())
            except ValueError:
                QMessageBox.warning(self, "警告", "公会ID必须是数字")
                return False
                
        elif self.current_mode == "multiple":
            if not self.gids_input.text():
                QMessageBox.warning(self, "警告", "请输入公会ID列表")
                return False
            try:
                [int(gid.strip()) for gid in self.gids_input.text().split(',')]
            except ValueError:
                QMessageBox.warning(self, "警告", "公会ID列表格式不正确")
                return False
                
        return True

    def update_progress(self, message):
        self.progress_label.setText(message)
        
    def analysis_finished(self):
        # 重新启用按钮
        self.start_button.setEnabled(True)
        self.progress_bar.hide()
        self.progress_label.setText("分析完成！")
        QMessageBox.information(self, "成功", "数据分析完成！")
        
    def analysis_error(self, error_msg):
        # 重新启用按钮
        self.start_button.setEnabled(True)
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

class AllGuildsAnalysisThread(QThread):
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
            loop.run_until_complete(processor.collect_all_guilds_data(
                current_date=self.params['current_date'],
                wid=self.params['wid'],
                ccid=self.params['ccid'],
                rank=self.params['rank'],
                is_benfu=self.params['is_benfu'],
                is_quanfu=self.params['is_quanfu']
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