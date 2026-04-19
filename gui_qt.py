import sys
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                           QTextEdit, QFrame, QFileDialog, QMessageBox,
                           QGroupBox, QGridLayout, QSpinBox, QScrollArea,
                           QProgressBar, QCheckBox, QDesktopWidget, QDateEdit,
                           QRadioButton, QButtonGroup, QAbstractButton, QComboBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QSize, QDate
from PyQt5.QtGui import QFont, QPalette, QColor, QScreen
from main import WarpathDataProcessor


def kvk_server_kind_cn(kind: str) -> str:
    """ServerList 档位 key → 中文（legend→传奇，gold→黄金）。"""
    if not kind:
        return ""
    k = str(kind).strip()
    low = k.lower()
    if low.startswith("legend"):
        return "传奇" + k[len("legend") :]
    if low.startswith("gold"):
        return "黄金" + k[len("gold") :]
    return k


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


class KvkSeasonsFetchThread(QThread):
    """拉取 KvkList，按 end_day 从新到旧排序。"""

    finished_ok = pyqtSignal(list)
    error = pyqtSignal(str)

    def run(self):
        try:
            from Neibu import fetch_kvk_season_list

            rows = fetch_kvk_season_list()
            rows = [
                r
                for r in rows
                if isinstance(r, dict) and r.get("end_day") is not None
            ]
            rows.sort(key=lambda r: int(r["end_day"]), reverse=True)
            self.finished_ok.emit(rows)
        except Exception as e:
            self.error.emit(str(e))


class KvkServersFetchThread(QThread):
    """拉取某 end_day 的 ServerList。"""

    finished_ok = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, end_day: int):
        super().__init__()
        self._end_day = int(end_day)

    def run(self):
        try:
            from Neibu import fetch_kvk_server_list

            sm = fetch_kvk_server_list(self._end_day)
            self.finished_ok.emit(sm)
        except Exception as e:
            self.error.emit(str(e))


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
            QLineEdit, QSpinBox, QComboBox {{
                padding: {int(base_font_size * 0.6)}px;
                border: 2px solid #e9ecef;
                border-radius: {int(base_font_size * 0.5)}px;
                background-color: white;
                font-size: {base_font_size}px;
                min-height: {int(base_font_size * 2.5)}px;
            }}
            QLineEdit:focus, QSpinBox:focus, QComboBox:focus {{
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
        self._kvk_season_thread = None
        self._kvk_server_thread = None
        self._kvk_server_gen = 0
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
        self.kvk_mode_radio = QRadioButton("赛季战场分析")
        
        # 设置默认选中
        self.single_mode_radio.setChecked(True)
        self.mode_button_group = QButtonGroup(self)
        self.mode_button_group.setExclusive(True)
        for rb in (
            self.single_mode_radio,
            self.multiple_mode_radio,
            self.all_guilds_radio,
            self.kvk_mode_radio,
        ):
            self.mode_button_group.addButton(rb)
        self.mode_button_group.buttonClicked[QAbstractButton].connect(
            self._on_mode_button_clicked
        )
        
        # 添加到布局
        layout.addWidget(self.single_mode_radio)
        layout.addWidget(self.multiple_mode_radio)
        layout.addWidget(self.all_guilds_radio)
        layout.addWidget(self.kvk_mode_radio)
        layout.addStretch()
        
        group.setLayout(layout)
        return group

    def _on_mode_button_clicked(self, button: QAbstractButton):
        if button is self.single_mode_radio:
            self.on_mode_changed("single")
        elif button is self.multiple_mode_radio:
            self.on_mode_changed("multiple")
        elif button is self.all_guilds_radio:
            self.on_mode_changed("all_guilds")
        elif button is self.kvk_mode_radio:
            self.on_mode_changed("kvk")

    def create_input_group(self, base_font_size, window_width, window_height):
        """创建输入区域"""
        group = QGroupBox("输入参数")
        layout = QGridLayout()
        
        # 当前日期
        self.date_label = QLabel("当前日期:")
        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDisplayFormat("yyyyMMdd")
        layout.addWidget(self.date_label, 0, 0)
        layout.addWidget(self.date_edit, 0, 1)
        
        # 开始日期（公会分析）
        self.start_label = QLabel("开始日期:")
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDisplayFormat("yyyyMMdd")
        layout.addWidget(self.start_label, 1, 0)
        layout.addWidget(self.start_date_edit, 1, 1)
        
        # 结束日期（公会分析）
        self.end_label = QLabel("结束日期:")
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDisplayFormat("yyyyMMdd")
        layout.addWidget(self.end_label, 2, 0)
        layout.addWidget(self.end_date_edit, 2, 1)
        
        # 公会ID输入
        self.gid_label = QLabel("公会ID:")
        self.gid_input = ModernLineEdit()
        self.gid_input.setPlaceholderText("输入单个公会ID")
        layout.addWidget(self.gid_label, 3, 0)
        layout.addWidget(self.gid_input, 3, 1)
        
        # 多公会ID输入
        self.gids_label = QLabel("多个公会ID:")
        self.gids_input = ModernLineEdit()
        self.gids_input.setPlaceholderText("输入多个公会ID，用逗号分隔")
        self.gids_input.setVisible(False)
        layout.addWidget(self.gids_label, 4, 0)
        layout.addWidget(self.gids_input, 4, 1)

        # 赛季战场（KvkList / ServerList / ServerDetail）
        self.kvk_season_label = QLabel("赛季:")
        self.kvk_season_combo = QComboBox()
        self.kvk_season_combo.setMinimumHeight(35)
        self.kvk_refresh_seasons_btn = ModernButton("刷新赛季")
        self.kvk_refresh_seasons_btn.clicked.connect(self._start_kvk_season_load)
        self.kvk_server_label = QLabel("场次:")
        self.kvk_server_combo = QComboBox()
        self.kvk_server_combo.setMinimumHeight(35)
        self.kvk_all_servers_cb = QCheckBox("导出该结束日下全部场次（所有 server，多 sheet）")
        self.kvk_member_avg_cb = QCheckBox(
            "计算联盟成员场均（按 GID 拉 guild_member + pid_detail，较慢）"
        )
        self.kvk_member_avg_cb.setChecked(True)
        for w in (
            self.kvk_season_label,
            self.kvk_season_combo,
            self.kvk_refresh_seasons_btn,
            self.kvk_server_label,
            self.kvk_server_combo,
            self.kvk_all_servers_cb,
            self.kvk_member_avg_cb,
        ):
            w.setVisible(False)
        layout.addWidget(self.kvk_season_label, 5, 0)
        layout.addWidget(self.kvk_season_combo, 5, 1)
        layout.addWidget(self.kvk_refresh_seasons_btn, 5, 2)
        layout.addWidget(self.kvk_server_label, 6, 0)
        layout.addWidget(self.kvk_server_combo, 6, 1, 1, 2)
        layout.addWidget(self.kvk_all_servers_cb, 7, 0, 1, 3)
        self.kvk_all_servers_cb.toggled.connect(self._on_kvk_all_toggled)
        self.kvk_season_combo.currentIndexChanged.connect(self._on_kvk_season_changed)
        layout.addWidget(self.kvk_member_avg_cb, 8, 0, 1, 3)
        
        # 输出目录选择
        output_label = QLabel("输出目录:")
        self.output_input = ModernLineEdit()
        self.output_input.setPlaceholderText("选择输出目录")
        self.browse_button = ModernButton("浏览")
        self.browse_button.clicked.connect(self.browse_output_dir)
        layout.addWidget(output_label, 9, 0)
        layout.addWidget(self.output_input, 9, 1)
        layout.addWidget(self.browse_button, 9, 2)
        
        # 开始按钮
        self.start_button = ModernButton("开始分析")
        self.start_button.clicked.connect(self.start_analysis)
        layout.addWidget(self.start_button, 10, 0, 1, 3)
        
        group.setLayout(layout)
        return group

    def on_mode_changed(self, mode):
        """处理模式切换"""
        self.current_mode = mode
        # 单个/多公会分析都显示日期区间
        show_dates = (mode == "single" or mode == "multiple")
        show_guild_date_row = mode != "kvk"
        for w in (self.date_label, self.date_edit):
            w.setVisible(show_guild_date_row)
        self.start_label.setVisible(show_dates)
        self.start_date_edit.setVisible(show_dates)
        self.end_label.setVisible(show_dates)
        self.end_date_edit.setVisible(show_dates)
        self.gid_label.setVisible(mode == "single")
        self.gid_input.setVisible(mode == "single")
        self.gids_label.setVisible(mode == "multiple")
        self.gids_input.setVisible(mode == "multiple")
        kvk_on = mode == "kvk"
        for w in (
            self.kvk_season_label,
            self.kvk_season_combo,
            self.kvk_refresh_seasons_btn,
            self.kvk_server_label,
            self.kvk_server_combo,
            self.kvk_all_servers_cb,
            self.kvk_member_avg_cb,
        ):
            w.setVisible(kvk_on)
        if kvk_on:
            self._on_kvk_all_toggled(self.kvk_all_servers_cb.isChecked())
            self._start_kvk_season_load()
        # 更新输入框提示文本
        if mode == "single":
            self.gid_input.setPlaceholderText("输入单个公会ID")
        elif mode == "multiple":
            self.gids_input.setPlaceholderText("输入多个公会ID，用逗号分隔")
        # 更新开始按钮文本
        if mode == "all_guilds":
            self.start_button.setText("获取全服联盟数据")
        elif mode == "kvk":
            self.start_button.setText("导出赛季战场 Excel")
        else:
            self.start_button.setText("开始分析")

    def _on_kvk_all_toggled(self, checked: bool):
        """全场次时不需要选择单个场次。"""
        self.kvk_server_label.setEnabled(not checked)
        if checked:
            self.kvk_server_combo.setEnabled(False)
        else:
            d = self.kvk_server_combo.currentData(Qt.UserRole)
            ok = d is not None and str(d).strip() != ""
            self.kvk_server_combo.setEnabled(ok)

    def _start_kvk_season_load(self):
        """请求 KvkList，填充赛季下拉（最新 end_day 在前）。"""
        if self._kvk_season_thread and self._kvk_season_thread.isRunning():
            self.progress_label.setText("赛季列表正在加载中…")
            return
        self.kvk_season_combo.blockSignals(True)
        self.kvk_season_combo.clear()
        self.kvk_season_combo.addItem("正在加载赛季列表…", None)
        self.kvk_season_combo.setEnabled(False)
        self.kvk_refresh_seasons_btn.setEnabled(False)
        self.kvk_season_combo.blockSignals(False)
        self.progress_label.setText("正在拉取赛季列表（KvkList）…")
        self._kvk_season_thread = KvkSeasonsFetchThread()
        self._kvk_season_thread.finished_ok.connect(self._on_kvk_seasons_loaded)
        self._kvk_season_thread.error.connect(self._on_kvk_seasons_error)
        self._kvk_season_thread.start()

    def _on_kvk_seasons_error(self, msg: str):
        logging.error("KvkList 加载失败: %s", msg)
        self.kvk_season_combo.blockSignals(True)
        self.kvk_season_combo.clear()
        self.kvk_season_combo.addItem("（加载失败，请点击「刷新赛季」）", None)
        self.kvk_season_combo.setEnabled(True)
        self.kvk_refresh_seasons_btn.setEnabled(True)
        self.kvk_season_combo.blockSignals(False)
        self.kvk_server_combo.blockSignals(True)
        self.kvk_server_combo.clear()
        self.kvk_server_combo.addItem("（请先成功加载赛季）", None)
        self.kvk_server_combo.setEnabled(False)
        self.kvk_server_combo.blockSignals(False)
        self.progress_label.setText("")
        QMessageBox.warning(self, "赛季列表", f"无法获取赛季数据：\n{msg}")

    def _on_kvk_seasons_loaded(self, rows):
        if self.current_mode != "kvk":
            return
        self.kvk_season_combo.blockSignals(True)
        self.kvk_season_combo.clear()
        self.kvk_refresh_seasons_btn.setEnabled(True)
        self.kvk_season_combo.setEnabled(True)
        if not rows:
            self.kvk_season_combo.addItem("（无赛季数据）", None)
            self.kvk_season_combo.blockSignals(False)
            self.progress_label.setText("")
            return
        for row in rows:
            try:
                sd = int(row.get("start_day") or 0)
                ed = int(row.get("end_day") or 0)
                name = str(row.get("kvkname") or "").strip() or "（未命名）"
                label = f"{name}    {sd} → {ed}"
            except (TypeError, ValueError):
                continue
            self.kvk_season_combo.addItem(label, row)
        if self.kvk_season_combo.count() == 0:
            self.kvk_season_combo.addItem("（无有效赛季）", None)
        self.kvk_season_combo.blockSignals(False)
        self.kvk_season_combo.setCurrentIndex(0)
        self.progress_label.setText("")

    def _on_kvk_season_changed(self, _index: int = 0):
        if self.current_mode != "kvk":
            return
        row = self.kvk_season_combo.currentData(Qt.UserRole)
        if not row or not isinstance(row, dict):
            self.kvk_server_combo.blockSignals(True)
            self.kvk_server_combo.clear()
            self.kvk_server_combo.addItem("请先选择有效赛季", None)
            self.kvk_server_combo.blockSignals(False)
            self.kvk_server_combo.setEnabled(False)
            return
        try:
            end_day = int(row["end_day"])
        except (TypeError, ValueError, KeyError):
            return
        self._start_kvk_server_load(end_day)

    def _start_kvk_server_load(self, end_day: int):
        self._kvk_server_gen += 1
        gen = self._kvk_server_gen
        self.kvk_server_combo.blockSignals(True)
        self.kvk_server_combo.clear()
        self.kvk_server_combo.addItem("正在加载场次列表…", None)
        self.kvk_server_combo.setEnabled(False)
        self.kvk_server_combo.blockSignals(False)
        self.progress_label.setText("正在拉取场次（ServerList）…")
        self._kvk_server_thread = KvkServersFetchThread(end_day)
        self._kvk_server_thread.finished_ok.connect(
            lambda sm, g=gen: self._on_kvk_servers_loaded(sm, g)
        )
        self._kvk_server_thread.error.connect(
            lambda err, g=gen: self._on_kvk_servers_error(err, g)
        )
        self._kvk_server_thread.start()

    def _on_kvk_servers_error(self, msg: str, gen: int):
        if gen != self._kvk_server_gen:
            return
        if self.current_mode != "kvk":
            return
        logging.error("ServerList 加载失败: %s", msg)
        self.kvk_server_combo.blockSignals(True)
        self.kvk_server_combo.clear()
        self.kvk_server_combo.addItem("（场次加载失败，可换赛季重试）", None)
        self.kvk_server_combo.setEnabled(True)
        self.kvk_server_combo.blockSignals(False)
        self.progress_label.setText("")
        QMessageBox.warning(self, "场次列表", f"无法获取场次数据：\n{msg}")

    def _on_kvk_servers_loaded(self, sm: dict, gen: int):
        if gen != self._kvk_server_gen:
            return
        if self.current_mode != "kvk":
            return
        self.kvk_server_combo.blockSignals(True)
        self.kvk_server_combo.clear()
        items = []
        for kind in sorted(sm.keys()):
            for srv in sm.get(kind) or []:
                items.append((kind, str(srv)))
        for kind, srv in items:
            label_kind = kvk_server_kind_cn(kind)
            self.kvk_server_combo.addItem(f"{label_kind}  ·  {srv}", srv)
        if not items:
            self.kvk_server_combo.addItem("（该赛季暂无场次）", None)
        self.kvk_server_combo.blockSignals(False)
        self.kvk_server_combo.setEnabled(True)
        if items:
            self.kvk_server_combo.setCurrentIndex(0)
        self.progress_label.setText("")
        self._on_kvk_all_toggled(self.kvk_all_servers_cb.isChecked())

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
        elif self.current_mode == "kvk":
            row = self.kvk_season_combo.currentData(Qt.UserRole)
            end_day = int(row["end_day"])
            out_dir = Path(self.output_input.text())
            out_dir.mkdir(parents=True, exist_ok=True)
            all_srv = self.kvk_all_servers_cb.isChecked()
            if all_srv:
                output_path = str(out_dir / f"kvk_all_{end_day}.xlsx")
            else:
                server = str(self.kvk_server_combo.currentData(Qt.UserRole) or "").strip()
                safe = "".join(c for c in server if c.isalnum() or c in ("-", "_")) or "server"
                output_path = str(out_dir / f"kvk_{end_day}_{safe}.xlsx")
            self.analysis_thread = KvkAnalysisThread(
                {
                    "end_day": end_day,
                    "server": str(self.kvk_server_combo.currentData(Qt.UserRole) or "").strip(),
                    "all_servers": all_srv,
                    "output_path": output_path,
                    "member_avg": self.kvk_member_avg_cb.isChecked(),
                    "max_concurrent": 10,
                }
            )
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

        elif self.current_mode == "kvk":
            row = self.kvk_season_combo.currentData(Qt.UserRole)
            if not row or not isinstance(row, dict):
                QMessageBox.warning(
                    self, "警告", "请等待赛季列表加载完成，或点击「刷新赛季」重试"
                )
                return False
            try:
                int(row["end_day"])
            except (TypeError, ValueError, KeyError):
                QMessageBox.warning(self, "警告", "当前选择的赛季数据无效")
                return False
            if not self.kvk_all_servers_cb.isChecked():
                srv = self.kvk_server_combo.currentData(Qt.UserRole)
                if srv is None or str(srv).strip() == "":
                    QMessageBox.warning(
                        self, "警告", "请在下拉框中选择场次，或勾选「导出全部场次」"
                    )
                    return False

        return True

    def update_progress(self, message):
        self.progress_label.setText(message)
        
    def analysis_finished(self):
        # 重新启用按钮
        self.start_button.setEnabled(True)
        self.progress_bar.hide()
        self.progress_label.setText("分析完成！")
        msg = "数据分析完成！"
        if self.current_mode == "kvk" and self.analysis_thread:
            outp = getattr(self.analysis_thread, "output_path", None)
            if outp:
                msg = f"赛季战场分析已导出:\n{outp}"
        QMessageBox.information(self, "成功", msg)
        
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
        for t in (self._kvk_season_thread, self._kvk_server_thread):
            if t and t.isRunning():
                t.wait(8000)
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


class KvkAnalysisThread(QThread):
    """后台调用 Neibu 赛季战场 API 导出 Excel。"""

    finished = pyqtSignal()
    error = pyqtSignal(str)
    progress = pyqtSignal(str)

    def __init__(self, params):
        super().__init__()
        self.params = params
        self.output_path = params["output_path"]
        self.is_running = True

    def stop(self):
        self.is_running = False

    def run(self):
        try:
            from Neibu import (
                export_kvk_all_servers_excel,
                export_kvk_battlefield_excel,
            )

            if self.params["all_servers"]:
                self.progress.emit("正在拉取 ServerList 与各场 ServerDetail …")
                export_kvk_all_servers_excel(
                    self.params["end_day"], self.params["output_path"]
                )
            else:
                self.progress.emit(
                    f"正在拉取 ServerDetail（end_day={self.params['end_day']}, "
                    f"server={self.params['server']}）…"
                )
                export_kvk_battlefield_excel(
                    self.params["end_day"],
                    self.params["server"],
                    self.params["output_path"],
                    member_avg=self.params.get("member_avg", True),
                    max_concurrent=self.params.get("max_concurrent", 8),
                )
            self.finished.emit()
        except Exception as e:
            logging.exception("赛季战场导出失败")
            self.error.emit(str(e))


def main():
    app = QApplication(sys.argv)
    window = WarpathDataGUI()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main() 