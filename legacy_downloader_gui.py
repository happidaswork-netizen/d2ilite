# -*- coding: utf-8 -*-
"""图形用户界面"""

import os
import json
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import re

from excel_reader import DataReader, preview_raw_table
from downloader import ImageDownloader, DownloadStatus
from config import Config


class TablePreviewDialog(tk.Toplevel):
    """表格预览对话框 - 用于预览原始数据并选择列"""
    
    def __init__(self, parent, filepath):
        super().__init__(parent)
        
        self.title("预览表格并选择列")
        self.geometry("1000x600")
        self.minsize(800, 500)
        
        self.filepath = filepath
        self.result = None  # 存储用户选择的结果
        
        # 设置为模态窗口
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        self._load_data()
        
        # 等待窗口关闭
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
    
    def _create_widgets(self):
        """创建界面组件"""
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # ===== 提示信息 =====
        tip_label = ttk.Label(
            main_frame, 
            text="💡 提示: 点击列标题可以选择该列 | 选中行会高亮显示",
            font=('微软雅黑', 10)
        )
        tip_label.pack(pady=(0, 10))
        
        # ===== 列选择区域 =====
        select_frame = ttk.LabelFrame(main_frame, text="列选择 (点击表头或在此输入)", padding="10")
        select_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 第一行：姓名列和链接列
        row1 = ttk.Frame(select_frame)
        row1.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(row1, text="姓名列:", width=8).pack(side=tk.LEFT)
        self.name_col_var = tk.StringVar(value="")
        self.name_col_entry = ttk.Entry(row1, textvariable=self.name_col_var, width=6)
        self.name_col_entry.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Label(row1, text="图片列:", width=8).pack(side=tk.LEFT)
        self.url_col_var = tk.StringVar(value="")
        self.url_col_entry = ttk.Entry(row1, textvariable=self.url_col_var, width=6)
        self.url_col_entry.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Label(row1, text="来源列:", width=8).pack(side=tk.LEFT)
        self.source_col_var = tk.StringVar(value="")
        self.source_col_entry = ttk.Entry(row1, textvariable=self.source_col_var, width=6)
        self.source_col_entry.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Label(row1, text="起始行:", width=8).pack(side=tk.LEFT)
        self.start_row_var = tk.StringVar(value="2")
        ttk.Entry(row1, textvariable=self.start_row_var, width=4).pack(side=tk.LEFT)
        
        # 第二行：说明列（多列）
        row2 = ttk.Frame(select_frame)
        row2.pack(fill=tk.X)
        
        ttk.Label(row2, text="说明列:", width=8).pack(side=tk.LEFT)
        self.intro_cols_var = tk.StringVar(value="")
        self.intro_cols_entry = ttk.Entry(row2, textvariable=self.intro_cols_var, width=20)
        self.intro_cols_entry.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Label(row2, text="(多列用逗号分隔)").pack(side=tk.LEFT)
        
        # 选择模式切换
        self.select_mode = tk.StringVar(value="name")
        mode_frame = ttk.Frame(row2)
        mode_frame.pack(side=tk.RIGHT)
        ttk.Label(mode_frame, text="点击表头:").pack(side=tk.LEFT, padx=(0, 5))
        ttk.Radiobutton(mode_frame, text="姓名", variable=self.select_mode, value="name").pack(side=tk.LEFT)
        ttk.Radiobutton(mode_frame, text="图片", variable=self.select_mode, value="url").pack(side=tk.LEFT)
        ttk.Radiobutton(mode_frame, text="来源", variable=self.select_mode, value="source").pack(side=tk.LEFT)
        ttk.Radiobutton(mode_frame, text="说明+", variable=self.select_mode, value="intro").pack(side=tk.LEFT)
        
        # ===== 表格预览区域 =====
        table_frame = ttk.LabelFrame(main_frame, text="表格预览", padding="5")
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # 创建带滚动条的Treeview
        tree_container = ttk.Frame(table_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)
        
        # 水平滚动条
        h_scroll = ttk.Scrollbar(tree_container, orient=tk.HORIZONTAL)
        h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        
        # 垂直滚动条
        v_scroll = ttk.Scrollbar(tree_container, orient=tk.VERTICAL)
        v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.preview_tree = ttk.Treeview(
            tree_container, 
            show='headings',
            xscrollcommand=h_scroll.set,
            yscrollcommand=v_scroll.set
        )
        self.preview_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        h_scroll.config(command=self.preview_tree.xview)
        v_scroll.config(command=self.preview_tree.yview)
        
        # 状态信息
        self.status_label = ttk.Label(main_frame, text="加载中...")
        self.status_label.pack(pady=(0, 10))
        
        # ===== 按钮区域 =====
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X)
        
        ttk.Button(btn_frame, text="清除说明列", command=self._clear_intro_cols).pack(side=tk.LEFT)
        
        ttk.Button(btn_frame, text="取消", command=self._on_cancel).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(btn_frame, text="确定", command=self._on_confirm).pack(side=tk.RIGHT)
    
        # 绑定输入框点击事件
        self.name_col_entry.bind('<Button-1>', lambda e: self._activate_input(self.name_col_entry, 'name'))
        self.url_col_entry.bind('<Button-1>', lambda e: self._activate_input(self.url_col_entry, 'url'))
        self.source_col_entry.bind('<Button-1>', lambda e: self._activate_input(self.source_col_entry, 'source'))
        self.intro_cols_entry.bind('<Button-1>', lambda e: self._activate_input(self.intro_cols_entry, 'intro'))
        
        # 初始高亮
        self._highlight_active_input('name')

    def _activate_input(self, entry_widget, mode):
        """激活输入框：设置模式并高亮"""
        self.select_mode.set(mode)
        self._highlight_active_input(mode)
        # 阻止默认聚焦（可选，防止光标闪烁影响体验，如果不希望手动输入的话。这里保留手动输入能力）
        # return "break" 
    
    def _highlight_active_input(self, mode):
        """高亮当前活动的输入框"""
        # 重置所有背景
        default_bg = 'white'
        active_bg = '#e6f3ff'  # 浅蓝色
        
        self.name_col_entry.config(background=active_bg if mode == 'name' else default_bg)
        self.url_col_entry.config(background=active_bg if mode == 'url' else default_bg)
        self.source_col_entry.config(background=active_bg if mode == 'source' else default_bg)
        self.intro_cols_entry.config(background=active_bg if mode == 'intro' else default_bg)

    def _load_data(self):
        """加载表格数据"""
        try:
            preview_data = preview_raw_table(self.filepath, max_rows=100)
            
            headers = preview_data['headers']
            data = preview_data['data']
            total_rows = preview_data['total_rows']
            total_cols = preview_data['total_cols']
            
            # 配置列 (添加行号列)
            columns = ['行号'] + headers
            self.preview_tree['columns'] = columns
            
            # 设置列标题和宽度
            self.preview_tree.heading('行号', text='行号')
            self.preview_tree.column('行号', width=50, anchor=tk.CENTER)
            
            # 自动识别列名
            self._auto_detect_columns(headers)
            
            for col in headers:
                self.preview_tree.heading(col, text=col, command=lambda c=col: self._on_header_click(c))
                self.preview_tree.column(col, width=100, minwidth=60)
            
            # 添加数据
            for i, row in enumerate(data, start=1):
                # 截断过长的单元格值
                display_row = [str(v)[:50] + '...' if len(str(v)) > 50 else str(v) for v in row]
                self.preview_tree.insert('', tk.END, values=[i] + display_row)
            
            # 更新状态
            if len(data) < total_rows:
                self.status_label.config(text=f"共 {total_rows} 行 × {total_cols} 列 (当前显示前 {len(data)} 行)")
            else:
                self.status_label.config(text=f"共 {total_rows} 行 × {total_cols} 列")
            
            self.headers = headers
            
        except Exception as e:
            messagebox.showerror("错误", f"加载表格失败: {str(e)}")
            self.destroy()
    
    def _auto_detect_columns(self, headers):
        """自动识别列"""
        # 关键词定义 (均为小写)
        patterns = {
            'name': ['name', '姓名', 'title', '名称', 'full name', 'user', 'username', 'lawyer name'],
            'url': ['url', 'link', 'img', 'image', 'pic', 'photo', '图片', '链接', 'src', 'href'],
            'source': ['source', 'origin', 'ref', 'from', '来源', '出处'],
            'intro': ['intro', 'desc', 'description', 'bio', 'about', '简介', '描述', 'practice area', 'education', 'bar admission']
        }
        
        intro_cols = []
        
        for col_name in headers:
            lower_name = str(col_name).lower()
            
            # 姓名
            if not self.name_col_var.get() and any(p in lower_name for p in patterns['name']):
                self.name_col_var.set(col_name)
                continue
                
            # 图片
            if not self.url_col_var.get() and any(p in lower_name for p in patterns['url']):
                self.url_col_var.set(col_name)
                continue
            
            # 来源
            if not self.source_col_var.get() and any(p in lower_name for p in patterns['source']):
                self.source_col_var.set(col_name)
                continue
                
            # 说明 (可多选)
            if any(p in lower_name for p in patterns['intro']) or 'text' in lower_name:
                intro_cols.append(col_name)
        
        if intro_cols:
            self.intro_cols_var.set(",".join(intro_cols))

    def _on_header_click(self, col):
        """点击列标题选择列"""
        mode = self.select_mode.get()
        
        if mode == "name":
            self.name_col_var.set(col)
        elif mode == "url":
            self.url_col_var.set(col)
        elif mode == "source":
            self.source_col_var.set(col)
        elif mode == "intro":
            # 追加模式
            current = self.intro_cols_var.get().strip()
            if current:
                # 检查是否已存在
                cols = [c.strip() for c in current.split(',')]
                if col not in cols:
                    self.intro_cols_var.set(current + ',' + col)
            else:
                self.intro_cols_var.set(col)
    
    def _clear_intro_cols(self):
        """清除说明列"""
        self.intro_cols_var.set("")
    
    def _on_confirm(self):
        """确定按钮"""
        name_col = self.name_col_var.get().strip()
        url_col = self.url_col_var.get().strip()
        intro_cols = self.intro_cols_var.get().strip()
        source_col = self.source_col_var.get().strip()
        start_row = self.start_row_var.get().strip()
        
        # 验证必填项
        if not name_col:
            messagebox.showwarning("提示", "请选择姓名列")
            return
        if not url_col:
            messagebox.showwarning("提示", "请选择图片列")
            return
        
        try:
            start_row_num = int(start_row)
            if start_row_num < 1:
                raise ValueError()
        except ValueError:
            messagebox.showwarning("提示", "起始行必须是正整数")
            return
        
        self.result = {
            'name_col': name_col,
            'url_col': url_col,
            'intro_cols': intro_cols if intro_cols else '',
            'source_col': source_col if source_col else '',
            'start_row': start_row_num
        }
        self.destroy()
    
    def _on_cancel(self):
        """取消按钮"""
        self.result = None
        self.destroy()


class Application(tk.Tk):
    """主应用程序"""
    
    def __init__(self):
        super().__init__()
        
        self.title("Excel图片下载器")
        self.geometry("750x700")  # 增大窗口
        self.minsize(700, 600)
        
        # 配置
        self.config = Config()
        
        # 数据
        self.data = []
        self.downloader = None
        self.download_thread = None
        self.downloaded_files = {}  # 存储 name -> filepath 映射
        
        # 批量下载控制
        self.current_batch_start = 0  # 当前批次起始位置
        self.auto_continue = False     # 是否自动继续
        self.rest_timer = None         # 休息计时器
        self.is_resting = False        # 是否正在休息
        self.total_success = 0         # 总成功数
        self.total_fail = 0            # 总失败数
        self.session_file = None       # 会话文件路径
        self.downloaded_urls = set()   # 已下载的URL集合
        
        # 创建界面
        self._create_widgets()
        self._bind_events()
    
    def _create_widgets(self):
        """创建界面组件"""
        # 主框架
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # ===== 文件选择区域 =====
        file_frame = ttk.LabelFrame(main_frame, text="文件设置", padding="10")
        file_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 数据文件
        ttk.Label(file_frame, text="数据文件:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.excel_path = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.excel_path, width=50).grid(row=0, column=1, padx=5, pady=2)
        ttk.Button(file_frame, text="浏览...", command=self._browse_excel).grid(row=0, column=2, pady=2)
        
        # 保存目录
        ttk.Label(file_frame, text="保存目录:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.save_dir = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.save_dir, width=50).grid(row=1, column=1, padx=5, pady=2)
        ttk.Button(file_frame, text="浏览...", command=self._browse_save_dir).grid(row=1, column=2, pady=2)
        
        # ===== 列设置区域（现在显示当前配置，可通过预览修改）=====
        col_frame = ttk.LabelFrame(main_frame, text="当前列配置 (点击「预览并选择列」进行设置)", padding="10")
        col_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 配置显示
        config_row = ttk.Frame(col_frame)
        config_row.pack(fill=tk.X)
        
        ttk.Label(config_row, text="姓名:").pack(side=tk.LEFT)
        self.name_col = tk.StringVar(value="E")
        ttk.Label(config_row, textvariable=self.name_col, font=('Consolas', 10, 'bold'), foreground='blue').pack(side=tk.LEFT, padx=(2, 10))
        
        ttk.Label(config_row, text="图片:").pack(side=tk.LEFT)
        self.url_col = tk.StringVar(value="G")
        ttk.Label(config_row, textvariable=self.url_col, font=('Consolas', 10, 'bold'), foreground='blue').pack(side=tk.LEFT, padx=(2, 10))
        
        ttk.Label(config_row, text="来源:").pack(side=tk.LEFT)
        self.source_col = tk.StringVar(value="")
        ttk.Label(config_row, textvariable=self.source_col, font=('Consolas', 10, 'bold'), foreground='blue').pack(side=tk.LEFT, padx=(2, 10))
        
        ttk.Label(config_row, text="说明:").pack(side=tk.LEFT)
        self.intro_col = tk.StringVar(value="F")
        ttk.Label(config_row, textvariable=self.intro_col, font=('Consolas', 10, 'bold'), foreground='blue').pack(side=tk.LEFT, padx=(2, 10))
        
        ttk.Label(config_row, text="起始行:").pack(side=tk.LEFT)
        self.start_row = tk.StringVar(value="2")
        ttk.Label(config_row, textvariable=self.start_row, font=('Consolas', 10, 'bold'), foreground='blue').pack(side=tk.LEFT, padx=(2, 10))
        
        # 预览按钮
        ttk.Button(config_row, text="📊 预览并选择列", command=self._preview_and_select).pack(side=tk.RIGHT)
        
        # ===== 下载设置区域 =====
        setting_frame = ttk.LabelFrame(main_frame, text="下载设置 (模拟自然浏览行为)", padding="10")
        setting_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 第一行：下载间隔范围
        row1 = ttk.Frame(setting_frame)
        row1.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(row1, text="间隔范围:").pack(side=tk.LEFT)
        self.interval_min = tk.StringVar(value="20")
        ttk.Entry(row1, textvariable=self.interval_min, width=4).pack(side=tk.LEFT, padx=2)
        ttk.Label(row1, text="~").pack(side=tk.LEFT)
        self.interval_max = tk.StringVar(value="45")
        ttk.Entry(row1, textvariable=self.interval_max, width=4).pack(side=tk.LEFT, padx=2)
        ttk.Label(row1, text="秒 (随机抖动)").pack(side=tk.LEFT, padx=(0, 15))
        
        ttk.Label(row1, text="每次限制:").pack(side=tk.LEFT)
        self.daily_limit = tk.StringVar(value="50")
        ttk.Entry(row1, textvariable=self.daily_limit, width=4).pack(side=tk.LEFT, padx=2)
        ttk.Label(row1, text="张").pack(side=tk.LEFT)
        
        # 加载数据按钮
        ttk.Button(row1, text="加载数据", command=self._load_data).pack(side=tk.RIGHT)
        
        # 第二行：浏览器模式和自动连续
        row2 = ttk.Frame(setting_frame)
        row2.pack(fill=tk.X, pady=(0, 5))
        
        self.use_browser = tk.BooleanVar(value=True)  # 默认开启
        browser_check = ttk.Checkbutton(
            row2, 
            text="🌐 浏览器模式", 
            variable=self.use_browser
        )
        browser_check.pack(side=tk.LEFT)
        
        # 极速模式
        self.turbo_mode = tk.BooleanVar(value=False)
        turbo_check = ttk.Checkbutton(
            row2,
            text="⚡ 极速模式 (无间隔，适合小批量)",
            variable=self.turbo_mode
        )
        turbo_check.pack(side=tk.LEFT, padx=(20, 0))
        
        # 第三行：自动连续下载
        row3 = ttk.Frame(setting_frame)
        row3.pack(fill=tk.X)
        
        self.auto_continue_var = tk.BooleanVar(value=True)  # 默认开启
        auto_check = ttk.Checkbutton(
            row3,
            text="🔄 自动连续下载 (下载完一批后自动继续)",
            variable=self.auto_continue_var
        )
        auto_check.pack(side=tk.LEFT)
        
        ttk.Label(row3, text="批次间休息:").pack(side=tk.LEFT, padx=(20, 5))
        self.rest_minutes = tk.StringVar(value="30")
        ttk.Entry(row3, textvariable=self.rest_minutes, width=4).pack(side=tk.LEFT)
        ttk.Label(row3, text="分钟").pack(side=tk.LEFT)
        
        # ===== 状态列表 =====
        list_frame = ttk.LabelFrame(main_frame, text="下载状态", padding="5")
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # 创建Treeview
        columns = ('序号', '姓名', '状态', '信息')
        self.status_tree = ttk.Treeview(list_frame, columns=columns, show='headings', height=12)
        
        self.status_tree.heading('序号', text='序号')
        self.status_tree.heading('姓名', text='姓名')
        self.status_tree.heading('状态', text='状态')
        self.status_tree.heading('信息', text='信息')
        
        self.status_tree.column('序号', width=50, anchor=tk.CENTER)
        self.status_tree.column('姓名', width=120)
        self.status_tree.column('状态', width=80, anchor=tk.CENTER)
        self.status_tree.column('信息', width=350)
        
        # 滚动条
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.status_tree.yview)
        self.status_tree.configure(yscrollcommand=scrollbar.set)
        
        self.status_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # 创建右键菜单
        self.tree_context_menu = tk.Menu(self, tearoff=0)
        self.tree_context_menu.add_command(label="📥 下载此项", command=self._download_selected_item)
        self.tree_context_menu.add_command(label="🔄 重新下载 (强制)", command=self._redownload_selected_item)
        self.tree_context_menu.add_separator()
        self.tree_context_menu.add_command(label="📋 复制URL", command=self._copy_selected_url)
        self.tree_context_menu.add_command(label="🗑 从已下载中移除", command=self._remove_from_downloaded)
        
        # 绑定右键事件
        self.status_tree.bind("<Button-3>", self._show_tree_context_menu)
        
        # ===== 进度区域 =====
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=(0, 5))
        
        self.progress_label = ttk.Label(progress_frame, text="就绪 - 请先选择数据文件并预览选择列")
        self.progress_label.pack()
        
        # ===== 控制按钮区域 =====
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=(5, 0))
        
        # 左侧按钮组
        left_btn_frame = ttk.Frame(btn_frame)
        left_btn_frame.pack(side=tk.LEFT)
        
        self.start_btn = ttk.Button(left_btn_frame, text="▶ 开始下载", command=self._start_download, width=12)
        self.start_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.continue_btn = ttk.Button(left_btn_frame, text="⏩ 继续下载", command=self._continue_download, width=12)
        # 默认隐藏，有剩余数据时才显示
        
        self.pause_btn = ttk.Button(left_btn_frame, text="⏸ 暂停", command=self._pause_download, state=tk.DISABLED, width=10)
        self.pause_btn.pack(side=tk.LEFT, padx=5)
        
        self.stop_btn = ttk.Button(left_btn_frame, text="⏹ 停止", command=self._stop_download, state=tk.DISABLED, width=10)
        self.stop_btn.pack(side=tk.LEFT, padx=5)
        
        # 右侧按钮组
        right_btn_frame = ttk.Frame(btn_frame)
        right_btn_frame.pack(side=tk.RIGHT)
        
        self.export_btn = ttk.Button(right_btn_frame, text="📄 导出Markdown", command=self._export_markdown, width=15)
        self.export_btn.pack(side=tk.RIGHT)
        
        self.clear_btn = ttk.Button(right_btn_frame, text="🗑 清除进度", command=self._confirm_clear_session, width=12)
        self.clear_btn.pack(side=tk.RIGHT, padx=(0, 10))
        
        # 统计标签
        self.stats_label = ttk.Label(btn_frame, text="")
        self.stats_label.pack(side=tk.RIGHT, padx=20)
    
    def _bind_events(self):
        """绑定事件"""
        self.protocol("WM_DELETE_WINDOW", self._on_close)
    
    def _show_tree_context_menu(self, event):
        """显示树形列表的右键菜单"""
        # 选中点击的行
        item = self.status_tree.identify_row(event.y)
        if item:
            self.status_tree.selection_set(item)
            self.tree_context_menu.post(event.x_root, event.y_root)
    
    def _get_selected_item_data(self):
        """获取选中项的数据"""
        selection = self.status_tree.selection()
        if not selection:
            messagebox.showwarning("提示", "请先选择一项")
            return None
        
        values = self.status_tree.item(selection[0], 'values')
        # values = (序号, 姓名, 状态, URL信息)
        index = int(values[0]) - 1
        if 0 <= index < len(self.data):
            return self.data[index], selection[0]
        return None
    
    def _download_selected_item(self):
        """下载选中的单个项目"""
        result = self._get_selected_item_data()
        if not result:
            return
        
        item, tree_item = result
        
        if item['url'] in self.downloaded_urls:
            if not messagebox.askyesno("确认", f"'{item['name']}' 已在下载记录中。\n是否仍要下载？"):
                return
        
        self._download_single_item(item, tree_item)
    
    def _redownload_selected_item(self):
        """强制重新下载选中项目（忽略已下载记录）"""
        result = self._get_selected_item_data()
        if not result:
            return
        
        item, tree_item = result
        
        # 从已下载记录中移除
        if item['url'] in self.downloaded_urls:
            self.downloaded_urls.remove(item['url'])
            self._save_session()
        
        self._download_single_item(item, tree_item)
    
    def _download_single_item(self, item, tree_item):
        """执行单个项目的下载"""
        save_dir = self.save_dir.get().strip()
        if not save_dir:
            messagebox.showwarning("提示", "请先选择保存目录")
            return
        
        # 获取间隔范围
        try:
            interval_min = max(5, int(self.interval_min.get()))
            interval_max = max(interval_min, int(self.interval_max.get()))
        except ValueError:
            interval_min, interval_max = 20, 45
        
        # 更新状态显示
        self.status_tree.item(tree_item, values=(
            self.status_tree.item(tree_item, 'values')[0],
            item['name'], "下载中...", ""
        ))
        self.update_idletasks()
        
        # 创建临时下载器（单个下载始终使用极速模式）
        downloader = ImageDownloader(
            save_dir=save_dir,
            interval_min=1,  # 单个下载不需要等待
            interval_max=1,
            use_browser=self.use_browser.get(),
            downloaded_urls=self.downloaded_urls,
            turbo_mode=True  # 单个下载始终极速
        )
        
        # 同步下载（单个项目不需要线程）
        def on_progress(current, total, item_data, status, message):
            status_text = {
                DownloadStatus.SUCCESS: "✓ 成功",
                DownloadStatus.FAILED: "✗ 失败",
                DownloadStatus.SKIPPED: "⊘ 跳过",
                DownloadStatus.DOWNLOADING: "下载中..."
            }.get(status, str(status))
            
            self.status_tree.item(tree_item, values=(
                self.status_tree.item(tree_item, 'values')[0],
                item_data['name'], status_text, message
            ))
            self.update_idletasks()
            
            if status == DownloadStatus.SUCCESS:
                self.downloaded_urls.add(item_data['url'])
                self._save_session()
        
        downloader.on_progress = on_progress
        
        # 在新线程中下载
        import threading
        def download_thread():
            downloader.download_all([item])
        
        thread = threading.Thread(target=download_thread, daemon=True)
        thread.start()
    
    def _copy_selected_url(self):
        """复制选中项的URL到剪贴板"""
        result = self._get_selected_item_data()
        if not result:
            return
        
        item, _ = result
        self.clipboard_clear()
        self.clipboard_append(item['url'])
        messagebox.showinfo("成功", "URL已复制到剪贴板")
    
    def _remove_from_downloaded(self):
        """将选中项从已下载记录中移除"""
        result = self._get_selected_item_data()
        if not result:
            return
        
        item, tree_item = result
        
        if item['url'] not in self.downloaded_urls:
            messagebox.showinfo("提示", "此项目不在已下载记录中")
            return
        
        self.downloaded_urls.remove(item['url'])
        self._save_session()
        
        # 更新显示
        self.status_tree.item(tree_item, values=(
            self.status_tree.item(tree_item, 'values')[0],
            item['name'], "等待中", item['url'][:50] + '...' if len(item['url']) > 50 else item['url']
        ))
        messagebox.showinfo("成功", f"已将 '{item['name']}' 从下载记录中移除")
    
    def _browse_excel(self):
        """浏览选择数据文件"""
        filepath = filedialog.askopenfilename(
            title="选择数据文件",
            filetypes=[
                ("支持的格式", "*.xlsx *.xls *.csv"),
                ("Excel文件", "*.xlsx *.xls"),
                ("CSV文件", "*.csv"),
                ("所有文件", "*.*")
            ]
        )
        if filepath:
            self.excel_path.set(filepath)
    
    def _browse_save_dir(self):
        """浏览选择保存目录"""
        dirpath = filedialog.askdirectory(title="选择保存目录")
        if dirpath:
            self.save_dir.set(dirpath)
            # 尝试加载该目录的下载进度
            self._load_session()
    
    def _get_session_file_path(self):
        """获取会话文件路径"""
        save_dir = self.save_dir.get().strip()
        if save_dir:
            return os.path.join(save_dir, 'download_session.json')
        return None
    
    def _load_session(self):
        """从文件加载下载进度"""
        session_file = self._get_session_file_path()
        if not session_file or not os.path.exists(session_file):
            return
        
        try:
            with open(session_file, 'r', encoding='utf-8') as f:
                session = json.load(f)
            
            self.downloaded_urls = set(session.get('downloaded_urls', []))
            self.total_success = session.get('total_success', 0)
            self.total_fail = session.get('total_fail', 0)
            
            # 更新界面显示
            downloaded_count = len(self.downloaded_urls)
            if downloaded_count > 0:
                self.progress_label.config(text=f"已加载进度: 之前已下载 {downloaded_count} 张")
                self.stats_label.config(text=f"历史: 成功{self.total_success}, 失败{self.total_fail}")
                # 显示继续按钮
                self.continue_btn.pack(side=tk.LEFT, padx=(0, 5), after=self.start_btn)
                
        except Exception as e:
            print(f"加载会话失败: {e}")
    
    def _save_session(self):
        """保存下载进度到文件"""
        session_file = self._get_session_file_path()
        if not session_file:
            return
        
        try:
            session = {
                'downloaded_urls': list(self.downloaded_urls),
                'total_success': self.total_success,
                'total_fail': self.total_fail,
            }
            with open(session_file, 'w', encoding='utf-8') as f:
                json.dump(session, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存会话失败: {e}")
    
    def _clear_session(self):
        """清除下载进度"""
        self.downloaded_urls = set()
        self.total_success = 0
        self.total_fail = 0
        self.current_batch_start = 0
        session_file = self._get_session_file_path()
        if session_file and os.path.exists(session_file):
            try:
                os.remove(session_file)
            except Exception:
                pass
    
    def _confirm_clear_session(self):
        """确认清除下载进度"""
        if not self.downloaded_urls:
            messagebox.showinfo("提示", "当前没有保存的下载进度")
            return
        
        downloaded_count = len(self.downloaded_urls)
        if messagebox.askyesno("确认清除", 
            f"确定要清除下载进度吗？\n\n"
            f"当前已记录 {downloaded_count} 个已下载URL。\n"
            f"清除后将从头开始下载。"):
            self._clear_session()
            self.continue_btn.pack_forget()  # 隐藏继续按钮
            self.progress_label.config(text="下载进度已清除")
            self.stats_label.config(text="")
            messagebox.showinfo("成功", "下载进度已清除")
    
    def _sanitize_filename(self, name):
        """清理文件名"""
        invalid_chars = r'[<>:"/\\|?*]'
        name = re.sub(invalid_chars, '_', name)
        name = name.strip(' .')
        if len(name) > 200:
            name = name[:200]
        return name or 'unnamed'
    
    def _preview_and_select(self):
        """预览表格并选择列"""
        excel_file = self.excel_path.get().strip()
        if not excel_file:
            messagebox.showwarning("提示", "请先选择数据文件")
            return
        
        if not os.path.exists(excel_file):
            messagebox.showerror("错误", "文件不存在")
            return
        
        # 打开预览对话框
        dialog = TablePreviewDialog(self, excel_file)
        self.wait_window(dialog)
        
        # 获取结果
        if dialog.result:
            self.name_col.set(dialog.result['name_col'])
            self.url_col.set(dialog.result['url_col'])
            self.source_col.set(dialog.result.get('source_col', '') or '(无)')
            self.intro_col.set(dialog.result['intro_cols'] if dialog.result['intro_cols'] else '(无)')
            self.start_row.set(str(dialog.result['start_row']))
            self.progress_label.config(text="已设置列配置，点击「加载数据」预览")
    
    def _load_data(self):
        """加载数据（旧的预览功能改名）"""
        excel_file = self.excel_path.get().strip()
        if not excel_file:
            messagebox.showwarning("提示", "请先选择数据文件")
            return
        
        if not os.path.exists(excel_file):
            messagebox.showerror("错误", "文件不存在")
            return
        
        # 检查列配置
        intro_col_val = self.intro_col.get()
        if intro_col_val == '(无)':
            intro_col_val = ''
        
        source_col_val = self.source_col.get()
        if source_col_val == '(无)':
            source_col_val = ''
        
        try:
            reader = DataReader(
                excel_file,
                self.name_col.get(),
                intro_col_val,
                self.url_col.get(),
                int(self.start_row.get()),
                source_col=source_col_val if source_col_val else None
            )
            self.data = reader.read()
            
            # 清空列表
            for item in self.status_tree.get_children():
                self.status_tree.delete(item)
            
            # 加载会话数据
            self._load_session()
            
            # 统计每个姓名出现的次数（检测重名）
            name_counts = {}
            for item in self.data:
                name = item['name']
                name_counts[name] = name_counts.get(name, 0) + 1
            
            # 扫描目录，预校验已存在的文件
            save_dir = self.save_dir.get().strip()
            already_exists_count = 0
            duplicate_names = {name for name, count in name_counts.items() if count > 1}
            
            if duplicate_names:
                print(f"[提示] 发现 {len(duplicate_names)} 个重名: {list(duplicate_names)[:5]}...")
            
            # 显示数据并标记状态
            for i, item in enumerate(self.data, 1):
                name = item['name']
                url = item['url']
                is_duplicate = name in duplicate_names
                
                # 调试输出重名项目
                if is_duplicate:
                    url_in_session = url in self.downloaded_urls
                    print(f"[DEBUG] 重名项目 #{i} '{name}': URL在会话中={url_in_session}, URL={url[:60]}...")
                
                # 检查是否已下载（URL在会话中）- 这是最可靠的检查
                if item['url'] in self.downloaded_urls:
                    status = "✓ 已下载"
                    already_exists_count += 1
                # 对于非重名项目，可以检查文件是否存在
                elif save_dir and not is_duplicate:
                    expected_filename = self._sanitize_filename(name) + '.jpg'
                    expected_path = os.path.join(save_dir, expected_filename)
                    if os.path.exists(expected_path):
                        status = "📁 文件存在"
                        # 将此URL标记为已下载
                        self.downloaded_urls.add(item['url'])
                        already_exists_count += 1
                    else:
                        status = "等待中"
                # 重名项目只依靠URL检查，不依靠文件存在
                elif is_duplicate:
                    status = "等待中 (重名)"
                else:
                    status = "等待中"
                
                self.status_tree.insert('', tk.END, values=(
                    i, item['name'], status, item['url'][:50] + '...' if len(item['url']) > 50 else item['url']
                ))
            
            # 保存更新后的会话
            if already_exists_count > 0:
                self._save_session()
            
            pending_count = len(self.data) - already_exists_count
            self.progress_label.config(text=f"已加载 {len(self.data)} 条数据 (已存在: {already_exists_count}, 待下载: {pending_count})")
            
            if already_exists_count > 0:
                messagebox.showinfo("成功", f"成功读取 {len(self.data)} 条数据\n\n已存在: {already_exists_count} 条\n待下载: {pending_count} 条")
            else:
                messagebox.showinfo("成功", f"成功读取 {len(self.data)} 条数据")
            
        except Exception as e:
            messagebox.showerror("错误", str(e))
    
    def _preview_data(self):
        """兼容旧接口"""
        self._load_data()
    
    def _start_download(self, is_continuation=False):
        """开始下载
        
        Args:
            is_continuation: 是否是自动继续的批次
        """
        # 验证输入
        if not self.data:
            self._load_data()
            if not self.data:
                return
        
        save_dir = self.save_dir.get().strip()
        if not save_dir:
            messagebox.showwarning("提示", "请选择保存目录")
            return
        
        # 获取间隔范围
        try:
            interval_min = max(5, int(self.interval_min.get()))
            interval_max = max(interval_min, int(self.interval_max.get()))
        except ValueError:
            interval_min, interval_max = 20, 45
        
        # 获取每次下载限制
        try:
            limit = int(self.daily_limit.get())
            if limit < 1:
                limit = 50
        except ValueError:
            limit = 50
        
        # 如果是新开始，询问是否清除之前的进度
        if not is_continuation:
            # 加载已有进度
            self._load_session()
            
            if self.downloaded_urls:
                downloaded_count = len(self.downloaded_urls)
                remaining_count = len([d for d in self.data if d['url'] not in self.downloaded_urls])
                
                if remaining_count == 0:
                    if messagebox.askyesno("提示", 
                        f"之前已下载 {downloaded_count} 张，全部完成。\n\n是否清除进度重新下载？"):
                        self._clear_session()
                    else:
                        return
                else:
                    choice = messagebox.askyesnocancel("继续下载", 
                        f"发现之前的下载进度：\n"
                        f"  已下载: {downloaded_count} 张\n"
                        f"  待下载: {remaining_count} 张\n\n"
                        f"是否继续上次的下载？\n\n"
                        f"点击「是」继续下载\n"
                        f"点击「否」清除进度重新开始\n"
                        f"点击「取消」取消操作")
                    
                    if choice is None:  # 取消
                        return
                    elif choice is False:  # 否 - 重新开始
                        self._clear_session()
                    # choice is True - 继续下载，保留进度
            
            self.downloaded_files = {}
        
        # 过滤掉已下载的URL
        pending_data = [d for d in self.data if d['url'] not in self.downloaded_urls]
        
        if not pending_data:
            messagebox.showinfo("完成", "所有数据已下载完毕!")
            return
        
        # 取本批次数据
        # 极速模式：不限制批次大小，直接下载全部
        # 普通模式：限制批次大小，避免触发反爬
        if self.turbo_mode.get():
            download_data = pending_data  # 极速模式下载全部
        else:
            download_data = pending_data[:limit]
        remaining_after_batch = len(pending_data) - len(download_data)
        
        if len(pending_data) > len(download_data) and not is_continuation:
            auto_mode = "开启" if self.auto_continue_var.get() else "关闭"
            messagebox.showinfo(
                "提示", 
                f"待下载 {len(pending_data)} 条，本批次下载 {len(download_data)} 条。\n"
                f"自动连续下载: {auto_mode}"
            )
        
        # 创建下载器，传入已下载URL列表
        self.downloader = ImageDownloader(
            save_dir=save_dir,
            interval_min=interval_min,
            interval_max=interval_max,
            use_browser=self.use_browser.get(),
            downloaded_urls=self.downloaded_urls,  # 共享已下载URL集合
            turbo_mode=self.turbo_mode.get()  # 极速模式
        )
        self.downloader.on_progress = self._on_progress
        self.downloader.on_complete = self._on_complete
        
        # 更新按钮状态
        self.start_btn.config(state=tk.DISABLED)
        self.continue_btn.pack_forget()  # 隐藏继续按钮
        self.pause_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.NORMAL)
        self.is_resting = False
        
        # 启动下载
        print(f"[DEBUG] 已下载URL数量: {len(self.downloaded_urls)}")
        print(f"[DEBUG] 总数据量: {len(self.data)}, 待下载: {len(pending_data)}, 本批次: {len(download_data)}")
        self.download_thread = self.downloader.start(download_data)
    
    def _continue_download(self):
        """继续下载（手动继续或跳过等待）"""
        # 如果正在休息，跳过等待
        if self.is_resting:
            self.is_resting = False
            if self.rest_timer:
                self.after_cancel(self.rest_timer)
                self.rest_timer = None
        
        # 确保会话数据已加载
        if not self.downloaded_urls:
            self._load_session()
        
        # 重置按钮文字
        self.continue_btn.config(text="⏩ 继续下载")
        self._start_download(is_continuation=True)
    
    def _pause_download(self):
        """暂停/继续下载"""
        if self.downloader:
            if self.downloader.is_paused:
                self.downloader.resume()
                self.pause_btn.config(text="⏸ 暂停")
            else:
                self.downloader.pause()
                self.pause_btn.config(text="▶ 继续")
    
    def _stop_download(self):
        """停止下载"""
        if self.downloader:
            self.downloader.stop()
            self._reset_buttons()
    
    def _on_progress(self, current, total, item, status, message):
        """下载进度回调"""
        # 更新进度条
        progress = (current / total) * 100 if total > 0 else 0
        self.progress_var.set(progress)
        
        # 计算剩余时间（使用间隔范围的平均值）
        try:
            interval_min = int(self.interval_min.get())
            interval_max = int(self.interval_max.get())
            avg_interval = (interval_min + interval_max) / 2
        except ValueError:
            avg_interval = 30
        remaining = int((total - current) * avg_interval)
        remaining_min = remaining // 60
        remaining_sec = remaining % 60
        
        self.progress_label.config(
            text=f"进度: {current}/{total} ({progress:.1f}%) - 预计剩余: {remaining_min}分{remaining_sec}秒"
        )
        
        # 更新状态列表
        status_text = {
            DownloadStatus.PENDING: "等待中",
            DownloadStatus.DOWNLOADING: "下载中...",
            DownloadStatus.SUCCESS: "✓ 成功",
            DownloadStatus.FAILED: "✗ 失败",
            DownloadStatus.SKIPPED: "⊘ 跳过"
        }.get(status, status)
        
        # 记录已下载文件
        if status == DownloadStatus.SUCCESS:
            save_dir = self.save_dir.get().strip()
            filename = self._sanitize_filename(item['name']) + '.jpg'
            filepath = os.path.join(save_dir, filename)
            # 处理重名
            if os.path.exists(filepath):
                counter = 2
                while True:
                    filename = f"{self._sanitize_filename(item['name'])}_{counter}.jpg"
                    filepath = os.path.join(save_dir, filename)
                    if not os.path.exists(filepath) or counter > 100:
                        break
                    counter += 1
            self.downloaded_files[item['name']] = {
                'filepath': filepath,
                'filename': filename,
                'intro': item.get('intro', '')
            }
            # 记录URL到已下载集合并保存进度
            self.downloaded_urls.add(item['url'])
            self.total_success += 1
            self._save_session()
        elif status == DownloadStatus.FAILED:
            self.total_fail += 1
            self._save_session()
        elif status == DownloadStatus.SKIPPED:
            # 跳过的项目也要记录URL（可能是"文件已存在"的情况）
            # 这样下次不会重复尝试下载
            if item.get('url') and item['url'] not in self.downloaded_urls:
                self.downloaded_urls.add(item['url'])
                self._save_session()
        
        # 找到对应行并更新（通过姓名匹配，因为索引可能不一致）
        item_name = item['name']
        for tree_item in self.status_tree.get_children():
            values = self.status_tree.item(tree_item, 'values')
            if values[1] == item_name:  # 通过姓名匹配
                original_index = values[0]  # 保留原始序号
                self.status_tree.item(tree_item, values=(
                    original_index, item['name'], status_text, message
                ))
                self.status_tree.see(tree_item)
                break
        
        # 强制刷新界面
        self.update_idletasks()
    
    def _on_complete(self, success_count, fail_count):
        """下载完成回调"""
        self.after(0, lambda: self._finish_download(success_count, fail_count))
    
    def _finish_download(self, success_count, fail_count):
        """完成下载处理"""
        # 计算剩余待下载数量（基于URL，不是索引）
        pending_data = [d for d in self.data if d['url'] not in self.downloaded_urls]
        remaining_items = len(pending_data)
        has_more = remaining_items > 0
        
        auto_continue = self.auto_continue_var.get()
        
        if has_more and auto_continue:
            # 获取休息时间
            try:
                rest_min = max(1, int(self.rest_minutes.get()))
            except ValueError:
                rest_min = 30
            
            self.stats_label.config(text=f"本批: 成功{success_count}/失败{fail_count} | 总计: 成功{self.total_success}/失败{self.total_fail}")
            self.progress_label.config(text=f"批次完成! 休息 {rest_min} 分钟后自动继续 (剩余 {remaining_items} 条)")
            
            # 设置休息计时器
            self.is_resting = True
            # 显示「跳过等待」和「停止」按钮
            self.start_btn.config(state=tk.DISABLED)
            self.continue_btn.config(text="⏭ 跳过等待")
            self.continue_btn.pack(side=tk.LEFT, padx=(0, 5), after=self.start_btn)
            self.pause_btn.config(state=tk.DISABLED)
            self.stop_btn.config(state=tk.NORMAL)  # 可以停止
            self._start_rest_countdown(rest_min * 60)
        else:
            # 全部完成或手动模式
            self._reset_buttons()
            self.stats_label.config(text=f"总计: 成功{self.total_success}, 失败{self.total_fail}")
            
            if has_more:
                self.progress_label.config(text=f"本批完成! 还剩 {remaining_items} 条，点击「⏩ 继续下载」")
                # 显示继续按钮
                self.continue_btn.pack(side=tk.LEFT, padx=(0, 5), after=self.start_btn)
                messagebox.showinfo("批次完成", 
                    f"本批次下载完成!\n"
                    f"本批: 成功 {success_count}, 失败 {fail_count}\n"
                    f"总计: 成功 {self.total_success}, 失败 {self.total_fail}\n\n"
                    f"还剩 {remaining_items} 条，点击「⏩ 继续下载」按钮继续。")
            else:
                self.progress_label.config(text=f"全部下载完成! 成功{self.total_success}, 失败{self.total_fail}")
                messagebox.showinfo("全部完成", 
                    f"所有下载已完成!\n\n"
                    f"总计: 成功 {self.total_success}, 失败 {self.total_fail}\n\n"
                    f"你可以点击「导出Markdown」生成汇总文档。")
    
    def _start_rest_countdown(self, seconds):
        """开始休息倒计时"""
        if not self.is_resting or seconds <= 0:
            # 休息结束，自动继续
            if self.is_resting:
                self._start_download(is_continuation=True)
            return
        
        minutes = seconds // 60
        secs = seconds % 60
        pending_data = [d for d in self.data if d['url'] not in self.downloaded_urls]
        remaining_items = len(pending_data)
        self.progress_label.config(text=f"休息中... {minutes:02d}:{secs:02d} 后自动继续 (剩余 {remaining_items} 条)")
        
        # 每秒更新
        self.rest_timer = self.after(1000, lambda: self._start_rest_countdown(seconds - 1))
    
    def _reset_buttons(self):
        """重置按钮状态"""
        self.start_btn.config(state=tk.NORMAL)
        self.pause_btn.config(state=tk.DISABLED, text="⏸ 暂停")
        self.stop_btn.config(state=tk.DISABLED)
        self.is_resting = False
        if self.rest_timer:
            self.after_cancel(self.rest_timer)
            self.rest_timer = None
    
    def _export_markdown(self):
        """导出Markdown文档"""
        save_dir = self.save_dir.get().strip()
        if not save_dir:
            messagebox.showwarning("提示", "请先选择保存目录")
            return
        
        if not self.data:
            messagebox.showwarning("提示", "请先预览或下载数据")
            return
        
        # 选择保存位置
        md_path = filedialog.asksaveasfilename(
            title="保存Markdown文档",
            initialdir=save_dir,
            initialfile="图片汇总.md",
            defaultextension=".md",
            filetypes=[("Markdown文件", "*.md"), ("所有文件", "*.*")]
        )
        
        if not md_path:
            return
        
        try:
            # 生成Markdown内容
            lines = ["# 图片汇总\n\n"]
            lines.append(f"共 {len(self.data)} 条记录\n\n")
            lines.append("---\n\n")
            
            for i, item in enumerate(self.data, 1):
                name = item['name']
                intro = item.get('intro', '')
                
                lines.append(f"## {i}. {name}\n\n")
                
                # 查找对应的图片文件
                if name in self.downloaded_files:
                    filename = self.downloaded_files[name]['filename']
                    lines.append(f"![{name}](./{filename})\n\n")
                else:
                    # 尝试在目录中查找
                    possible_files = [
                        f"{self._sanitize_filename(name)}.jpg",
                        f"{self._sanitize_filename(name)}_2.jpg",
                    ]
                    found = False
                    for pf in possible_files:
                        if os.path.exists(os.path.join(save_dir, pf)):
                            lines.append(f"![{name}](./{pf})\n\n")
                            found = True
                            break
                    if not found:
                        lines.append("*（图片未下载）*\n\n")
                
                # 个人简介
                if intro:
                    lines.append(f"**简介：** {intro}\n\n")
                
                lines.append("---\n\n")
            
            # 写入文件
            with open(md_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            messagebox.showinfo("成功", f"Markdown文档已保存到:\n{md_path}")
            
            # 询问是否打开
            if messagebox.askyesno("打开文件", "是否用默认程序打开Markdown文档？"):
                os.startfile(md_path)
                
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {str(e)}")
    
    def _on_close(self):
        """关闭窗口"""
        if self.downloader and self.downloader.is_running:
            if messagebox.askyesno("确认", "下载正在进行中，确定要退出吗？"):
                self.downloader.stop()
                self.destroy()
        else:
            self.destroy()


def main():
    app = Application()
    app.mainloop()


if __name__ == "__main__":
    main()
