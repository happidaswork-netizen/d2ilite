# -*- coding: utf-8 -*-
"""队列管理器 - 支持多队列并发下载"""

import os
import json
import uuid
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
from enum import Enum

from excel_reader import DataReader
from downloader import ImageDownloader, DownloadStatus


class QueueStatus(Enum):
    """队列状态"""
    PENDING = "pending"       # 等待中
    RUNNING = "running"       # 运行中
    PAUSED = "paused"         # 已暂停
    STOPPED = "stopped"       # 已停止
    COMPLETED = "completed"   # 已完成


@dataclass
class DownloadQueue:
    """下载队列"""
    id: str
    name: str
    data_file: str
    save_dir: str
    
    # 列配置
    name_col: str = "E"
    url_col: str = "G"
    intro_cols: str = "F"
    source_col: str = ""
    start_row: int = 2
    
    # 下载配置
    turbo_mode: bool = False
    use_browser: bool = True
    interval_min: int = 20
    interval_max: int = 45
    batch_limit: int = 50
    
    # 运行状态
    status: QueueStatus = QueueStatus.PENDING
    progress: int = 0
    total: int = 0
    success_count: int = 0
    fail_count: int = 0
    
    # 内部对象
    items: List[dict] = field(default_factory=list)
    downloaded_urls: set = field(default_factory=set)
    thread: Optional[threading.Thread] = None
    downloader: Optional[ImageDownloader] = None
    
    # 回调
    on_progress: Optional[Callable] = None
    on_status_change: Optional[Callable] = None


class QueueManager:
    """队列管理器 - 管理多个并发下载队列"""
    
    def __init__(self, state_file: str = None):
        self.queues: Dict[str, DownloadQueue] = {}
        self._lock = threading.Lock()
        
        # 状态文件路径
        if state_file:
            self.state_file = state_file
        else:
            # 默认保存在脚本目录下
            self.state_file = os.path.join(os.path.dirname(__file__), 'queue_state.json')
        
        # 全局回调
        self.on_queue_added: Optional[Callable] = None
        self.on_queue_removed: Optional[Callable] = None
        self.on_queue_updated: Optional[Callable] = None
    
    def create_queue(self, config: dict) -> str:
        """
        创建新队列
        
        Args:
            config: {
                'name': str,
                'data_file': str,
                'save_dir': str,
                'name_col': str,
                'url_col': str,
                'intro_cols': str,
                'source_col': str,
                'start_row': int,
                'turbo_mode': bool,
                'use_browser': bool,
                'interval_min': int,
                'interval_max': int,
                'batch_limit': int
            }
        
        Returns:
            队列 ID
        """
        queue_id = str(uuid.uuid4())[:8]
        
        queue = DownloadQueue(
            id=queue_id,
            name=config.get('name', f'队列-{queue_id}'),
            data_file=config['data_file'],
            save_dir=config['save_dir'],
            name_col=config.get('name_col', 'E'),
            url_col=config.get('url_col', 'G'),
            intro_cols=config.get('intro_cols', 'F'),
            source_col=config.get('source_col', ''),
            start_row=config.get('start_row', 2),
            turbo_mode=config.get('turbo_mode', False),
            use_browser=config.get('use_browser', True),
            interval_min=config.get('interval_min', 20),
            interval_max=config.get('interval_max', 45),
            batch_limit=config.get('batch_limit', 50)
        )
        
        # 加载数据
        self._load_queue_data(queue)
        
        with self._lock:
            self.queues[queue_id] = queue
        
        if self.on_queue_added:
            self.on_queue_added(queue)
        
        return queue_id
    
    def _load_queue_data(self, queue: DownloadQueue):
        """加载队列数据"""
        try:
            reader = DataReader(
                queue.data_file,
                queue.name_col,
                queue.intro_cols,
                queue.url_col,
                queue.start_row,
                source_col=queue.source_col if queue.source_col else None
            )
            queue.items = reader.read()
            queue.total = len(queue.items)
        except Exception as e:
            queue.items = []
            queue.total = 0
            raise Exception(f"加载数据失败: {e}")
    
    def remove_queue(self, queue_id: str):
        """移除队列"""
        with self._lock:
            if queue_id not in self.queues:
                return
            
            queue = self.queues[queue_id]
            
            # 停止运行中的队列
            if queue.status == QueueStatus.RUNNING:
                self.stop_queue(queue_id)
            
            del self.queues[queue_id]
        
        if self.on_queue_removed:
            self.on_queue_removed(queue_id)
    
    def start_queue(self, queue_id: str):
        """启动队列"""
        queue = self.queues.get(queue_id)
        if not queue:
            return

        # 已暂停的队列优先走恢复逻辑
        if queue.status == QueueStatus.PAUSED and queue.downloader:
            self.resume_queue(queue_id)
            return
        
        if queue.status == QueueStatus.RUNNING:
            return

        # 兜底：确保 progress 与统计字段一致（progress 表示已处理数量）
        queue.success_count = max(queue.success_count, len(queue.downloaded_urls))
        queue.progress = min(queue.success_count + queue.fail_count, queue.total)

        # 过滤已下载项（基于 URL）
        pending_items = [
            item for item in queue.items
            if item.get('url') not in queue.downloaded_urls
        ]

        # 没有待下载项
        if not pending_items:
            queue.progress = queue.total
            queue.status = QueueStatus.COMPLETED
            if self.on_queue_updated:
                self.on_queue_updated(queue)
            return
        
        # 创建下载器
        queue.downloader = ImageDownloader(
            save_dir=queue.save_dir,
            interval_min=queue.interval_min,
            interval_max=queue.interval_max,
            use_browser=queue.use_browser,
            downloaded_urls=queue.downloaded_urls,
            turbo_mode=queue.turbo_mode
        )
        
        # 极速模式：不限制批次大小，直接下载全部
        # 普通模式：限制批次大小，避免触发反爬
        if queue.turbo_mode:
            batch_items = pending_items  # 极速模式下载全部
        else:
            batch_items = pending_items[:queue.batch_limit]

        queue.status = QueueStatus.RUNNING

        # 设置回调
        def on_progress(current, total, item, status, message):
            if status == DownloadStatus.SUCCESS:
                queue.success_count += 1
                url = item.get('url')
                if url:
                    queue.downloaded_urls.add(url)
            elif status == DownloadStatus.SKIPPED:
                queue.success_count += 1
                url = item.get('url')
                if url:
                    queue.downloaded_urls.add(url)
            elif status == DownloadStatus.FAILED:
                queue.fail_count += 1

            # progress 表示已处理数量（成功 + 失败）；DOWNLOADING 不推进
            if status in (DownloadStatus.SUCCESS, DownloadStatus.SKIPPED, DownloadStatus.FAILED):
                queue.progress = min(queue.success_count + queue.fail_count, queue.total)
            
            if queue.on_progress:
                # 注意：我们也应该把 total 修正为 queue.total 传给 UI
                queue.on_progress(queue, queue.progress, queue.total, item, status, message)
            
            if self.on_queue_updated:
                self.on_queue_updated(queue)
        
        def on_complete(success, fail):
            # stop_queue 会把状态设为 STOPPED；不要被 on_complete 覆盖
            if queue.status == QueueStatus.STOPPED:
                if self.on_queue_updated:
                    self.on_queue_updated(queue)
                return

            queue.progress = min(queue.success_count + queue.fail_count, queue.total)
            queue.status = QueueStatus.COMPLETED
            if self.on_queue_updated:
                self.on_queue_updated(queue)
        
        queue.downloader.on_progress = on_progress
        queue.downloader.on_complete = on_complete
        queue.thread = queue.downloader.start(batch_items)
        
        if self.on_queue_updated:
            self.on_queue_updated(queue)
    
    def pause_queue(self, queue_id: str):
        """暂停队列"""
        queue = self.queues.get(queue_id)
        if not queue or not queue.downloader:
            return
        
        queue.downloader.pause()
        queue.status = QueueStatus.PAUSED
        
        if self.on_queue_updated:
            self.on_queue_updated(queue)
    
    def resume_queue(self, queue_id: str):
        """恢复队列"""
        queue = self.queues.get(queue_id)
        if not queue or not queue.downloader:
            return
        
        queue.downloader.resume()
        queue.status = QueueStatus.RUNNING
        
        if self.on_queue_updated:
            self.on_queue_updated(queue)
    
    def stop_queue(self, queue_id: str):
        """停止队列"""
        queue = self.queues.get(queue_id)
        if not queue:
            return
        
        if queue.downloader:
            queue.downloader.stop()
        
        queue.status = QueueStatus.STOPPED
        
        if self.on_queue_updated:
            self.on_queue_updated(queue)
    
    def get_queue(self, queue_id: str) -> Optional[DownloadQueue]:
        """获取队列"""
        return self.queues.get(queue_id)
    
    def update_queue_settings(self, queue_id: str, settings: dict):
        """
        更新队列设置（可在运行时动态修改）
        
        Args:
            queue_id: 队列 ID
            settings: {
                'turbo_mode': bool,
                'interval_min': int,
                'interval_max': int,
                'batch_limit': int,
                'use_browser': bool,
                'name_col': str,       # 列配置
                'url_col': str,
                'intro_cols': str,
                'source_col': str,
                'start_row': int
            }
        """
        queue = self.queues.get(queue_id)
        if not queue:
            return
        
        # 检查列配置是否有变化
        col_config_changed = False
        for key in ['name_col', 'url_col', 'intro_cols', 'source_col', 'start_row']:
            if key in settings and getattr(queue, key) != settings[key]:
                col_config_changed = True
                break
        
        # 更新队列设置
        if 'turbo_mode' in settings:
            queue.turbo_mode = settings['turbo_mode']
        if 'interval_min' in settings:
            queue.interval_min = settings['interval_min']
        if 'interval_max' in settings:
            queue.interval_max = settings['interval_max']
        if 'batch_limit' in settings:
            queue.batch_limit = settings['batch_limit']
        if 'use_browser' in settings:
            queue.use_browser = settings['use_browser']
        
        # 更新列配置
        if 'name_col' in settings:
            queue.name_col = settings['name_col']
        if 'url_col' in settings:
            queue.url_col = settings['url_col']
        if 'intro_cols' in settings:
            queue.intro_cols = settings['intro_cols']
        if 'source_col' in settings:
            queue.source_col = settings['source_col']
        if 'start_row' in settings:
            queue.start_row = settings['start_row']
        
        # 如果列配置变化了，需要重新加载数据
        if col_config_changed:
            try:
                old_downloaded = queue.downloaded_urls.copy()
                self._load_queue_data(queue)
                queue.downloaded_urls = old_downloaded
                queue.success_count = len([item for item in queue.items if item.get('url') in old_downloaded])
                queue.progress = min(queue.success_count + queue.fail_count, queue.total)
            except Exception as e:
                print(f"重新加载数据失败: {e}")
        
        # 如果有正在运行的下载器，更新其设置
        if queue.downloader:
            queue.downloader.turbo_mode = queue.turbo_mode
            queue.downloader.interval_min = queue.interval_min
            queue.downloader.interval_max = queue.interval_max
        
        if self.on_queue_updated:
            self.on_queue_updated(queue)
    
    def get_all_queues(self) -> List[DownloadQueue]:
        """获取所有队列"""
        return list(self.queues.values())
    
    def start_all(self):
        """启动所有等待中的队列"""
        for queue_id, queue in self.queues.items():
            if queue.status in (QueueStatus.PENDING, QueueStatus.STOPPED, QueueStatus.COMPLETED) and queue.progress < queue.total:
                self.start_queue(queue_id)
    
    def pause_all(self):
        """暂停所有运行中的队列"""
        for queue_id, queue in self.queues.items():
            if queue.status == QueueStatus.RUNNING:
                self.pause_queue(queue_id)
    
    def stop_all(self):
        """停止所有队列"""
        for queue_id in list(self.queues.keys()):
            self.stop_queue(queue_id)
    
    def save_state(self):
        """保存队列状态到文件"""
        state = {
            'queues': []
        }
        
        for queue in self.queues.values():
            queue_data = {
                'id': queue.id,
                'name': queue.name,
                'data_file': queue.data_file,
                'save_dir': queue.save_dir,
                'name_col': queue.name_col,
                'url_col': queue.url_col,
                'intro_cols': queue.intro_cols,
                'source_col': queue.source_col,
                'start_row': queue.start_row,
                'turbo_mode': queue.turbo_mode,
                'use_browser': queue.use_browser,
                'interval_min': queue.interval_min,
                'interval_max': queue.interval_max,
                'batch_limit': queue.batch_limit,
                'downloaded_urls': list(queue.downloaded_urls),
                'success_count': queue.success_count,
                'fail_count': queue.fail_count
            }
            state['queues'].append(queue_data)
        
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存队列状态失败: {e}")
    
    def load_state(self):
        """从文件加载队列状态"""
        if not os.path.exists(self.state_file):
            return
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            for queue_data in state.get('queues', []):
                # 检查数据文件是否存在
                if not os.path.exists(queue_data.get('data_file', '')):
                    continue
                
                queue = DownloadQueue(
                    id=queue_data['id'],
                    name=queue_data['name'],
                    data_file=queue_data['data_file'],
                    save_dir=queue_data['save_dir'],
                    name_col=queue_data.get('name_col', 'E'),
                    url_col=queue_data.get('url_col', 'G'),
                    intro_cols=queue_data.get('intro_cols', 'F'),
                    source_col=queue_data.get('source_col', ''),
                    start_row=queue_data.get('start_row', 2),
                    turbo_mode=queue_data.get('turbo_mode', False),
                    use_browser=queue_data.get('use_browser', True),
                    interval_min=queue_data.get('interval_min', 20),
                    interval_max=queue_data.get('interval_max', 45),
                    batch_limit=queue_data.get('batch_limit', 50),
                    downloaded_urls=set(queue_data.get('downloaded_urls', [])),
                    success_count=queue_data.get('success_count', 0),
                    fail_count=queue_data.get('fail_count', 0)
                )
                
                # 加载数据文件内容
                try:
                    self._load_queue_data(queue)
                    queue.success_count = max(queue.success_count, len(queue.downloaded_urls))
                    queue.progress = min(queue.success_count + queue.fail_count, queue.total)
                except Exception:
                    continue
                
                with self._lock:
                    self.queues[queue.id] = queue
                
                if self.on_queue_added:
                    self.on_queue_added(queue)
        
        except Exception as e:
            print(f"加载队列状态失败: {e}")
