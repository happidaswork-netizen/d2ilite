# -*- coding: utf-8 -*-
"""
图片下载模块 - 支持两种模式:
1. 普通模式: 使用requests (适合普通网站)
2. 浏览器模式: 使用Selenium (适合有JS防护的网站，如加速乐/Cloudflare)
"""

import os
import re
import time
import json
import random
import requests
import threading
import base64
import urllib3
from urllib.parse import urlparse
from metadata_writer import write_xmp_metadata, write_description
from text_parser import build_metadata_from_item, extract_name_from_text, looks_like_person_name

# 禁用 SSL 证书验证警告（某些政府网站证书配置有问题）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DownloadStatus:
    """下载状态枚举"""
    PENDING = "pending"       # 等待中
    DOWNLOADING = "downloading"  # 下载中
    SUCCESS = "success"       # 成功
    FAILED = "failed"         # 失败
    SKIPPED = "skipped"       # 已跳过（之前已下载）


# 常用的真实浏览器User-Agent列表
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
]

# 敏感域名列表 - 遇到这些域名必须使用浏览器模拟访问
SENSITIVE_DOMAINS = [
    '.gov.cn',      # 政府网站
    '.edu.cn',      # 教育机构
    '.mil.cn',      # 军事/涉密
    '.org.cn',      # 组织机构
    '12371.cn',     # 共产党员网
    'people.com.cn',# 人民网
    'xinhuanet.com' # 新华网
]


class ImageDownloader:
    """图片下载器 - 支持普通模式和浏览器模式，模拟自然浏览行为"""
    
    def __init__(self, save_dir, interval_min=20, interval_max=45, timeout=30, max_retries=3, use_browser=False, downloaded_urls=None, turbo_mode=False):
        """
        初始化下载器
        
        Args:
            save_dir: 保存目录
            interval_min: 最小下载间隔（秒）
            interval_max: 最大下载间隔（秒）
            timeout: 超时时间（秒）
            max_retries: 最大重试次数
            use_browser: 是否使用浏览器模式（用于绕过JS防护）
            downloaded_urls: 已下载URL集合（由GUI管理）
            turbo_mode: 极速模式（无间隔快速下载，适合小批量）
        """
        self.save_dir = save_dir
        self.interval_min = interval_min
        self.interval_max = interval_max
        self.timeout = timeout
        self.max_retries = max_retries
        self.use_browser = use_browser
        self.turbo_mode = turbo_mode
        
        # 使用传入的已下载集合，如果没有则创建空集合
        self.downloaded = downloaded_urls if downloaded_urls is not None else set()
        
        self._running = False
        self._paused = False
        self._stop_flag = False
        
        # 线程锁（用于多线程安全）
        self._lock = threading.Lock()
        self._progress_lock = threading.Lock()
        
        # 下载统计
        self._success_count = 0
        self._fail_count = 0
        self._completed_count = 0
        
        # 浏览器实例
        self.driver = None
        
        # requests Session
        self.session = requests.Session()
        self._setup_session()
        
        # 回调函数
        self.on_progress = None
        self.on_complete = None
    
    def _setup_session(self):
        """配置Session"""
        user_agent = random.choice(USER_AGENTS)
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        # 忽略SSL证书验证（某些政府网站证书可能有问题）
        self.session.verify = False
    
    def _init_browser(self):
        """初始化浏览器 - 使用 undetected-chromedriver 绕过反爬虫检测"""
        if self.driver is not None:
            return
        
        try:
            # 优先使用 undetected-chromedriver（更好的反检测能力）
            uc_error = None
            try:
                import undetected_chromedriver as uc
                
                options = uc.ChromeOptions()
                # 不使用无头模式，因为很多网站会检测
                # options.add_argument('--headless=new')  # 禁用无头模式！
                options.add_argument('--disable-gpu')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--window-size=1920,1080')
                options.add_argument('--ignore-certificate-errors')
                options.add_argument('--ignore-ssl-errors')
                # 禁用自动化标志
                options.add_argument('--disable-blink-features=AutomationControlled')
                
                # 创建 undetected Chrome
                self.driver = uc.Chrome(options=options, use_subprocess=True)
                self.driver.set_page_load_timeout(self.timeout)
                self._is_undetected = True
                return
                
            except Exception as e:
                # ImportError 或版本不匹配等运行时错误，统一回退到普通 Selenium。
                uc_error = e
                print(f"[警告] undetected-chromedriver 不可用，回退 Selenium: {e}")
            
            # 回退到普通 Selenium（但添加更多反检测措施）
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.chrome.options import Options
            from webdriver_manager.chrome import ChromeDriverManager
            
            options = Options()
            # 不使用无头模式（容易被检测）
            # options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--ignore-ssl-errors')
            # 反自动化检测
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
            options.add_experimental_option('useAutomationExtension', False)
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            self.driver.set_page_load_timeout(self.timeout)
            
            # 移除 webdriver 标志
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })
            self._is_undetected = False
            
        except Exception as e:
            raise Exception(f"初始化浏览器失败: {str(e)}")
    
    def _close_browser(self):
        """关闭浏览器"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
    
    def _load_progress(self):
        """加载已下载记录"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return set(json.load(f))
            except Exception:
                pass
        return set()
    
    def _save_progress(self):
        """保存下载进度"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.downloaded), f, ensure_ascii=False)
        except Exception:
            pass
    
    def _sanitize_filename(self, name):
        """清理文件名"""
        invalid_chars = r'[<>:"/\\|?*]'
        name = re.sub(invalid_chars, '_', name)
        name = name.strip(' .')
        if len(name) > 200:
            name = name[:200]
        return name or 'unnamed'
    
    def _get_unique_filename(self, base_name, ext='.jpg'):
        """获取唯一文件名"""
        filename = self._sanitize_filename(base_name) + ext
        filepath = os.path.join(self.save_dir, filename)
        
        if not os.path.exists(filepath):
            return filepath
        
        counter = 2
        while True:
            filename = f"{self._sanitize_filename(base_name)}_{counter}{ext}"
            filepath = os.path.join(self.save_dir, filename)
            if not os.path.exists(filepath):
                return filepath
            counter += 1
    
    def _download_with_requests(self, url, save_path):
        """使用requests下载"""
        headers = {'Referer': f"{urlparse(url).scheme}://{urlparse(url).netloc}/"}
        
        response = self.session.get(url, headers=headers, timeout=self.timeout, stream=True)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return save_path
    
    def _download_with_browser(self, url, save_path):
        """使用浏览器下载（绕过JS防护如Cloudflare/加速乐）"""
        try:
            from urllib.parse import urlparse
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}/"
            domain = parsed.netloc
            
            # 检查是否已经验证过此域名
            if not hasattr(self, '_verified_domains'):
                self._verified_domains = set()
            
            # 首次访问此域名时，先去首页通过 Cloudflare 验证
            if domain not in self._verified_domains:
                try:
                    self.driver.get(base_url)
                    # 等待 Cloudflare 验证完成（通常需要 5-10 秒）
                    time.sleep(8)
                    
                    # 检查是否还在验证页面
                    page_source = self.driver.page_source.lower()
                    cloudflare_indicators = ['checking your browser', 'just a moment', 'ddos protection', 'ray id']
                    
                    retry_count = 0
                    while any(ind in page_source for ind in cloudflare_indicators) and retry_count < 6:
                        time.sleep(5)
                        page_source = self.driver.page_source.lower()
                        retry_count += 1
                    
                    self._verified_domains.add(domain)
                    
                except Exception as e:
                    # 即使首页访问失败，也继续尝试下载
                    pass
            
            # 访问图片URL
            self.driver.get(url)
            time.sleep(4)  # 等待页面加载
            
            # 获取浏览器的cookies
            cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}
            
            # 使用带cookies的requests下载图片
            headers = {
                'User-Agent': self.driver.execute_script("return navigator.userAgent;"),
                'Referer': base_url,
                'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            }
            
            response = requests.get(url, headers=headers, cookies=cookies, timeout=self.timeout, verify=False)
            
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'image' in content_type or len(response.content) > 1000:
                    with open(save_path, 'wb') as f:
                        f.write(response.content)
                    return save_path
            
            # 如果直接下载失败，尝试从浏览器页面截取图片
            try:
                # 检查页面是否有图片
                imgs = self.driver.find_elements(By.TAG_NAME, "img")
                if imgs:
                    img_src = imgs[0].get_attribute("src")
                    if img_src:
                        if img_src.startswith("data:image"):
                            # Base64图片
                            img_data = img_src.split(",", 1)[1]
                            with open(save_path, "wb") as f:
                                f.write(base64.b64decode(img_data))
                            return save_path
                        elif img_src.startswith("http"):
                            resp = requests.get(img_src, headers=headers, cookies=cookies, 
                                              timeout=self.timeout, verify=False)
                            if resp.status_code == 200 and len(resp.content) > 500:
                                with open(save_path, "wb") as f:
                                    f.write(resp.content)
                                return save_path
            except Exception:
                pass
            
            raise Exception(f"HTTP {response.status_code}" if 'response' in dir() else "无法获取图片")
            
        except Exception as e:
            raise Exception(f"浏览器下载失败: {str(e)}")


    
    def _should_use_browser(self, url):
        """检测是否需要使用浏览器（针对敏感域名）"""
        try:
            domain = urlparse(url).netloc.lower()
            return any(d in domain for d in SENSITIVE_DOMAINS)
        except:
            return False

    def _download_image(self, url, save_path):
        """下载单张图片 - 智能路由"""
        # 1. 决策：是否需要浏览器
        # 显式开启、敏感域名、或包含防爬特征
        route_to_browser = self.use_browser or self._should_use_browser(url)
        
        if route_to_browser:
            # 2. 懒加载：确保浏览器已启动
            if self.driver is None:
                # 使用线程锁防止多线程同时启动浏览器
                with self._lock: 
                    self._init_browser()
            return self._download_with_browser(url, save_path)
        else:
            # 3. 直连下载
            return self._download_with_requests(url, save_path)
    
    def _download_single(self, item, index, total):
        """
        下载单个文件（线程安全）
        
        Returns:
            tuple: (success: bool, item: dict, error_msg: str)
        """
        # 检查停止标志
        if self._stop_flag:
            return (False, item, "已停止")
        
        # 等待暂停
        while self._paused and not self._stop_flag:
            time.sleep(0.5)
        
        if self._stop_flag:
            return (False, item, "已停止")
        
        raw_name = item.get('name', '')
        intro = item.get('intro', '')
        url = item['url']

        # 兜底：抓取表格里“题头/标题”经常不是人名；优先从简介语义抽取姓名用于命名与写入元数据
        name = str(raw_name).strip() if raw_name is not None else ''
        if name and " - " in name:
            candidate = name.split(" - ", 1)[0].strip()
            if looks_like_person_name(candidate):
                name = candidate

        intro_text = str(intro).strip() if intro else ''
        derived_name = extract_name_from_text(intro_text)
        if derived_name:
            # 只要简介里能明确抽取到姓名，就优先用它（题头/岗位经常误导）
            if (not name) or (not intro_text.startswith(name)) or (not looks_like_person_name(name)):
                name = derived_name
                item['name'] = name
        
        # 线程安全地检查URL是否已下载
        with self._lock:
            if url in self.downloaded:
                with self._progress_lock:
                    self._completed_count += 1
                    if self.on_progress:
                        self.on_progress(self._completed_count, total, item, DownloadStatus.SKIPPED, "URL已下载")
                return (True, item, "跳过")
        
        # 预先判断下载模式用于显示
        is_stealth = self.use_browser or self._should_use_browser(url)
        mode_label = "🕵️ Stealth" if is_stealth else "⚡ Turbo"

        # 通知开始下载
        with self._progress_lock:
            if self.on_progress:
                self.on_progress(self._completed_count + 1, total, item, DownloadStatus.DOWNLOADING, f"[{mode_label}] 下载中...")
        
        success = False
        error_msg = ""
        
        for attempt in range(self.max_retries):
            try:
                # 线程安全地获取唯一文件名
                with self._lock:
                    save_path = self._get_unique_filename(name, '.jpg')
                temp_path = save_path + '.tmp'
                
                self._download_image(url, temp_path)
                
                # 构建元数据（自动从简介提取性别、年龄、职业等）
                try:
                    metadata = build_metadata_from_item(item)
                    final_path = write_xmp_metadata(temp_path, metadata)
                except Exception as xmp_err:
                    print(f"[警告] XMP 元数据写入失败 ({name}): {xmp_err}")
                    try:
                        final_path = write_description(temp_path, intro)
                    except Exception as exif_err:
                        print(f"[警告] EXIF 元数据写入也失败 ({name}): {exif_err}")
                        final_path = temp_path
                
                # 线程安全地重命名文件
                with self._lock:
                    if final_path != save_path:
                        if os.path.exists(save_path):
                            os.remove(save_path)
                        os.rename(final_path, save_path)
                    elif os.path.exists(temp_path):
                        os.rename(temp_path, save_path)
                    
                    self.downloaded.add(url)
                
                success = True
                break
                
            except Exception as e:
                error_msg = str(e)
                if 'temp_path' in locals() and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass
                
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt + random.uniform(1, 3))
        
        # 更新进度
        with self._progress_lock:
            self._completed_count += 1
            if success:
                self._success_count += 1
                if self.on_progress:
                    self.on_progress(self._completed_count, total, item, DownloadStatus.SUCCESS, "下载成功")
            else:
                self._fail_count += 1
                if self.on_progress:
                    self.on_progress(self._completed_count, total, item, DownloadStatus.FAILED, f"失败: {error_msg[:50]}")
        
        return (success, item, error_msg)
    
    def download_all(self, items):
        """批量下载所有图片（顺序队列模式，避免触发反爬）"""
        self._running = True
        self._stop_flag = False
        self._success_count = 0
        self._fail_count = 0
        self._completed_count = 0
        
        total = len(items)
        os.makedirs(self.save_dir, exist_ok=True)
        
        # 如果使用浏览器模式，初始化浏览器
        if self.use_browser:
            try:
                self._init_browser()
            except Exception as e:
                if self.on_progress:
                    self.on_progress(0, total, {}, DownloadStatus.FAILED, f"浏览器初始化失败: {str(e)}")
                self._running = False
                if self.on_complete:
                    self.on_complete(0, 0)
                return
        
        try:
            for i, item in enumerate(items):
                if self._stop_flag:
                    break
                
                self._download_single(item, i, total)
                
                # 下载间隔（极速模式无间隔，普通模式有间隔避免反爬）
                if i < len(items) - 1 and not self._stop_flag and not self.turbo_mode:
                    actual_interval = random.uniform(self.interval_min, self.interval_max)
                    time.sleep(actual_interval)
        
        finally:
            if self.use_browser:
                self._close_browser()
            
            self._running = False
            
            if self.on_complete:
                self.on_complete(self._success_count, self._fail_count)
    
    def start(self, items):
        """在新线程中启动下载"""
        thread = threading.Thread(target=self.download_all, args=(items,))
        thread.daemon = True
        thread.start()
        return thread
    
    def pause(self):
        self._paused = True
    
    def resume(self):
        self._paused = False
    
    def stop(self):
        self._stop_flag = True
        self._paused = False
    
    @property
    def is_running(self):
        return self._running
    
    @property
    def is_paused(self):
        return self._paused
