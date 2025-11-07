"""
单实例运行控制模块
使用文件锁确保程序只运行一个实例
"""

import sys
import os
import tempfile
from pathlib import Path

if sys.platform == 'win32':
    import msvcrt
    
    class SingleInstance:
        """Windows 平台单实例控制"""
        
        def __init__(self, app_name='AI基金大师'):
            """
            初始化单实例控制
            
            参数:
                app_name: 应用名称，用于生成锁文件名
            """
            self.app_name = app_name
            self.lock_file = None
            self.lock_fd = None
            
            # 使用临时目录存放锁文件
            lock_filename = f'{app_name}.lock'.replace(' ', '_')
            self.lock_path = Path(tempfile.gettempdir()) / lock_filename
        
        def is_already_running(self):
            """
            检查是否已有实例在运行
            
            返回:
                True: 已有实例运行
                False: 无实例运行
            """
            try:
                # 尝试打开锁文件
                self.lock_fd = os.open(
                    str(self.lock_path),
                    os.O_CREAT | os.O_EXCL | os.O_RDWR
                )
                
                # 写入当前进程ID
                os.write(self.lock_fd, str(os.getpid()).encode())
                
                return False  # 成功创建锁文件，无实例运行
                
            except FileExistsError:
                # 锁文件已存在，检查是否为僵尸锁
                try:
                    with open(self.lock_path, 'r') as f:
                        old_pid = f.read().strip()
                    
                    # 检查旧进程是否还在运行
                    if old_pid.isdigit():
                        try:
                            # Windows: 使用 tasklist 检查进程
                            import subprocess
                            result = subprocess.run(
                                ['tasklist', '/FI', f'PID eq {old_pid}'],
                                capture_output=True,
                                text=True
                            )
                            
                            if old_pid not in result.stdout:
                                # 旧进程已死，删除僵尸锁
                                os.remove(self.lock_path)
                                return self.is_already_running()  # 重试
                        except Exception:
                            pass
                    
                    return True  # 确实有实例在运行
                    
                except Exception:
                    return True  # 保守起见，认为有实例运行
            
            except Exception as e:
                print(f"单实例检查失败: {e}")
                return False  # 出错时允许运行
        
        def release(self):
            """释放锁"""
            if self.lock_fd is not None:
                try:
                    os.close(self.lock_fd)
                    os.remove(self.lock_path)
                except Exception:
                    pass
        
        def __del__(self):
            """析构时自动释放锁"""
            self.release()

else:
    # Linux/Mac 使用 fcntl
    import fcntl
    
    class SingleInstance:
        """Linux/Mac 平台单实例控制"""
        
        def __init__(self, app_name='AI基金大师'):
            self.app_name = app_name
            self.lock_file = None
            
            # 使用临时目录存放锁文件
            lock_filename = f'{app_name}.lock'.replace(' ', '_')
            self.lock_path = Path(tempfile.gettempdir()) / lock_filename
        
        def is_already_running(self):
            """检查是否已有实例在运行"""
            try:
                self.lock_file = open(self.lock_path, 'w')
                fcntl.lockf(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # 写入当前进程ID
                self.lock_file.write(str(os.getpid()))
                self.lock_file.flush()
                
                return False  # 成功获取锁
                
            except (IOError, OSError):
                return True  # 锁已被占用
            
            except Exception as e:
                print(f"单实例检查失败: {e}")
                return False
        
        def release(self):
            """释放锁"""
            if self.lock_file is not None:
                try:
                    fcntl.lockf(self.lock_file, fcntl.LOCK_UN)
                    self.lock_file.close()
                    os.remove(self.lock_path)
                except Exception:
                    pass
        
        def __del__(self):
            self.release()


def ensure_single_instance(app_name='AI基金大师', show_dialog=True):
    """
    确保只运行一个实例
    
    参数:
        app_name: 应用名称
        show_dialog: 是否显示对话框提示
    
    返回:
        SingleInstance 对象（需要保持引用，否则锁会被释放）
        如果已有实例运行，返回 None
    """
    instance = SingleInstance(app_name)
    
    if instance.is_already_running():
        if show_dialog:
            try:
                import tkinter as tk
                from tkinter import messagebox
                
                root = tk.Tk()
                root.withdraw()  # 隐藏主窗口
                
                messagebox.showwarning(
                    "程序已在运行",
                    f"{app_name} 已经在运行中。\n\n"
                    "请在系统托盘中查找程序图标，\n"
                    "或关闭已有实例后重新启动。"
                )
                
                root.destroy()
            except Exception:
                print(f"{app_name} 已经在运行中！")
        else:
            print(f"{app_name} 已经在运行中！")
        
        return None
    
    return instance


if __name__ == '__main__':
    # 测试代码
    print("测试单实例控制...")
    
    instance = ensure_single_instance('测试应用', show_dialog=False)
    
    if instance:
        print("✓ 这是第一个实例")
        input("按 Enter 退出...")
        instance.release()
    else:
        print("✗ 已有实例在运行")

