import os
import time
import socket
import select
import threading

class FeedService:

    def __init__(self,
                 product: str,
                 version: str,
                 login: str,
                 password: str,
                 autoconnect: bool,
                 savelogininfo: bool):
        self.product = product
        self.version = version
        self.login = login
        self.password = password
        self.autoconnect = autoconnect
        self.savelogininfo = savelogininfo
        self.launch_msg_printed = False

    def launch(self) -> None:
        iqfeed_args = "-product %s -version %s, -login %s -password %s" % (
            self.product, self.version, self.login, self.password)
        if self.autoconnect:
            iqfeed_args = "%s -autoconnect" % iqfeed_args
        if self.savelogininfo:
            iqfeed_args = "%s -savelogininfo" % iqfeed_args
        if os.name == 'nt':
            # noinspection PyUnresolvedReferences
            import win32api
            # noinspection PyUnresolvedReferences
            import win32con
            win32api.ShellExecute(0, "open", "IQConnect.exe", iqfeed_args, "",
                                  win32con.SW_SHOWNORMAL)
        elif os.name == 'posix':
            import subprocess
            #use nohup to detach the child process:
            iqfeed_call = "nohup wine iqconnect.exe %s" % iqfeed_args
            p = subprocess.Popen(iqfeed_call, shell=True,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, preexec_fn=os.setpgrp)

            #
            # Wait until we can successfully connect to an iqfeed port
            # (sleep had intermittent timeouts & added 5 secs of latency each time)
            # This was more painful than expected but it seems to work nicely:
            #
            host = "127.0.0.1"  #localhost
            port = 9300         #default iqfeed admin port
            timeout = 20        #seconds
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            before = time.time()
            connecting = True
            lock = threading.RLock()
            ready = select.select([s], [], [s], 5)
            while connecting or ready[2]:
                ready = select.select([s], [], [s], 5)
                try:
                    s.connect((host, port))
                    time.sleep(0.01)
                except Exception as err:
                    connecting = True
                    if time.time() - before > timeout:
                        print("Timeout Error: Can not connect to iqfeed...")
                        raise err
                    else:
                        pass
                else:
                    if ready[0]:
                        connecting = False
                        with lock:
                            msg = "S,CONNECT\r\n"
                            while True:
                                s.sendall(msg.encode(encoding='utf-8', errors='strict'))
                                data = s.recv(16384).decode()
                                if ",Connected," in data:
                                    break
                                if time.time() - before > timeout:
                                    raise SystemError("Timeout: Can not connect to iqfeed...")
                            msg = "S, DISCONNECT\r\n"
                            s.sendall(msg.encode(encoding='utf-8', errors='strict'))
                        s.shutdown(socket.SHUT_RDWR)
                        s.close()
                        break

    def admin_variables(self):
        return {"product": self.product,
                "login": self.login,
                "password": self.password,
                "autoconnect": self.autoconnect}


if __name__ == "__main__":
    from pyiqfeed.passwords import dtn_product_id, dtn_login, dtn_password
    feed = FeedService(product=dtn_product_id,
                       version="TestingInIDE",
                       login=dtn_login,
                       password=dtn_password,
                       autoconnect=True,
                       savelogininfo=True)
    feed.launch()
