import os
import time
import socket


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
            iqfeed_call = "wine iqconnect.exe %s" % iqfeed_args
            p = subprocess.Popen(iqfeed_call, shell=True,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
            s = socket.socket()
            host = "127.0.0.1"
            port = 9400
            timeout = 30 #seconds
            before = time.time()
            connecting = True
            while connecting:
                try:
                    s.connect((host, port))
                except:
                    connecting = True
                    if time.time() - before > timeout:
                        raise SystemError('Timeout: Can not connect to the iqfeed port...')
                else:
                    connecting = False
                    s.recv(16384)
                    s.close()

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
