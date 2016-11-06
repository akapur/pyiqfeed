# coding=utf-8

"""
This is an example that launches IQConnect.exe.

You need a file called password.py (described in README.md that can be
imported here.

This code just launches IQConnect.exe and returns.

You probably want this to launch from cron and exit when you stop
trading, either at an end of day or at an end of week.

You probably also want to  open a socket to IQConnect.exe and maybe read
from the Admin Socket and log some data or put it on a dashboard.  You
can find out for example if your trading code is not reading ticks fast
enough or if the socket get closed.

Note that IQConnect.exe exits once the last connection to it is closed
so you want to keep at least one socket to it open unless you want
IQConnect.exe to exit.

Read the comments and code in service.py for more details.

"""

import pyiqfeed as pi
from passwords import dtn_product_id, dtn_login, dtn_password

if __name__ == "__main__":
    IQ_FEED = pi.FeedService(product=dtn_product_id,
                             version="IQFEED_LAUNCHER",
                             login=dtn_login,
                             password=dtn_password)
    IQ_FEED.launch()

    # Your code to connect to the socket etc as described above here
