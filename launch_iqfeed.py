#! /usr/bin/env python3
# coding=utf-8

"""
This is an example that launches IQConnect.exe.

You need a file called localconfig.py (described in README.md that can be
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

This program launches an instance of IQFeed.exe if it isn't running, creates
an AdminConn and writes messages received by the AdminConn to stdout. It looks
for a file with the name passed as the option ctrl_file, defaults to
/tmp/stop_iqfeed.ctrl. When it sees that file it drops it's connection to
IQFeed.exe, deletes the control file and exits. If there are no other open
connections to IQFeed.exe, IQFeed.exe will by default exit 5 seconds later.

"""

import os
import time
import argparse
import pyiqfeed as iq

from passwords import dtn_product_id, dtn_login, dtn_password

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Launch IQFeed.")
    parser.add_argument('--nohup', action='store_true',
                        dest='nohup', default=False,
                        help="Don't kill IQFeed.exe when this script exists.")
    parser.add_argument('--headless', action="store_true",
                        dest='headless', default=False,
                        help="Launch IQFeed in a headless XServer.")
    parser.add_argument('--control_file', action='store',
                        dest='ctrl_file', default="/tmp/stop_iqfeed.ctrl",
                        help='Stop running if this file exists.')
    arguments = parser.parse_args()

    IQ_FEED = iq.FeedService(product=dtn_product_id,
                             version="IQFEED_LAUNCHER",
                             login=dtn_login,
                             password=dtn_password)

    nohup = arguments.nohup
    headless = arguments.headless
    ctrl_file = arguments.ctrl_file
    IQ_FEED.launch(timeout=30,
                   check_conn=True,
                   headless=headless,
                   nohup=nohup)

    # Modify code below to connect to the socket etc as described above
    admin = iq.AdminConn(name="Launcher")
    admin_listener = iq.VerboseAdminListener("Launcher-listen")
    admin.add_listener(admin_listener)
    with iq.ConnConnector([admin]) as connected:
        admin.client_stats_on()
        while not os.path.isfile(ctrl_file):
            time.sleep(10)

    os.remove(ctrl_file)
