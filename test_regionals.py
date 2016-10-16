# coding=utf-8
"""Recreating problem with no regionals messages"""


import time
import numpy as np

import pyiqfeed as iq
from passwords import dtn_product_id, dtn_login, dtn_password


def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = iq.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(check_conn=False)
    time.sleep(2)


class RegionalOnlyListener(iq.SilentQuoteListener):
    def __init__(self, name: str):
        super().__init__(name)

    def process_regional_quote(self, quote: np.array) -> None:
        """Not silent"""
        print("Regional receivewd by %s:" % self._name)
        print(quote)


if __name__ == "__main__":
    ticker = "IBM"
    wait = 120

    launch_service()

    quote_conn = iq.QuoteConn(name="pyiqfeed-Test-regional")
    quote_conn.start_runner()
    quote_conn.set_log_levels(["Admin",
                               "L1Data", "L1Request", "L1System", "L1Error",
                               "L2Data", "L2Request", "L2System", "L2Error",
                               "LookupData", "LookupRequest", "LookupError",
                               "Information", "Debug", "Connectivity"])
    quote_conn.set_log_levels(["Admin",
                               "L1Data", "L1Request", "L1System", "L1Error",
                               "L2Data", "L2Request", "L2System", "L2Error",
                               "LookupData", "LookupRequest", "LookupError",
                               "Information", "Debug", "Connectivity"])

    quote_listener = iq.VerboseQuoteListener("Regional Listener")
    quote_conn.add_listener(quote_listener)

    quote_conn.regional_watch(ticker)
    time.sleep(wait)
    quote_conn.regional_unwatch(ticker)
    quote_conn.remove_listener(quote_listener)
    quote_conn.stop_runner()
    del quote_conn