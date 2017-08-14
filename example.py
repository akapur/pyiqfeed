#! /usr/bin/env python3
# coding=utf-8
"""
Some examples of how to use the library.

Run first with no options to see the usage message.
Then try with different options to see that different functionality. Not all
library functionality is used in this file. Look at conn.py and listeners.py
for more details.
"""

import argparse
import datetime
import pyiqfeed as iq
import time
from localconfig import dtn_product_id, dtn_login, dtn_password


def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = iq.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(headless=True)

    # If you are running headless comment out the line above and uncomment
    # the line below instead. This runs IQFeed.exe using the xvfb X Framebuffer
    # server since IQFeed.exe runs under wine and always wants to create a GUI
    # window.
    # svc.launch(headless=True)


def get_level_1_quotes_and_trades(ticker: str, seconds: int):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = iq.QuoteConn(name="pyiqfeed-Example-lvl1")
    quote_listener = iq.VerboseQuoteListener("Level 1 Listener")
    quote_conn.add_listener(quote_listener)
    with iq.ConnConnector([quote_conn]) as connector:
        all_fields = sorted(list(iq.QuoteConn.quote_msg_map.keys()))
        quote_conn.select_update_fieldnames(all_fields)
        quote_conn.watch(ticker)
        time.sleep(seconds)
        quote_conn.unwatch(ticker)
        quote_conn.remove_listener(quote_listener)


def get_regional_quotes(ticker: str, seconds: int):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = iq.QuoteConn(name="pyiqfeed-Example-regional")
    quote_listener = iq.VerboseQuoteListener("Regional Listener")
    quote_conn.add_listener(quote_listener)

    with iq.ConnConnector([quote_conn]) as connector:
        quote_conn.regional_watch(ticker)
        time.sleep(seconds)
        quote_conn.regional_unwatch(ticker)


def get_trades_only(ticker: str, seconds: int):
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    quote_conn = iq.QuoteConn(name="pyiqfeed-Example-trades-only")
    quote_listener = iq.VerboseQuoteListener("Trades Listener")
    quote_conn.add_listener(quote_listener)

    with iq.ConnConnector([quote_conn]) as connector:
        quote_conn.trades_watch(ticker)
        time.sleep(seconds)
        quote_conn.unwatch(ticker)


def get_live_interval_bars(ticker: str, bar_len: int, seconds: int):
    """Get real-time interval bars"""
    bar_conn = iq.BarConn(name='pyiqfeed-Example-interval-bars')
    bar_listener = iq.VerboseBarListener("Bar Listener")
    bar_conn.add_listener(bar_listener)

    with iq.ConnConnector([bar_conn]) as connector:
        bar_conn.watch(symbol=ticker, interval_len=bar_len,
                       interval_type='s', update=1, lookback_bars=10)
        time.sleep(seconds)


def get_administrative_messages(seconds: int):
    """Run and AdminConn and print connection stats to screen."""

    admin_conn = iq.AdminConn(name="pyiqfeed-Example-admin-messages")
    admin_listener = iq.VerboseAdminListener("Admin Listener")
    admin_conn.add_listener(admin_listener)

    with iq.ConnConnector([admin_conn]) as connector:
        admin_conn.set_admin_variables(product=dtn_product_id,
                                       login=dtn_login,
                                       password=dtn_password,
                                       autoconnect=True)
        admin_conn.client_stats_on()
        time.sleep(seconds)


def get_tickdata(ticker: str, max_ticks: int, num_days: int):
    """Show how to read tick-data"""

    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-tickdata")
    hist_listener = iq.VerboseIQFeedListener("History Tick Listener")
    hist_conn.add_listener(hist_listener)

    # Look at conn.py for request_ticks, request_ticks_for_days and
    # request_ticks_in_period to see various ways to specify time periods
    # etc.

    with iq.ConnConnector([hist_conn]) as connector:
        # Get the last 10 trades
        try:
            tick_data = hist_conn.request_ticks(ticker=ticker,
                                                max_ticks=max_ticks)
            print(tick_data)

            # Get the last num_days days trades between 10AM and 12AM
            # Limit to max_ticks ticks else too much will be printed on screen
            bgn_flt = datetime.time(hour=10, minute=0, second=0)
            end_flt = datetime.time(hour=12, minute=0, second=0)
            tick_data = hist_conn.request_ticks_for_days(ticker=ticker,
                                                         num_days=num_days,
                                                         bgn_flt=bgn_flt,
                                                         end_flt=end_flt,
                                                         max_ticks=max_ticks)
            print(tick_data)

            # Get all ticks between 9:30AM 5 days ago and 9:30AM today
            # Limit to max_ticks since otherwise too much will be printed on
            # screen
            today = datetime.date.today()
            sdt = today - datetime.timedelta(days=5)
            start_tm = datetime.datetime(year=sdt.year,
                                         month=sdt.month,
                                         day=sdt.day,
                                         hour=9,
                                         minute=30)
            edt = today
            end_tm = datetime.datetime(year=edt.year,
                                       month=edt.month,
                                       day=edt.day,
                                       hour=9,
                                       minute=30)

            tick_data = hist_conn.request_ticks_in_period(ticker=ticker,
                                                          bgn_prd=start_tm,
                                                          end_prd=end_tm,
                                                          max_ticks=max_ticks)
            print(tick_data)
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))


def get_historical_bar_data(ticker: str, bar_len: int, bar_unit: str,
                            num_bars: int):
    """Shows how to get interval bars."""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-historical-bars")
    hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)

    with iq.ConnConnector([hist_conn]) as connector:
        # look at conn.py for request_bars, request_bars_for_days and
        # request_bars_in_period for other ways to specify time periods etc
        try:
            bars = hist_conn.request_bars(ticker=ticker,
                                          interval_len=bar_len,
                                          interval_type=bar_unit,
                                          max_bars=num_bars)
            print(bars)

            today = datetime.date.today()
            start_date = today - datetime.timedelta(days=10)
            start_time = datetime.datetime(year=start_date.year,
                                           month=start_date.month,
                                           day=start_date.day,
                                           hour=0,
                                           minute=0,
                                           second=0)
            end_time = datetime.datetime(year=today.year,
                                         month=today.month,
                                         day=today.day,
                                         hour=23,
                                         minute=59,
                                         second=59)
            bars = hist_conn.request_bars_in_period(ticker=ticker,
                                                    interval_len=bar_len,
                                                    interval_type=bar_unit,
                                                    bgn_prd=start_time,
                                                    end_prd=end_time)
            print(bars)
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))


def get_daily_data(ticker: str, num_days: int):
    """Historical Daily Data"""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-daily-data")
    hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)

    with iq.ConnConnector([hist_conn]) as connector:
        try:
            daily_data = hist_conn.request_daily_data(ticker, num_days)
            print(daily_data)
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))


def get_reference_data():
    """Markets, SecTypes, Trade Conditions etc"""
    table_conn = iq.TableConn(name="pyiqfeed-Example-reference-data")
    table_listener = iq.VerboseIQFeedListener("Reference Data Listener")
    table_conn.add_listener(table_listener)
    with iq.ConnConnector([table_conn]) as connector:
        table_conn.update_tables()
        print("Markets:")
        print(table_conn.get_markets())
        print("")

        print("Security Types:")
        print(table_conn.get_security_types())
        print("")

        print("Trade Conditions:")
        print(table_conn.get_trade_conditions())
        print("")

        print("SIC Codes:")
        print(table_conn.get_sic_codes())
        print("")

        print("NAIC Codes:")
        print(table_conn.get_naic_codes())
        print("")
        table_conn.remove_listener(table_listener)


def get_ticker_lookups(ticker: str):
    """Lookup tickers."""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Ticker-Lookups")
    lookup_listener = iq.VerboseIQFeedListener("TickerLookupListener")
    lookup_conn.add_listener(lookup_listener)

    with iq.ConnConnector([lookup_conn]) as connector:
        syms = lookup_conn.request_symbols_by_filter(
            search_term=ticker, search_field='s')
        print("Symbols with %s in them" % ticker)
        print(syms)
        print("")

        sic_symbols = lookup_conn.request_symbols_by_sic(83)
        print("Symbols in SIC 83:")
        print(sic_symbols)
        print("")

        naic_symbols = lookup_conn.request_symbols_by_naic(10)
        print("Symbols in NAIC 10:")
        print(naic_symbols)
        print("")
        lookup_conn.remove_listener(lookup_listener)


def get_equity_option_chain(ticker: str):
    """Equity Option Chains"""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Eq-Option-Chain")
    lookup_listener = iq.VerboseIQFeedListener("EqOptionListener")
    lookup_conn.add_listener(lookup_listener)
    with iq.ConnConnector([lookup_conn]) as connector:
        # noinspection PyArgumentEqualDefault
        e_opt = lookup_conn.request_equity_option_chain(
            symbol=ticker,
            opt_type='pc',
            month_codes="".join(iq.LookupConn.call_month_letters +
                                iq.LookupConn.put_month_letters),
            near_months=None,
            include_binary=True,
            filt_type=0, filt_val_1=None, filt_val_2=None)
        print("Currently trading options for %s" % ticker)
        print(e_opt)
        lookup_conn.remove_listener(lookup_listener)


def get_futures_chain(ticker: str):
    """Futures chain"""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Futures-Chain")
    lookup_listener = iq.VerboseIQFeedListener("FuturesChainLookupListener")
    lookup_conn.add_listener(lookup_listener)
    with iq.ConnConnector([lookup_conn]) as connector:
        f_syms = lookup_conn.request_futures_chain(
            symbol=ticker,
            month_codes="".join(iq.LookupConn.futures_month_letters),
            years="67",
            near_months=None,
            timeout=None)
        print("Futures symbols with underlying %s" % ticker)
        print(f_syms)
        lookup_conn.remove_listener(lookup_listener)


def get_futures_spread_chain(ticker: str):
    """Futures spread chain"""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Futures-Spread-Lookup")
    lookup_listener = iq.VerboseIQFeedListener("FuturesSpreadLookupListener")
    lookup_conn.add_listener(lookup_listener)
    with iq.ConnConnector([lookup_conn]) as connector:
        f_syms = lookup_conn.request_futures_spread_chain(
            symbol=ticker,
            month_codes="".join(iq.LookupConn.futures_month_letters),
            years="67",
            near_months=None,
            timeout=None)
        print("Futures Spread symbols with underlying %s" % ticker)
        print(f_syms)
        lookup_conn.remove_listener(lookup_listener)


def get_futures_options_chain(ticker: str):
    """Futures Option Chain"""
    lookup_conn = iq.LookupConn(name="pyiqfeed-Example-Futures-Options-Chain")
    lookup_listener = iq.VerboseIQFeedListener("FuturesOptionLookupListener")
    lookup_conn.add_listener(lookup_listener)
    with iq.ConnConnector([lookup_conn]) as connector:
        f_syms = lookup_conn.request_futures_option_chain(
            symbol=ticker,
            month_codes="".join(iq.LookupConn.call_month_letters +
                                iq.LookupConn.put_month_letters),
            years="67",
            near_months=None,
            timeout=None)
        print("Futures Option symbols with underlying %s" % ticker)
        print(f_syms)
        lookup_conn.remove_listener(lookup_listener)


def get_news():
    """Exercise NewsConn functionality"""
    news_conn = iq.NewsConn("pyiqfeed-example-News-Conn")
    news_listener = iq.VerboseIQFeedListener("NewsListener")
    news_conn.add_listener(news_listener)

    with iq.ConnConnector([news_conn]) as connector:
        cfg = news_conn.request_news_config()
        print("News Configuration:")
        print(cfg)
        print("")

        print("Latest 10 headlines:")
        headlines = news_conn.request_news_headlines(
            sources=[], symbols=[], date=None, limit=10)
        print(headlines)
        print("")

        story_id = headlines[0].story_id
        story = news_conn.request_news_story(story_id)
        print("Text of story with story id: %s:" % story_id)
        print(story.story)
        print("")

        today = datetime.date.today()
        week_ago = today - datetime.timedelta(days=7)

        counts = news_conn.request_story_counts(
            symbols=["AAPL", "IBM", "TSLA"],
            bgn_dt=week_ago, end_dt=today)
        print("Number of news stories in last week for AAPL, IBM and TSLA:")
        print(counts)
        print("")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pyiqfeed example code")
    parser.add_argument('-l', action="store_true", dest='level_1',
                        help="Run Level 1 Quotes")
    parser.add_argument('-r', action="store_true", dest='regional_quotes',
                        help="Run Regional Quotes")
    parser.add_argument('-t', action="store_true", dest='trade_updates',
                        help="Run Trades Only Quotes")
    parser.add_argument('-i', action="store_true", dest='interval_data',
                        help="Run interval data")
    parser.add_argument("-a", action='store_true', dest='admin_socket',
                        help="Run Administrative Connection")
    parser.add_argument("-k", action='store_true', dest='historical_tickdata',
                        help="Get historical tickdata")
    parser.add_argument("-b", action='store_true', dest='historical_bars',
                        help="Get historical bar-data")
    parser.add_argument("-d", action='store_true', dest='historical_daily_data',
                        help="Get historical daily data")
    parser.add_argument("-f", action='store_true', dest='reference_data',
                        help="Get reference data")
    parser.add_argument("-c", action='store_true', dest='lookups_and_chains',
                        help="Lookups and Chains")
    parser.add_argument("-n", action='store_true', dest='news',
                        help="News related stuff")
    results = parser.parse_args()

    launch_service()

    if results.level_1:
        get_level_1_quotes_and_trades(ticker="SPY", seconds=30)
    if results.regional_quotes:
        get_regional_quotes(ticker="SPY", seconds=120)
    if results.trade_updates:
        get_trades_only(ticker="SPY", seconds=30)
    if results.interval_data:
        get_live_interval_bars(ticker="SPY", bar_len=5, seconds=30)
    if results.admin_socket:
        get_administrative_messages(seconds=30)
    if results.historical_tickdata:
        get_tickdata(ticker="SPY", max_ticks=100, num_days=4)
    if results.historical_bars:
        get_historical_bar_data(ticker="SPY",
                                bar_len=60,
                                bar_unit='s',
                                num_bars=100)
    if results.historical_daily_data:
        get_daily_data(ticker="SPY", num_days=10)
    if results.reference_data:
        get_reference_data()
    if results.lookups_and_chains:
        get_ticker_lookups("SPH9GBM1")
        get_equity_option_chain("SPY")
        get_futures_chain("@VX")
        get_futures_spread_chain("@VX")
        get_futures_options_chain("CL")
    if results.news:
        get_news()
