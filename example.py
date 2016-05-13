if __name__ == "__main__":
    from pyiqfeed.service import FeedService
    from pyiqfeed.listeners import VerboseIQFeedListener, VerboseQuoteListener, VerboseAdminListener
    from pyiqfeed.passwords import dtn_login, dtn_password, dtn_product_id
    from pyiqfeed.conn import AdminConn, QuoteConn, HistoryConn, LookupConn, TableConn
    import time
    import datetime

    svc = FeedService(product=dtn_product_id, version="Debugging",
                      login=dtn_login, password=dtn_password,
                      autoconnect=True, savelogininfo=True)
    svc.launch()

    admin_conn = AdminConn(name="RunningInIde")
    admin_listener = VerboseAdminListener("AdminListener")
    admin_conn.add_listener(admin_listener)
    admin_conn.start_runner()
    admin_conn.set_admin_variables_from_dict(svc.admin_variables())
    admin_conn.client_stats_on()

    quote_conn = QuoteConn(name="RunningInIDE")
    quote_listener = VerboseQuoteListener("QuoteListener")
    quote_conn.add_listener(quote_listener)
    quote_conn.start_runner()

    quote_conn.request_all_update_fieldnames()
    quote_conn.request_current_update_fieldnames()
    quote_conn.request_fundamental_fieldnames()
    all_fields = sorted(list(QuoteConn.quote_msg_map.keys()))
    quote_conn.select_update_fieldnames(all_fields)
    quote_conn.watch("SPY")
    time.sleep(10)

    hist_conn = HistoryConn(name="RunningInIde")
    hist_listener = VerboseIQFeedListener("HistListener")
    hist_conn.add_listener(hist_listener)
    hist_conn.start_runner()

    ticks = hist_conn.request_ticks("INTC", 10)
    print(ticks)

    ticks = hist_conn.request_ticks_for_days(
        "IBM", 365)
    print(ticks)

    today = datetime.date.today()
    sdt = today - datetime.timedelta(days=5)
    edt = today

    start_tm = datetime.datetime(year=sdt.year, month=sdt.month, day=sdt.day, hour=9, minute=30)
    end_tm = datetime.datetime(year=edt.year, month=edt.month, day=edt.day, hour=9, minute=30)

    ticks = hist_conn.request_ticks_in_period(
        "INTC",
        start_tm,
        end_tm,
        max_ticks=100)
    print(ticks)

    bars = hist_conn.request_bars("INTC", 60, 's', 10)
    print(bars)

    bars = hist_conn.request_bars_for_days(
        "INTC", 60, 's', 365)
    print(bars)

    bars = hist_conn.request_bars_in_period(
        "INTC", 60, 's',
        start_tm,
        end_tm,
        max_bars=100)
    print(bars)

    daily = hist_conn.request_daily_data("@VXH16", 10)
    print(daily)

    daily = hist_conn.request_daily_data_for_dates(
        "INTC", datetime.date(2016, 1, 1), datetime.date(2016, 3, 4))
    print(daily)

    weekly = hist_conn.request_weekly_data("INTC", 10)
    print(weekly)

    monthly = hist_conn.request_monthly_data("INTC", 12)
    print(monthly)

    table_conn = TableConn(name="RunningInIDE")
    table_listener = VerboseIQFeedListener("TableListener")
    table_conn.add_listener(table_listener)
    table_conn.update_tables()
    print(table_conn.get_markets())
    print(table_conn.get_security_types())
    print(table_conn.get_trade_conditions())
    print(table_conn.get_sic_codes())
    print(table_conn.get_naic_codes())

    lookup_conn = LookupConn(name="RunningInIDE")
    lookup_listener = VerboseIQFeedListener("LookupListener")
    lookup_conn.add_listener(lookup_listener)
    lookup_conn.start_runner()

    tesla_syms = lookup_conn.request_symbols_by_filter(
        search_term='TSLA', search_field='s')
    print(tesla_syms)

    sic_symbols = lookup_conn.request_symbols_by_sic(83)
    print(sic_symbols)

    naic_symbols = lookup_conn.request_symbols_by_naic(10)
    print(naic_symbols)
    #
    f_syms = lookup_conn.request_futures_chain(
        symbol="@VX",
        month_codes="".join(LookupConn.futures_month_letters),
        years="67",
        near_months=None,
        timeout=None)
    print(f_syms)

    f_spread = lookup_conn.request_futures_spread_chain(
        symbol="@VX",
        month_codes="".join(LookupConn.futures_month_letters),
        years="67",
        near_months=None,
        timeout=None)
    print(f_spread)
    #
    f_opt = lookup_conn.request_futures_option_chain(
        symbol="CL",
        opt_type='pc',
        month_codes="".join(LookupConn.futures_month_letters),
        years="67",
        near_months=None,
        timeout=None)
    print(f_opt)

    e_opt = lookup_conn.request_equity_option_chain(
        symbol="INTC",
        opt_type='pc',
        month_codes="".join(LookupConn.equity_call_month_letters +
                            LookupConn.equity_put_month_letters),
        near_months=None,
        include_binary=True,
        filt_type=0, filt_val_1=None, filt_val_2=None,
        timeout=None)
    print(e_opt)

    time.sleep(20)
    admin_conn.client_stats_off()
    quote_conn.unwatch("SPY")
    print("Unwatched")
    time.sleep(3)

    lookup_conn.stop_runner()
    quote_conn.stop_runner()
    hist_conn.stop_runner()
    admin_conn.stop_runner()
