#!python3.5

if __name__ == "__main__":
    from pyiqfeed.service import FeedService
    from pyiqfeed.listeners import VerboseIQFeedListener, VerboseQuoteListener, VerboseAdminListener, VerboseBarListener
    from pyiqfeed.listeners import SilentIQFeedListener, SilentQuoteListener, SilentAdminListener, SilentBarListener
    from pyiqfeed.passwords import dtn_login, dtn_password, dtn_product_id
    from pyiqfeed.conn import AdminConn, QuoteConn, HistoryConn, LookupConn, TableConn,  NewsConn, BarConn
    from pprint import pprint
    import datetime
    import time

    svc = FeedService(product=dtn_product_id, version="Debugging",
                      login=dtn_login, password=dtn_password,
                      autoconnect=True, savelogininfo=True)
    svc.launch()

    admin_conn = AdminConn(name="RunningInIde")
    #admin_listener = VerboseAdminListener("AdminListener")
    admin_listener = SilentAdminListener()
    admin_conn.add_listener(admin_listener)
    admin_conn.start_runner()
    admin_conn.set_admin_variables_from_dict(svc.admin_variables())
    admin_conn.client_stats_on()

    quote_conn = QuoteConn(name="RunningInIDE")
    quote_listener = VerboseQuoteListener("QuoteListener")
    #quote_listener = SilentQuoteListener()
    quote_conn.add_listener(quote_listener)
    quote_conn.start_runner()

    quote_conn.request_all_update_fieldnames()
    quote_conn.request_current_update_fieldnames()
    quote_conn.request_fundamental_fieldnames()
    all_fields = sorted(list(QuoteConn.quote_msg_map.keys()))
    quote_conn.select_update_fieldnames(all_fields)
    quote_conn.watch("SPY")

    # turn on real-time news for watched symbols
    # see if there is any news streaming about SPY
    quote_conn.news_on()

    time.sleep(5)

    hist_conn = HistoryConn(name="RunningInIde")
    hist_listener = VerboseIQFeedListener("HistListener")
    #hist_listener = SilentIQFeedListener()
    hist_conn.add_listener(hist_listener)
    hist_conn.start_runner()

    ticks = hist_conn.request_ticks("INTC", 10)
    print("\nINTC ticks:\n", ticks[0].dtype.names, "\n", ticks, "\n")

    ticks = hist_conn.request_ticks_for_days("IBM", 365)
    print("\nIBM ticks for days:\n", ticks[0].dtype.names, "\n", ticks, "\n")

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
    print("\nINTC ticks in period:\n", ticks[0].dtype.names, "\n", ticks, "\n")

    bars = hist_conn.request_bars("INTC", 60, 's', 10)
    print("\nINTC bars:\n", bars[0].dtype.names, "\n", bars, "\n")

    bars = hist_conn.request_bars_for_days(
        "INTC", 60, 's', 365)
    print("\nINTC bars for days:\n", bars[0].dtype.names, "\n", bars, "\n")

    bars = hist_conn.request_bars_in_period(
        "INTC", 60, 's',
        start_tm,
        end_tm,
        max_bars=100)
    print("\nINTC bars in period:\n", bars[0].dtype.names, "\n", bars, "\n")

    daily = hist_conn.request_daily_data("@VXH16", 10)
    print("\nVXH16 historical daily data:\n", daily[0].dtype.names, "\n", daily, "\n")

    daily = hist_conn.request_daily_data_for_dates(
        "INTC", datetime.date(2016, 1, 1), datetime.date(2016, 3, 4))
    print("\nINTC historical daily data for dates:\n", daily[0].dtype.names, "\n", daily, "\n")

    weekly = hist_conn.request_weekly_data("INTC", 10)
    print("\nINTC historical weekly data:\n", weekly[0].dtype.names, "\n", weekly, "\n")

    monthly = hist_conn.request_monthly_data("INTC", 12)
    print("\nINTC historical monthly data:\n", monthly[0].dtype.names, "\n", monthly, "\n")

    table_conn = TableConn(name="RunningInIDE")
    table_listener = VerboseIQFeedListener("TableListener")
    table_conn.add_listener(table_listener)
    table_conn.update_tables()
    print("\nTableConn: get_markets:\n", table_conn.get_markets(), "\n")
    print("\nTableConn: get_security_types:\n", table_conn.get_security_types(), "\n")
    print("\nTableConn: get_trade_conditions:\n", table_conn.get_trade_conditions(), "\n")
    print("\nTableConn: get_sic_codes:\n", table_conn.get_sic_codes(), "\n")
    print("\nTableConn: get_naic_codes:\n", table_conn.get_naic_codes(), "\n")

    lookup_conn = LookupConn(name="RunningInIDE")
    lookup_listener = VerboseIQFeedListener("LookupListener")
    lookup_conn.add_listener(lookup_listener)
    lookup_conn.start_runner()

    tesla_syms = lookup_conn.request_symbols_by_filter(
        search_term='TSLA', search_field='s')
    print("\nTSLA request symbols by search filter:\n", tesla_syms, "\n")

    sic_symbols = lookup_conn.request_symbols_by_sic(83)
    print("\nRequest symbols by SIC:\n", sic_symbols, "\n")

    naic_symbols = lookup_conn.request_symbols_by_naic(10)
    print("\nRequest symbols by NAIC:\n", naic_symbols, "\n")



    f_syms = lookup_conn.request_futures_chain(
        symbol="@VX",
        month_codes="".join(LookupConn.futures_month_letters),
        years="67",
        near_months=None,
        timeout=None)
    print("\n@VX Request futures chain:\n", f_syms, "\n")

    f_spread = lookup_conn.request_futures_spread_chain(
        symbol="@VX",
        month_codes="".join(LookupConn.futures_month_letters),
        years="67",
        near_months=None,
        timeout=None)
    print("\n@VX request futures spread:\n", f_spread, "\n")
    #
    f_opt = lookup_conn.request_futures_option_chain(
        symbol="CL",
        opt_type='pc',
        month_codes="".join(LookupConn.futures_month_letters),
        years="67",
        near_months=None,
        timeout=None)
    print("\nCL request futures option chain:\n", f_opt, "\n")

    e_opt = lookup_conn.request_equity_option_chain(
        symbol="INTC",
        opt_type='pc',
        month_codes="".join(LookupConn.equity_call_month_letters +
                            LookupConn.equity_put_month_letters),
        near_months=None,
        include_binary=True,
        filt_type=0, filt_val_1=None, filt_val_2=None,
        timeout=None)
    print("\nINTC request equity option chain:\n", e_opt, "\n")


    ############

    #
    # Lets get tick data for some derivatives
    # all symbols here is too many for an example
    # so let's only get data for 10 of each:
    if( len(f_syms) > 10 ):
        f_syms = f_syms[0:10]
    if( len(f_spread) > 10 ):
        f_spread = f_spread[0:10]
    if( len(e_opt['p']) > 10 ):
        e_opt['p'] = e_opt['p'][0:10]
    if( len(e_opt['c']) > 10 ):
        e_opt['c'] = e_opt['c'][0:10]

    derivatives = f_syms + f_spread + e_opt['p'] + e_opt['c']

    #
    # Get a bunch of derivatives tick data:
    for deriv in derivatives:
        try:
            ticks = hist_conn.request_ticks(deriv, 10)
            print("\n", deriv," DERIVATIVE TICKS!!\n", ticks[0].dtype.names, "\n", ticks, "\n")
        except Exception as err:
            if '!NO_DATA!' in str(err):
                # exception was causing code to halt
                pass
            else:
                print("\n",err,"\n")  # subscription error?
                break


    #
    # Let's test the BarConn class:
    #
    bar_conn = BarConn(name="RunningInIDE")
    bar_listener = VerboseBarListener("BarListener")
    bar_conn.add_listener(bar_listener)
    bar_conn.start_runner()

    # if( len(f_opt['c']) > 10 ):
    #     f_opt['c'] = f_opt['c'][0:10]
    # if( len(f_opt['p']) > 10 ):
    #     f_opt['p'] = f_opt['p'][0:10]
    #derivatives = derivatives + f_opt['p'] + f_opt['c']

    for deriv in derivatives:
        # request bars in invervals of every 60 seconds:
        bar_conn.request_interval_bar_watch(  symbol=deriv ,
                                                interval=60,
                                                bgn_prd=start_tm,
                                                max_days_data=50,
                                                interval_type='s' )

    time.sleep(3)
    bar_conn.unwatch_all()
    bar_conn.stop_runner()
    time.sleep(1)

    #
    # Let's test the NewsConn API and get some news:
    #
    news_conn = NewsConn(name="RunningInIDE")
    news_listener = VerboseIQFeedListener("LookupListener")
    news_conn.add_listener(news_listener)
    news_conn.start_runner()

    #
    # News Configuration request
    #
    config = news_conn.request_news_config()
    print("\nCLIENT NEWS CONFIG:")
    pprint(config, width=60)
    all_sources = [n['type'] for n in config]
    print( "Subscribed news sources:  ", all_sources, "\n" )

    #
    # News Headlines request:
    # Get only headlines from these sources and only about these companies:
    #
    srcs ="AP:DTN:CPR:CBW:RTT:MNT:MW:CPZ:CIW"
    companies='INTC:AMZN:FB'
    headlines = news_conn.request_news_headlines( sources=srcs, symbols=companies)

    # easy to make news headlines into time sequence data frame:
    try:
        import pandas as pd
        headlines_dataframe = pd.DataFrame(headlines).set_index('timestamp')
        headlines_dataframe.sort_index(inplace=True)
        print("\n\nNEWS HEADLINES PANDAS DATAFRAME:\n",headlines_dataframe, "\n\n" )
    except Exception as err:
        print(err) # maybe pandas isn't installed on your machine, no other code depends on it, so...


    print("\n\nNEWS HEADLINES DATA:\n")
    pprint(headlines, width=120)

    #
    # News Story request:
    # Get story content body per headline id:
    #
    counter=0
    for headline in headlines:
        story = news_conn.request_news_story(story_id= headline['id'] )
        print("\nNEWS STORY CONTENT:\n", story, "\n")
        counter += 1
        if counter == 3: break

    #
    # News Story Count Request:
    # Get number of news stories per company within a given date range:
    # returns a dictionary where TICKER is key and NUM_STORIES is value
    #
    today = datetime.date.today()
    five_days_ago = today - datetime.timedelta(days=5)
    companies = 'AAPL:TSLA:INTC:AMZN:FB:TWTR'
    story_counts = news_conn.request_story_counts(symbols=companies,
                                                  bgn_dt=five_days_ago,
                                                  end_dt=today )
    print( "\nNEWS STORY COUNTS:", story_counts, "\n")


    quote_conn.news_off()
    admin_conn.client_stats_off()
    quote_conn.unwatch("SPY")

    print("Unwatched")
    time.sleep(1)


    lookup_conn.stop_runner()
    quote_conn.stop_runner()
    hist_conn.stop_runner()
    news_conn.stop_runner()
    admin_conn.stop_runner()
