# coding=utf-8

"""
Listener classes to listen to messages from IQFeed.

You connect to IQFeed using one of the XXXConn classes. For things
like historical data, when you request the data the data is returned
as the return value of the requesting functions. However when you
request something like real-time tick data, which may come at some
indeterminate time in the future, the connection classes send you
the data by calling callbacks on listeners. In addition even the
connection classes which return data immediately may call callbacks
in listeners for things like a status message telling you the feed
disconnected.

This file contains abstract base classes for a few different types of these
listeners. python's abc mechanism (PEP 3141) was deliberately NOT used since
it doesn't provide any benefit for this use but add a lot of needless
complexity. This being python, duck-typing rules so you don't HAVE to derive
from one of these ABCs.

There is a SilentListener and a VerboseListener version for each listener.
The SilentListeners do nothing, so if you want to handle only one message
type, you could derive from one of these and override one specific function.
The VerboseListeners print a message to stdout every time they get a message
so if you don't override a callback and the callback gets called you will see
output.

"""

from typing import Sequence
import numpy as np
from .conn import FeedConn, AdminConn, QuoteConn


# noinspection PyMethodMayBeStatic
class SilentIQFeedListener:
    """
    Base class for the silent listener classes.

    Silent listeners do nothing with any messages.

    You can also use the base listener for the classes which don't send any
    messages other than admin messages to the listeners like HistoryConn. You
    should do this if you are having issues. For example if you request
    historical data but before your request the feed_is_stale callback was
    called on the listener, you know that there is an issue with connectivity
    between IQFeed.exe and DTN's servers.

    """

    def __init__(self, name: str):
        self._name = name

    def feed_is_stale(self) -> None:
        """Connection between IQFeed.exe and DTN's servers dropped."""
        pass

    def feed_is_fresh(self) -> None:
        """Connection between IQFeed.exe and DTN's servers reconnected."""
        pass

    def feed_has_error(self) -> None:
        """
        Connection between IQFeed.exe and DTN's servers is bad.

        Usually because the reconnection failed.

        """
        pass

    def process_conn_stats(self, stats: FeedConn.ConnStatsMsg) -> None:
        """
        Connection statistics for this connection.

        Fields in namedtuple ConnStatsMsg mean exactly what their names mean.

        """
        pass

    def process_timestamp(self, time_val: FeedConn.TimeStampMsg) -> None:
        """Timestamp when you have requested timestamps."""
        pass

    def process_error(self, fields):
        """Called with an error message"""
        pass


# noinspection PyMethodMayBeStatic
class SilentQuoteListener(SilentIQFeedListener):
    """
    Listens to messages from a QuoteConn class.

    Receives messages related to real-time quotes, trades and news. May also
    receive the messages all other listeners receive.

    """

    def __init__(self, name: str):
        super().__init__(name)

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """
        You made a subscription request with an invalid symbol

        :param bad_symbol: The bad symbol

        """
        pass

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        """
        A news story hit the news wires.

        :param news_item: NewsMsg namedtuple .

           The elements of each HeadlineMsg are:
          'distributor': News Source
          'story_id': ID of the story. Used to get full text
          'symbol_list': Symbols that are affected by the story
          'story_time': When the story went out
          'headline': The story's headline

        If you want the full text, get it using NewsConn using story_id.

        """
        pass

    def process_regional_quote(self, quote: np.array) -> None:
        """
        The top of book at a market-center was updated

        :param quote: numpy structured array with the actual quote

        dtype of quote is QuoteConn.regional_type

        """
        pass

    def process_summary(self, summary: np.array) -> None:
        """
        Initial data after subscription with latest quote, last trade etc.

        :param summary: numpy structured array with the data.

        Fields in each update can be changed by calling
        select_update_fieldnames on the QuoteConn class sending updates.

        The dtype of the array includes all requested fields. It can be
        different for each QuoteConn depending on the last call to
        select_update_fieldnames.

        """
        pass

    def process_update(self, update: np.array) -> None:
        """
        Update with latest quote, last trade etc.

        :param update: numpy structured array with the data.

        Compare with prior cached values to find our what changed. Nothing may
        have changed.

        Fields in each update can be changed by calling
        select_update_fieldnames on the QuoteConn class sending updates.

        The dtype of the array includes all requested fields. It can be
        different for each QuoteConn depending on the last call to
        select_update_fieldnames.

        """
        pass

    def process_fundamentals(self, fund: np.array) -> None:
        """
        Message with information about symbol which does not change.

        :param fund: numpy structured array with the data.

        Despite the word fundamentals used to describe this message in the
        IQFeed docs and the name of this function, you don't get just
        fundamental data. You also get reference date like the expiration date
        of an option.

        Called once when you first subscribe and every time you request a
        refresh.

        """
        pass

    def process_auth_key(self, key: str) -> None:
        """Authorization key: Ignore unless you have a good reason not to."""
        pass

    def process_keyok(self) -> None:
        """Relic from old authorization mechanism. Ignore."""
        pass

    def process_customer_info(self,
                              cust_info: QuoteConn.CustomerInfoMsg) -> None:
        """
        Information about your entitlements etc.

        :param cust_info: The data as a named tuple

        Useful to look at if you are getting delayed data when you expect
        real-time etc.

        """
        pass

    def process_watched_symbols(self, symbols: Sequence[str]) -> None:
        """List of all watched symbols when requested."""
        pass

    def process_log_levels(self, levels: Sequence[str]) -> None:
        """List of current log levels when requested."""
        pass

    def process_symbol_limit_reached(self, sym: str) -> None:
        """
        Subscribed to more than the number of symbols you are authorized for.

        :param sym: The subscription which took you over the limit.

        """
        pass

    def process_ip_addresses_used(self, ip: str) -> None:
        """IP Address used to connect to DTN's servers."""
        pass


# noinspection PyMethodMayBeStatic
class SilentAdminListener(SilentIQFeedListener):
    """
    Receive Administrative messages related to the whole feed.

    If you turn on client statistics, you get a message once a second about
    each connection to IQFeed. Pay particular attention to the kbps_queued
    field in the client statistics message. If data is queuing, you aren't
    reading data fast enough and you are running on stale data.

    """

    def __init__(self, name: str):
        super().__init__(name)

    def process_register_client_app_completed(self) -> None:
        """
        The client app is now registered.

        DTN requires you to register as a developer before writing to their
        API and requires you to use your developer token when an app you wrote
        logs into IQFeed. This is called when DTN servers have accepted your
        developer token.

        """
        pass

    def process_remove_client_app_completed(self) -> None:
        """
        The client app is now de-registered.

        DTN requires you to register as a developer before writing to their
        API and requires you to use your developer token when an app you wrote
        logs into IQFeed. This is called when DTN servers have acknowledged
        that your app is no longer running. Note that an app written by someone
        else could still be running so this is a good idea.

        """
        pass

    def process_current_login(self, login: str) -> None:
        """Current user login processed."""
        pass

    def process_current_password(self, password: str) -> None:
        """Current password processed."""
        pass

    def process_login_info_saved(self) -> None:
        """User's login and password saved."""
        pass

    def process_autoconnect_on(self) -> None:
        """Request to autoconnect processed."""
        pass

    def process_autoconnect_off(self) -> None:
        """Request not to autoconnect processed."""
        pass

    def process_client_stats(self,
                             client_stats: AdminConn.ClientStatsMsg) -> None:
        """
        Message with information about a specific connection.

        :param client_stats: Data in a ClientStatsMsg namedtuple

        Each connection can be named so connections are distinguishable in
        these messages. Pay particular attention to the kb_queued. If you
        aren't reading data fast enough, IQFeed will drop your connection and
        of course you are reacting to stale data.

        """
        pass


# noinspection PyMethodMayBeStatic
class SilentBarListener(SilentIQFeedListener):
    """
    This class listens for updates to real-time interval bar data.

    If you have subscribed to real-time interval bar data, then you will
    receive updates every time a bar-interval ends. When you subscribe you
    can request some historical data to back-fill your data structures. This
    data is is also received via callbacks since otherwise by the time you
    process all the back-fills requested, you may end up missing a live update
    or the interval boundaries may not match up.

    """

    def __init__(self, name: str):
        super().__init__(name)

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        """
        Update to the currently-live bar.

        :param bar_data: The actual bar data as a numpy structured array
            bar_data is a numpy structured array of length 1 of dtype
            BarConn.interval_data_type.

        This function is called IQFeed has an update for the interval data for
        the current live interval. This callback will be called repeatedly every
        time the interval data for the current interval updates until the end
        of the current interval when you get a process_live_bar message. After
        that process_latest_bar_update messages are updates for the next
        interval bar which is the new live bar.

        When you request live bar updates, you can request that latest bar
        updates are sent no more than a certain number of seconds apart. If you
        request updates every 0 seconds, you get an update every time there is
        any change.

        """
        pass

    def process_live_bar(self, bar_data: np.array) -> None:
        """
        Bar update for a complete bar.

        :param bar_data: The actual bar data as a numpy structured array
            bar_data is a numpy structured array of length 1 of dtype
            BarConn.interval_data_type.

        This function is called when the current bar is complete, for example
        if you requested 60 second bars this will be called at minute
        boundaries. After this function is called, further calls to
        process_latest_bar_update are updates to the next bar.

        """
        pass

    def process_history_bar(self, bar_data: np.array) -> None:
        """
        Bar update for a historical bar.

        :param bar_data: The actual bar data as a numpy structured array
            bar_data is a numpy structured array of length 1 of dtype
            BarConn.interval_data_type.

        Called immediately after you request real-time interval data with some
        historical bars if you requested back-fill data in your request

        Called immediately after you request real-time interval data with some
        historical bars if you requested back-fill data in your request.

        """
        pass

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        """
        Bar request with invalid symbol or no authorization for symbol.

        :param bad_symbol: The invalid symbol

        """
        pass

    def process_symbol_limit_reached(self, symbol: str) -> None:
        """
        Bar request would put us over the limit for the number or symbols

        :param symbol: Symbol which went over the limit

        """
        pass

    def process_replaced_previous_watch(self, symbol: str) -> None:
        """
        Previous request for bars overridden by new request.

        :param symbol: Offending symbol

        A request for bar data has resulted in a previous request for bar
        data being cancelled with the current request replacing
        the prior request.

        """
        pass

    def process_watch(self, symbol: str, interval: int, request_id: str):
        """
        One of a list of the symbols you are subscribed.

        :param symbol: The symbol.
        :param interval: Bar interval
        :param request_id: Request ID (blank if none)

        This callback is called once for each subscription when you call the
        request_watches function on a BarConn.

        """
        pass


# noinspection PyMethodMayBeStatic,PyMissingOrEmptyDocstring
class VerboseIQFeedListener:
    """
    Verbose version of SilentIQFeedListener.

    See documentation for SilentIQFeedListener member functions.

    """

    def __init__(self, name: str):
        self._name = name

    def feed_is_stale(self) -> None:
        print("%s: Feed Disconnected" % self._name)

    def feed_is_fresh(self) -> None:
        print("%s: Feed Connected" % self._name)

    def feed_has_error(self) -> None:
        print("%s: Feed Reconnect Failed" % self._name)

    def process_conn_stats(self, stats: FeedConn.ConnStatsMsg) -> None:
        print("%s: Connection Stats:" % self._name)
        print(stats)

    def process_timestamp(self, time_val: FeedConn.TimeStampMsg):
        print("%s: Timestamp:" % self._name)
        print(time_val)

    def process_error(self, fields):
        print("%s: Process Error:" % self._name)
        print(fields)


# noinspection PyMethodMayBeStatic,PyMissingOrEmptyDocstring
class VerboseQuoteListener(VerboseIQFeedListener):
    """
    Verbose version of SilentQuoteListener.

    See documentation for SilentQuoteListener member functions.

    """

    def __init__(self, name: str):
        super().__init__(name)

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        print("%s: Invalid Symbol: %s" % (self._name, bad_symbol))

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        print("%s: News Item Received" % self._name)
        print(news_item)

    def process_regional_quote(self, quote: np.array) -> None:
        print("%s: Regional Quote:" % self._name)
        print(quote)

    def process_summary(self, summary: np.array) -> None:
        print("%s: Data Summary" % self._name)
        print(summary)

    def process_update(self, update: np.array) -> None:
        print("%s: Data Update" % self._name)
        print(update)

    def process_fundamentals(self, fund: np.array) -> None:
        print("%s: Fundamentals Received:" % self._name)
        print(fund)

    def process_auth_key(self, key: str) -> None:
        print("%s: Authorization Key Received: %s" % (self._name, key))

    def process_keyok(self) -> None:
        print("%s: Authorization Key OK" % self._name)

    def process_customer_info(self,
                              cust_info: QuoteConn.CustomerInfoMsg) -> None:
        print("%s: Customer Information:" % self._name)
        print(cust_info)

    def process_watched_symbols(self, symbols: Sequence[str]):
        print("%s: List of subscribed symbols:" % self._name)
        print(symbols)

    def process_log_levels(self, levels: Sequence[str]) -> None:
        print("%s: Active Log levels:" % self._name)
        print(levels)

    def process_symbol_limit_reached(self, sym: str) -> None:
        print("%s: Symbol Limit Reached with subscription to %s" %
              (self._name, sym))

    def process_ip_addresses_used(self, ip: str) -> None:
        print("%s: IP Addresses Used: %s" % (self._name, ip))


# noinspection PyMethodMayBeStatic,PyMissingOrEmptyDocstring
class VerboseAdminListener(VerboseIQFeedListener):
    """
    Verbose version of SilentAdminListener.

    See documentation for SilentAdminListener member functions.

    """

    def __init__(self, name: str):
        super().__init__(name)

    def process_register_client_app_completed(self) -> None:
        print("%s: Register Client App Completed" % self._name)

    def process_remove_client_app_completed(self) -> None:
        print("%s: Remove Client App Completed" % self._name)

    def process_current_login(self, login: str) -> None:
        print("%s: Current Login: %s" % (self._name, login))

    def process_current_password(self, password: str) -> None:
        print("%s: Current Password: %s" % (self._name, password))

    def process_login_info_saved(self) -> None:
        print("%s: Login Info Saved" % self._name)

    def process_autoconnect_on(self) -> None:
        print("%s: Autoconnect On" % self._name)

    def process_autoconnect_off(self) -> None:
        print("%s: Autoconnect Off" % self._name)

    def process_client_stats(self,
                             client_stats: AdminConn.ClientStatsMsg) -> None:
        print("%s: Client Stats:" % self._name)
        print(client_stats)


# noinspection PyMethodMayBeStatic,PyMissingOrEmptyDocstring
class VerboseBarListener(VerboseIQFeedListener):
    """
    Verbose version of SilentBarListener.

    See documentation for SilentBarListener member functions.

    """

    def __init__(self, name: str):
        super().__init__(name)

    def process_latest_bar_update(self, bar_data: np.array) -> None:
        print("%s: Process latest bar update:" % self._name)
        print(bar_data)

    def process_live_bar(self, bar_data: np.array) -> None:
        print("%s: Process live bar:" % self._name)
        print(bar_data)

    def process_history_bar(self, bar_data: np.array) -> None:
        print("%s: Process history bar:" % self._name)
        print(bar_data)

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        print("%s: Invalid Symbol: %s" % (self._name, bad_symbol))

    def process_symbol_limit_reached(self, symbol: str) -> None:
        print("%s: Symbol Limit reached: %s" % (self._name, symbol))

    def process_replaced_previous_watch(self, symbol: str) -> None:
        print("%s: Replaced previous watch: %s" % (self._name, symbol))

    def process_watch(self, symbol: str, interval: int, request_id: str):
        print("%s: Process watch: %s, %d, %s" %
              (self._name, symbol, interval, request_id))
