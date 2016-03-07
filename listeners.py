import numpy as np
import time


class SilentIQFeedListener:

    def feed_is_stale(self) -> None:
        pass

    def feed_is_fresh(self) -> None:
        pass

    def feed_has_error(self) -> None:
        pass

    def process_conn_stats(self, stats: dict) -> None:
        pass

    def process_timestamp(self, time_val: time.struct_time) -> None:
        pass


class SilentQuoteListener(SilentIQFeedListener):
    def process_invalid_symbol(self, bad_symbol: str) -> None:
        pass

    def process_news(self, news_item: dict) -> None:
        pass

    def process_regional_quote(self, quote: np.array) -> None:
        pass

    def process_summary(self, summary: np.array) -> None:
        pass

    def process_update(self, update: np.array) -> None:
        pass

    def process_fundamentals(self, fund: np.array) -> None:
        pass

    def process_auth_key(self, key: str) -> None:
        pass

    def process_keyok(self) -> None:
        pass

    def process_customer_info(self, cust_info: dict) -> None:
        pass

    def process_symbol_limit_reached(self, sym: str) -> None:
        pass

    def process_ip_addresses_used(self, ip: str) -> None:
        pass


class SilentAdminListener(SilentIQFeedListener):

    def process_register_client_app_completed(self) -> None:
        pass

    def process_remove_client_app_completed(self) -> None:
        pass

    def process_current_login(self, login: str) -> None:
        pass

    def process_current_password(self, password: str) -> None:
        pass

    def process_login_info_saved(self) -> None:
        pass

    def process_autoconnect_on(self) -> None:
        pass

    def process_autoconnect_off(self) -> None:
        pass

    def process_client_stats(self, client_stats: dict) -> None:
        pass


# noinspection PyMethodMayBeStatic
class VerboseIQFeedListener:

    def feed_is_stale(self) -> None:
        print("Feed Disconnected")

    def feed_is_fresh(self) -> None:
        print("Feed Connected")

    def feed_has_error(self) -> None:
        print("Feed Reconnect Failed")

    def process_conn_stats(self, stats: dict) -> None:
        print("Connection Stats:")
        print(stats)

    def process_timestamp(self, time_val: time.struct_time):
        print("Timestamp:")
        print(time_val)


# noinspection PyMethodMayBeStatic
class VerboseQuoteListener(VerboseIQFeedListener):
    def process_invalid_symbol(self, bad_symbol: str) -> None:
        print("Invalid Symbol: %s" % bad_symbol)

    def process_news(self, news_item: dict) -> None:
        print("News Item Received")
        print(news_item)

    def process_regional_quote(self, quote: np.array) -> None:
        print("Regional Quote:")
        print(quote)

    def process_summary(self, summary: np.array) -> None:
        print("Data Summary")
        print(summary)

    def process_update(self, update: np.array) -> None:
        print("Data Update")
        print(update)

    def process_fundamentals(self, fund: np.array) -> None:
        print("Fundamentals Received:")
        print(fund)

    def process_auth_key(self, key: str) -> None:
        print("Authorization Key Received: %s" % key)

    def process_keyok(self) -> None:
        print("Authorization Key OK")

    def process_customer_info(self, cust_info: dict) -> None:
        print("Customer Information:")
        print(cust_info)

    def process_symbol_limit_reached(self, sym: str) -> None:
        print("Symbol Limit Reached with subscription to %s" % sym)

    def process_ip_addresses_used(self, ip: str) -> None:
        print("IP Addresses Used: %s" % ip)


# noinspection PyMethodMayBeStatic
class VerboseAdminListener(VerboseIQFeedListener):

    def process_register_client_app_completed(self) -> None:
        print("Register Client App Completed")

    def process_remove_client_app_completed(self) -> None:
        print("Remove Client App Completed")

    def process_current_login(self, login: str) -> None:
        print("Current Login: %s" % login)

    def process_current_password(self, password: str) -> None:
        print("Current Password: %s" % password)

    def process_login_info_saved(self) -> None:
        print("Login Info Saved")

    def process_autoconnect_on(self) -> None:
        print("Autoconnect On")

    def process_autoconnect_off(self) -> None:
        print("Autoconnect Off")

    def process_client_stats(self, client_stats: dict) -> None:
        print("Client Stats:")
        print(client_stats)

