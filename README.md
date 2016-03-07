# pyiqfeed 
Reads data from IQFeed

Contains classes that can read market data from DTN's IQFeed service. You need
a subscription to IQFeed or this won't work. This library is generally kept up
to date to the current version of IQFeed.

Numpy is a dependency. Much of the data is returned as an numpy array or numpy
structured array.

Most of the code is in the file conn.py. There is a class service.py that can
launch the IQFeed service on a windows machine or a mac/unix machine with wine.

For an example of how to use it do the followwing

 1) Create a file in the same directory as conn.py called passwords.py. In this
 file you must define 3 variables:

 dtn_product_id = "PRODUCT_ID_GIVEN_TO_YOU_BY_DTN_WHEN_YOU_SIGNED_UP_FOR_IQFEED"
 dtn_login="Your_IQFEED_LOGIN"
 dtn_password="Your_IQFEED_PASSWORD"


Then just run the file main.py. At the end of the file there is a section scoped with

if __name__ == "__main__":

which exercises various things.

Currently it's pre-alpha. However, it does work for me. I'm paper-trading using
the library and as I move the systems to live-trading, any bugs that impact my
trading code will be fixed. Bug reports, enhancements etc are welcomed.


