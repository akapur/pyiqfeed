# pyiqfeed 
Reads data from IQFeed (http://www.iqfeed.net)

Contains classes that can read market data from DTN's IQFeed service. You need
a subscription to IQFeed or this won't work. This library is usually kept up
to date to the current version of IQFeed.

Numpy is a dependency. Much of the data is returned as an numpy array or numpy
structured array. If you are using python for trading you are probably using
numpy in your trading code anyway so this should not be a big deal.

Most of the code is in the file conn.py. There is a class service.py that can
launch the IQFeed service on a windows machine or a mac/unix machine so long as
IQFeed has been installed using a recent version of Wine. You cannong install
IQFeed in a Windows Virtual Machine on the same physical machine, it won't work.

For an example of how to use it do the following

Create a file in the same directory as example.py called passwords.py. In this
file you must define 3 variables:

 dtn_product_id = "PRODUCT_ID_GIVEN_TO_YOU_BY_DTN_WHEN_YOU_SIGNED_UP_FOR_IQFEED"
 dtn_login="Your_IQFEED_LOGIN"
 dtn_password="Your_IQFEED_PASSWORD"

Install the
This exercises many different parts of the library. The best documentation for
the library is just reading the file conn.py.

It works for me in live trading. A diligent effort will be made to squash any
bugs reported to me. Bug reports which do not include a short easy to use python
script that reproduces the bug and an explanation of the bug will be ignored.
Pull requests with bug-fixes and enhancements are greatly encouraged and
appreciated.

