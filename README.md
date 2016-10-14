# pyiqfeed 

Reads and parses data from IQFeed (http://www.iqfeed.net).

If you are considering using this library, it's probably a good idea
to have DTN's API docs handy. They are available at:

https://www.iqfeed.net/dev/api/docs/index.cfm

You need a subscription to IQFeed or this won't work. This library is
usually kept up to date to the current version of IQFeed. The variable
FeedConn.protocol is the version of the IQFeed protocol currently being
used.

Numpy is a dependency. Much of the data is returned as an numpy array
or numpy structured array. If you are using python for trading you are
probably using numpy in your trading code anyway so this should not be
a big deal. Data that is not numerical, or data like Connection
Statistics Data that tells you how the feed is doing but isn't something
you are likely to do number crunching on is returned as a namedtuple.
Different namedtuples are defined for different kinds of data.

Most of the code is in the file conn.py. There is a class service.py
that can launch the IQFeed service on a windows, OSX or Linux 
machine so long as IQFeed has been installed using a recent version of
Wine.

You cannot install IQFeed in a Windows Virtual Machine on the same
physical machine, it won't work. DTN does not allow you to get data on a
machine other than the one that IQConnect.exe is running on, even if it's
the same physical machine that is running a virtual machine inside which you
are running IQConnect.exe

If you are installing on OSX, install Wine or Wine-Devel using one of
Homebrew, Macports, or Fink and then install IQFeed inside that. The Mac
Download of IQFeed is basically just a CodeWeavers Wine "bottled" version
of IQFeed, not a native Mac version and because it's been "bottled", passing
arguments to IQFeed at startup is "complicated". This library assumes that
you have installed Wine and then IQFeed and are NOT using DTN's Mac download.
If you still choose to use the DTN package, things will still work but you
will have to startup IQFeed yourself and pass it parameters like the App Name,
login and password using messages, instead of using the command line at
startup.

On Ubuntu, use the Wine-Devel from the Wine Development Team's ppa, not the
Wine that comes from Canonical.

For an example of how to use it do the following:

1. Install the package. python setup.py works.

2. Create a file in the same directory as example.py called passwords.py.
In this file you need 3 lines:

<pre> <code>
 dtn_product_id = "PRODUCT_ID_GIVEN_TO_YOU_BY_DTN_WHEN_YOU_SIGNED_UP_FOR_IQFEED"
 dtn_login="Your_IQFEED_LOGIN"
 dtn_password="Your_IQFEED_PASSWORD"
 </code> </pre>
 
3. Run example.py using something like python3 ./example.py. You must use
python 3.5.

This exercises many different parts of the library depending on what options
you pass it. The best documentation for the library is just reading the file
conn.py and example.py and viewing the output from running example.py. It's
really not a lot of code and you should probably actually read it really
carefully and then test it extensively yourself before you use it, especially
if you are going to commit any money based on something that uses this library.

It works for me in live trading. A diligent effort will be made to
squash any bugs reported to me. Bug reports which do not include a short,
easy-to-use python script that reproduces the bug and an explanation of
the bug will be ignored. Pull requests with bug-fixes and enhancements
are greatly encouraged and appreciated.

For all pull requests please ensure the following:

1. Ensure all code is compliant with PEP8.

2. Ensure all code has been run through at least some of the various python
 code checkers, preferably pylint. If you have IntelliJ or PyCharm, 
 Analyze->Inspect Code does something pretty similar. Python is a duck-typed
 language. This means many errors can exist in your code and you'll only
 find them if your tests actually exercise that part of the code.  Anywhere
 near 100% test coverage is basically impossible so please, please run the
 checkers.

3. Please keep external dependencies to a minimum. Ideally only use packages
 that are built into CPython and numpy. Please do not import other third-party
 packages, not even pandas, not even in example or test code.

4. Documentation is good. But gratuitous documentation is bad. The best
 documentation is well chosen names and simple code so you don't have to read
 anything to understand what it does. Remember comments also have to be
 maintained and the worst kind of comments are those that out of sync with the
 code. So if you are tempted to add a description to a function that says
 something like "Private function" or "Implementation Detail", consider
 changing the function's name so it starts with an underscore instead and
 leave it at that.
 
 5. Keep your code as simple and straightforward as you can. This library has
 a really simple function and is going is used by people in a context where
 errors are VERY EXPENSIVE so if you think you need to describe something
 tricky in a comment, DON'T. Change what you are doing so it's not tricky
 instead. That having been said concise pydocs which describe how to use
 functions and interfaces that you expect users to use is good. Efficiency
 is important but nobody is going to use either IQFeed or Python in a latency
 sensitive context so don't do anything tricky just to get an extra 1%.
 
This code is provided for entertainment purposes only.  If you use it and
bankrupt yourself or other bad stuff happens, neither I nor any other
contributor to this library is responsible and we disclaim all liability
and/or any warranty including any implied warranty.

The code is licensed under the GPL v2 (see file LICENSE.md for details), which
means that as a condition of being allowed to use this code, you are agreeing
that if you use this code, it could do absolutely anything, upto and including
initiating global thermonuclear war and if you use this code and that actually
happens, you are liable, not me, any other contributor or anybody else. If you
don't know what you are doing, don't use this library.
