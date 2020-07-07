# pyiqfeed 

Reads and parses data from IQFeed (http://www.iqfeed.net). Now supports
API version 6.0.

You likely want DTN's API docs handy. They are available at:

https://www.iqfeed.net/dev/api/docs/index.cfm

You need a subscription to IQFeed or this won't work. This library is
usually kept up to date to the current version of IQFeed. The variable
FeedConn.protocol (available if you import pyiqfeed or just look at
conn.py) is the version of the IQFeed protocol currently being used.

Then library depends on Numpy. Much of the data is returned as an numpy
array or numpy structured array. If you are using python for trading
you are probably using numpy in your trading code anyway so this should
not be a big deal. Data that is not numerical market data is returned
as a namedtuple. Different namedtuples are defined for different kinds
of data.

Most of the code is in the file conn.py. There is a class service.py
that can launch the IQFeed service on a windows, OSX or Linux 
machine (so long as IQFeed has been installed using a recent version of
Wine if you aren't on Windows).

If you are installing on OSX, install Wine or Wine-Devel using one of
Homebrew, Macports, or Fink and then install IQFeed inside that. The
Mac Download of IQFeed is basically just a CodeWeavers Wine "bottled"
version of IQFeed, not a native Mac version and because it's been
"bottled", passing arguments to IQFeed at startup is complicated. This
library assumes that you have installed Wine and then IQFeed and are
not using DTN's Mac download. If you choose to use the DTN package,
things will still work but you will have to startup IQFeed yourself and
pass it parameters like the App Name, login and password using messages
from AdminConn, instead of using the command line at startup.

On Ubuntu, use the Wine-Devel from the Wine Development Team's ppa, not
the Wine that comes from Canonical.

If you want to run headless you need to have the package xvfb installed.
IQFeed.exe is launched under wine and wants to create a GUI. If X isn't
running, wine will crash. If you have xvfb, the virtual framebuffer
"fake" X server running, this fools wine into believing it has launched
the IQFeed.exe GUI. This is also useful if you find the IQFeed.exe gui
annoying.

You cannot install IQFeed in a Windows Virtual Machine on the same
physical machine. It won't work unless you subscribe to data while
running within the same virtual machine. DTN does not allow you to get
data on a machine other than the one that IQConnect.exe is running on,
even if it's the same physical machine. IQFeed.exe listens on 127.0.0.1.
So if you absolutely must run it in a virtual machine and run code that
talks to IQFeed on the main machine or vice-versa, there are ways to
make it work. But please **DO NOT** ask me how to do it for you. Hint:
Tunneling or run something on the same machine to forward requests.

For an example of how to use it do the following:

1. Install the package or put it in your search path. python setup.py
works.

2. Create a folder in the same directory as example.py named localconfig and place a file named
passwords.py in it. In this file you need 3 lines:

<pre> <code>
 dtn_product_id = "PRODUCT_ID_FROM_DTN"
 dtn_login="Your_IQFEED_LOGIN"
 dtn_password="Your_IQFEED_PASSWORD"
 </code> </pre>
 
3. Run example.py using something like python3 ./example.py. You must
use python ver >= 3.5.

This exercises many different parts of the library depending on what
options you pass it. The best documentation for the library is just
reading the source. Public functions are documented and the code has
been deliberately kept simple.

It works for me (TM). A diligent effort will be made to squash any bugs
reported to me. Bug reports which do not include a short,
easy-to-use python script that reproduces the bug and an explanation of
the bug will be ignored. Pull requests with bug-fixes and enhancements
are greatly encouraged and appreciated.

For all pull requests please ensure the following:

1. Ensure all code has been run through at least some of the various
python code checkers, preferably pylint. If you have IntelliJ or
PyCharm, Analyze->Inspect Code does something pretty similar. Python is
a duck-typed language. This means many errors can exist in your code
and you'll only find them if your tests actually exercise that part of
the code.  Anywhere near 100% test coverage is basically impossible so
please, please run the checkers. Please do not complain about numpy
related errors from pylint and how pylint won't work with numpy, just
read the pylint docs on how to make it work with external libraries
that call into C.  Ensure all code is PEP8 compliant. The plan is to
put type annotations everywhere and ensure that code passes mypy static
type checks. 

2. Please keep external dependencies to a minimum. Ideally only use
packages that are built into CPython and numpy. Please do not import
other third-party packages, not even pandas, not even in example or
test code.

3. Documentation is good. But gratuitous documentation is bad. The best
 documentation is well chosen names and simple code so you don't have
 to read anything to understand what it does. Comments have to be
 maintained just like other code. No comments is better than comments
 out of sync with the code. If you are tempted to add a description to
 a function that says something like "Private function" or
 "Implementation Detail", consider changing the function's name so it
 starts with an underscore instead and leave it at that.
 
4. Keep your code as simple and straightforward as you can. If you
think you need to describe something tricky in a comment, DON'T. Change
what you are doing so it's not tricky instead. Concise pydocs which
describe how to use functions and interfaces that you expect library
users to use is good.
 
