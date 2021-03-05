# HelloDB

A hello kitty implementation of a NoSQL (and no frills) database server based on the [WiredTiger 3.2.1](http://source.wiredtiger.com/) storage engine. Currently supports up to 7 fields per table :joy:

## Installation

Download, compile, and install the [WiredTiger 3.2.1 binaries](http://source.wiredtiger.com/releases/wiredtiger-3.2.1.tar.bz2). For convenience, here is a summary of their [instructions](http://source.wiredtiger.com/3.2.1/build-posix.html):
```
cd wiredtiger[-3.2.1]
./configure
make
make install
```
You should now have `/usr/local/lib/libwiredtiger.so`, `/usr/local/include/wiredtiger.h`, and `/usr/local/bin/wt`.

Clone or download `hellodb.cpp`.

Compile the `hellodb` binary:
```
g++ -o /path/to/hellodb /path/to/hellodb.cpp -lwiredtiger -lpthread
```

## Running the server

Create a text file to define your table and index meta data:
```
#<table>
<field>
<field>
<field>

!<table>:<index>
<field>
<field>

#<table>
<field> <field> <field> <field>

!<table>:<index>
<field> <field>
```
For example, `meta.cfg`:
```
#customers
name age location

!customers:justname
name

!customers:location_age
location age

#transactions
item
price
date
customer

!transactions:dateAndItem
date
item

!transactions:itemAndCustomer
item
customer
```


Start the database server (in the background with log output):
```
/path/to/hellodb /path/to/meta.cfg /path/to/db/folder [port default is 27000] &>/path/to/hellodb.log &
```

## Usage

Sorry, no client yet (but there is a [bulk loader](https://github.com/mrderive/GoToDB)). You can use TCP tools such as `nc` or `telnet`. For convenience, you can set up a `bash` function like so:
```
hello() {
    echo $1 | nc -w1 localhost 27000
}
````

### insert

```
$ hello insert]customers]Joe]28]Boston

inserted>
recno: 1

$ hello 'insert]customers]Bob]35]New York'

inserted>
recno: 2

$ hello 'insert]customers]]35]New York'

inserted>
recno: 3

$ hello insert]customers]Susan]35

inserted>
recno: 4
```
Note: Single quotes are not necessary if you are using `echo` or `nc` interactively instead of via `bash` function.

### at

```
$ hello at]customers]1

1>
name: Joe
age: 28
location: Boston

$ hello at]customers]2

2>
name: Bob
age: 35
location: New York

$ hello at]customers]3

3>
name:
age: 35
location: New York

$ hello at]customers]4

4>
name: Susan
age: 35
location:
```

### find

```
$ hello find]customers]justname]Bob

2>
name: Bob
age: 35
location: New York

$ hello 'find]customers]location_age]New York]35'

2>
name: Bob
age: 35
location: New York

3>
name:
age: 35
location: New York
```

### dump

```
$ hello dump]customers

1>
name: Joe
age: 28
location: Boston

2>
name: Bob
age: 35
location: New York

3>
name:
age: 35
location: New York

4>
name: Susan
age: 35
location:
```

### update

```
$ hello update]customers]3]Jack

updated>
recno: 3

$ hello at]customers]3

3>
name: Jack
age: 35
location: New York

$ hello update]customers]4]Susan]35]Chicago

updated>
recno: 4

$ hello at]customers]4

4>
name: Susan
age: 35
location: Chicago
```

### delete

```
$ hello delete]customers]3

deleted>
recno: 3

$ hello at]customers]3

Not found

$ hello find]customers]justname]Jack

Not found
```

### shutdown

```
$ hello shutdown

$ ps -ef | grep hellodb
[1]+  Done hellodb 
```

### exit

If you are using an interactive session via `nc` or `telnet` directly:
```
$ nc localhost 27000
find]customers]location_age]New York]35

2>
name: Bob
age: 35
location: New York

exit

$ 
```
