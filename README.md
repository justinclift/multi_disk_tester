# DESTRUCTIVE (only!) multi-disk testing for new disks

Similar to badblocks, except writes to multiple disks at once

## Installation

As this is written in Python, getting the dependencies installed is a bit
of a pain:

```
$ git clone https://github.com/justinclift/multi_disk_tester
$ cd multi_disk_tester
$ python3 -m venv stuff
$ . stuff/bin/activate
$ pip3 install poetry
$ poetry install
```


## Usage:

Because the code doesn't handle *sudo* at all, so you'll need to ensure your user
can write to the disk devices you need first:

```
$ sudo chmod o+rw /dev/zvol/rpool/disktests1 /dev/zvol/rpool/disktests2
$ ./multi_test.py -d /dev/zvol/rpool/disktests1 -d /dev/zvol/rpool/disktests2
```
