# DESTRUCTIVE (only!) multi-disk testing for new disks

Similar to badblocks, except writes to multiple disks at once

![Multi Disk Tester Screenshot](https://github.com/justinclift/multi_disk_tester/raw/master/pics/2024.08.19-screenshot_v0.0.1.png "Multi Disk Tester Screenshot")

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

Because the code doesn't handle *sudo* at all, you'll need to ensure your user
has permission to write to the disk devices first:

```
$ sudo chmod o+rw /dev/zvol/rpool/disktests1 /dev/zvol/rpool/disktests2
$ ./multi_test.py -d /dev/zvol/rpool/disktests1 -d /dev/zvol/rpool/disktests2
```
