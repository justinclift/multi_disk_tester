# DESTRUCTIVE (only!) multi-disk testing for new disks

Similar to badblocks, except writes to multiple disks at once

Use it like:

```
$ sudo ./multi_test.py -d /dev/zvol/rpool/disktests1 -d /dev/zvol/rpool/disktests2
```
