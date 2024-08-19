#!/usr/bin/env python3

import mmap
import os
import subprocess
import argparse
import pathlib
import time
from multiprocessing import Pool, Array

from rich.progress import Progress

CMD_BLOCKDEV="/usr/sbin/blockdev"
DEBUG = False

DEVICE_LIST = []
DEVICE_PROGRESS = Array('I', [])
DEVICE_STATUS = Array('B', [])
TASK_LIST = Array('I', [])


def test_disk(device):
    global DEBUG, DEVICE_LIST, DEVICE_PROGRESS, DEVICE_STATUS

    # Determine which DEVICE_LIST element number we've been passed
    list_element = 0
    found = False
    for element in DEVICE_LIST:
        if element == device:
            found = True
            break
        else:
            list_element += 1

    if found is False:
        print(f"{device} not found in DEVICE_LIST.  Aborting.")
        return

    # Get the total size of the device
    try:
        device_size = int(subprocess.check_output([CMD_BLOCKDEV, "--getsize64", device]).strip())
    except subprocess.CalledProcessError:
        print(f"Couldn't run blockdev on {device}.  Aborting!")
        return False
    except Exception as e:
        return False

    if DEBUG:
        print(f"Total size of '{device}': {round(device_size / (1024 * 1024))} MB")

    # Get block size to use for the transfers
    try:
        # 'blockdev --getss' gets logical block size, and 'blockdev --getpbsz' gets physical block size.  Not sure which
        # one is better for this program
        physical_block_size = int(subprocess.check_output([CMD_BLOCKDEV, "--getpbsz", device]).strip())
    except:
        print("Something went wrong")
        return False

    if DEBUG:
        print(f"Physical block size of device: {physical_block_size} bytes")

    num_blocks_in_device = round(device_size / physical_block_size)

    # Open the device for read-write operations
    try:
        # To add Windows support, apparently "os.O_BINARY" would need to be added
        device_handle = os.open(path=device, flags=os.O_DIRECT | os.O_RDWR)
    except FileNotFoundError:
        print(f"Can't access device '{device}'. Permissions problem, or wrong device name maybe?")
        return False
    except PermissionError:
        print(f"Trying to open '{device}' for writing failed. Did you forget sudo?")
        return False
    file_handle = os.fdopen(fd=device_handle, mode="rb+", buffering=0)

    # Test the device with various characters
    verify_succeeded = True
    for test_byte in [bytearray.fromhex('aa'), bytearray.fromhex('55'),
                      bytearray.fromhex('ff'), bytearray.fromhex('00')]:

        # Create a byte array of the character being tested
        test_array = bytearray()
        for i in range(physical_block_size):
            test_array += test_byte

        # Write to the device
        write_status = False
        try:
            write_status = write_disk(list_element, file_handle, physical_block_size, num_blocks_in_device, test_array, test_byte)
        except Exception as e:
            break
        if not write_status:
            print(f"Writing {test_byte} to {device} failed!")

        # Verify writing to the device worked
        # TODO: This feels like a dodgy way to check the verification worked?
        verify_status = False
        try:
            verify_status = verify_disk(list_element, file_handle, physical_block_size, num_blocks_in_device, test_array, test_byte)
        except:
            print(f"Reading from '{device}' failed!")
            verify_succeeded = False
            break
        if not verify_status:
            verify_succeeded = False
            print(f"Verifying {test_byte} on {device} failed!")

    # Close the device
    file_handle.close()
    if verify_succeeded:
        DEVICE_STATUS[list_element] = 30  # Disk testing was successful
    else:
        DEVICE_STATUS[list_element] = 20  # Disk testing failed
    return True


def write_disk(list_element, file_to_be_read, physical_block_size, num_blocks_in_device, array_to_write, test_byte):
    global DEVICE_LIST, DEVICE_PROGRESS, DEVICE_STATUS
    if test_byte == bytearray.fromhex('aa'):
        DEVICE_STATUS[list_element] = 1
    elif test_byte == bytearray.fromhex('55'):
        DEVICE_STATUS[list_element] = 3
    elif test_byte == bytearray.fromhex('ff'):
        DEVICE_STATUS[list_element] = 5
    elif test_byte == bytearray.fromhex('00'):
        DEVICE_STATUS[list_element] = 7

    # Set up progress counter output
    progress_counter_blocks = round(num_blocks_in_device / 100)
    DEVICE_PROGRESS[list_element] = 0

    try:
        for block_number in range(num_blocks_in_device):
            seek_position = block_number * physical_block_size

            # Update the reported progress percentage
            if block_number % progress_counter_blocks == 0:
                DEVICE_PROGRESS[list_element] += 1

            # Write the test byte to disk
            m = mmap.mmap(fileno=file_to_be_read.fileno(), length=physical_block_size, offset=seek_position)
            m.write(array_to_write)
            m.close()
    except Exception as e:
        # TODO: Better reporting of write failures
        return False

    return True


def verify_disk(list_element, file_to_be_read, physical_block_size, num_blocks_in_device, comparison_array,
                expected_byte):
    global DEVICE_LIST, DEVICE_PROGRESS, DEVICE_STATUS
    if expected_byte == bytearray.fromhex('aa'):
        DEVICE_STATUS[list_element] = 2
    elif expected_byte == bytearray.fromhex('55'):
        DEVICE_STATUS[list_element] = 4
    elif expected_byte == bytearray.fromhex('ff'):
        DEVICE_STATUS[list_element] = 6
    elif expected_byte == bytearray.fromhex('00'):
        DEVICE_STATUS[list_element] = 8

    # Set up progress counter output
    progress_counter_blocks = round(num_blocks_in_device / 100)
    DEVICE_PROGRESS[list_element] = 0

    for block_number in range(num_blocks_in_device):
        seek_position = block_number * physical_block_size

        # Update the reported progress percentage
        if block_number % progress_counter_blocks == 0:
            DEVICE_PROGRESS[list_element] += 1

        # Read the test byte from disk
        m = mmap.mmap(fileno=file_to_be_read.fileno(), length=physical_block_size, offset=seek_position)
        read_buffer = m.read(physical_block_size)
        if read_buffer != comparison_array:
            # TODO: Better reporting of verification failure(s)
            return False
        m.close()
    return True


def main():
    global DEVICE_LIST, DEVICE_PROGRESS, DEVICE_STATUS, TASK_LIST

    # Output program info
    print(f"Destructive disk testing utility v0.0.1")

    # Get the device name(s) to test
    parser = argparse.ArgumentParser(description="Badblocks, but in Python and able"
                                                 " to test multiple devices simultaneously")
    parser.add_argument("-d", "--devices", help="device to test (multiple occurrences is allowed)", type=pathlib.Path,
                        action="append", required=True)
    args = parser.parse_args()

    with Progress() as progress:
        # Size the progress information arrays appropriately
        num_drives = len(args.devices)
        DEVICE_PROGRESS = Array('I', range(num_drives))
        DEVICE_STATUS = Array('B', range(num_drives))
        TASK_LIST = Array('I', range(num_drives))

        cnt = 0
        for device in args.devices:
            DEVICE_LIST.append(device)
            friendly_name = str(device).split("/")[-1:][0]
            TASK_LIST[cnt] = progress.add_task(f"[cyan]{friendly_name} writing...", total=100)
            cnt += 1

        # Launch the background drive read/verify tasks
        with Pool(processes=len(DEVICE_LIST)) as pool:
            pool.imap(test_disk, DEVICE_LIST)

            # This main process just reports the results until the tasks are finished
            finished = False
            while not finished:
                time.sleep(0.1)

                maybe_finished = True
                for idx, task in enumerate(TASK_LIST):

                    # Determine the friendly name for the current task's device
                    device = DEVICE_LIST[idx]
                    friendly_name = str(device).split("/")[-1:][0]

                    # Update the task name in the rich progress output
                    if DEVICE_STATUS[idx] == 1:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} writing 'aa'")
                    elif DEVICE_STATUS[idx] == 2:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} verifying 'aa'")
                    elif DEVICE_STATUS[idx] == 3:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} writing '55'")
                    elif DEVICE_STATUS[idx] == 4:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} verifying '55'")
                    elif DEVICE_STATUS[idx] == 5:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} writing 'ff'")
                    elif DEVICE_STATUS[idx] == 6:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} verifying 'ff'")
                    elif DEVICE_STATUS[idx] == 7:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} writing '00'")
                    elif DEVICE_STATUS[idx] == 8:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} verifying '00'")
                    elif DEVICE_STATUS[idx] == 20:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} verification failed")
                    elif DEVICE_STATUS[idx] == 30:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} completed successfully")
                    else:
                        progress.update(task_id=task, description=f"[cyan]{friendly_name} unknown?")

                    # Determine if any tasks are still progressing
                    if DEVICE_STATUS[idx] < 20:
                        maybe_finished = False
                        progress.update(task_id=task, completed=DEVICE_PROGRESS[idx], refresh=True)
                    else:
                        progress.update(task_id=task, completed=True, refresh=True)

                if maybe_finished is True:
                    finished = True


if __name__ == "__main__":
    main()
