#!/usr/bin/env python3

"""
A cli to destructively test attached disks, using the same patterns as badblocks.

Unlike badblocks though, this reads and writes to the chosen disks in parallel.
"""

import mmap
import os
from stat import S_ISLNK
import subprocess
import argparse
import pathlib
import sys
import time
import rapidjson
from datetime import datetime
from multiprocessing import Pool, Array

# Rich
from rich import print
from rich.layout import Layout
from rich.live import Live
from rich.progress import Progress
from rich.table import Table

# Prompt Toolkit
from prompt_toolkit import prompt
from prompt_toolkit.key_binding import KeyBindings

CMD_BLOCKDEV = "/usr/sbin/blockdev"
CMD_LSBLK = "/usr/bin/lsblk"
CMD_ZFS = "/usr/bin/zfs"
DEBUG = False
VERSION = "0.0.3"

DEVICE_LIST = []
DEVICE_PROGRESS = Array("I", [])
DEVICE_STATUS = Array("B", [])
TASK_LIST = Array("I", [])

CURSOR_ROW = 0
CURSOR_VISIBLE = True
drive_list = []
layout = Layout(name="root")

bindings = KeyBindings()


@bindings.add("up")
def _(event):
    "Move the cursor up when the up arrow is pressed"
    global CURSOR_ROW
    global drive_list, layout

    CURSOR_ROW -= 1 if CURSOR_ROW > 0 else 0
    choice_table = build_drive_list_table(CURSOR_ROW)
    layout["left"].update(choice_table)
    print(layout)


@bindings.add("down")
def _(event):
    "Move the cursor down when the down arrow is pressed"
    global CURSOR_ROW
    global drive_list, layout

    drive_list_length = len(drive_list)
    CURSOR_ROW += 1 if CURSOR_ROW < drive_list_length else drive_list_length
    choice_table = build_drive_list_table(CURSOR_ROW)
    layout["left"].update(choice_table)
    print(layout)


@bindings.add("space")
def _(event):
    "Invert the current drive selection when the space bar is pressed"
    update_drive_selection()
    choice_table = build_drive_list_table(CURSOR_ROW)
    layout["left"].update(choice_table)
    print(layout)


def build_drive_list_table(selected_row=0) -> Table:
    global CURSOR_VISIBLE
    global drive_list
    drive_list_table = Table(
        title="Choose the drives to destructively test",
        caption="Use up/down arrows. SPACE to select. ENTER to continue",
    )
    drive_list_table.add_column("Test?")
    drive_list_table.add_column("Name")
    drive_list_table.add_column("Size")
    drive_list_table.add_column("Vendor")
    drive_list_table.add_column("Model")

    current_row = 0
    selected_color = "green on blue"
    for drive in drive_list:
        vendor = "Unknown" if drive["vendor"] is None else drive["vendor"].strip()
        model = "Unknown" if drive["model"] is None else drive["model"].strip()
        selection = r"\[x]" if drive["selected"] else r"\[ ]"
        if current_row == selected_row and CURSOR_VISIBLE:
            drive_list_table.add_row(
                f"[{selected_color}]{selection}[/]",
                f"[{selected_color}]{drive['name']}[/]",
                f"[{selected_color}]{drive['size']}[/]",
                f"[{selected_color}]{vendor}[/]",
                f"[{selected_color}]{model}[/]",
            )
        else:
            drive_list_table.add_row(
                selection, drive["name"], drive["size"], vendor, model
            )
        current_row += 1

    return drive_list_table


def get_drive_list(selected_devices=None) -> list:
    try:
        lsblk_output = subprocess.check_output(
            [CMD_LSBLK, "-o", "name,type,size,vendor,model", "-J"]
        ).strip()
    except subprocess.CalledProcessError:
        print("Couldn't run lsblk.  Aborting!")
        return []
    except FileNotFoundError:
        print("lsblk doesn't seem to exist at {CMD_LSBLK}")
        return []

    drives_json = rapidjson.loads(lsblk_output)

    # Get the zfs volume list
    zfs_vol_list = get_zfs_volumes()

    drives = []
    for drive in drives_json["blockdevices"]:
        if drive["type"] == "disk":
            # Check if the drive was selected on the command line
            selected = False
            if selected_devices:
                for sel in selected_devices:
                    friendly_name = str(sel).split("/")[-1:][0]

                    # Check if the selected device matches the /dev/zd[number] name
                    if drive["name"] == friendly_name:
                        selected = True

                    # Check if the selected device matches the ZFS pool/dataset name
                    pool_dataset = str(sel).removeprefix("/dev/zvol/")
                    for zfs_volume in zfs_vol_list:
                        if (
                            drive["name"] == zfs_volume["device_name"]
                            and pool_dataset == zfs_volume["name"]
                        ):
                            selected = True

            # If the name matches a zfs volume device, then we use pool/dataset for the name instead
            zfs = False
            drive_name = drive["name"]
            device_name = drive["name"]
            for zfs_volume in zfs_vol_list:
                if zfs_volume["device_name"] == drive["name"]:
                    drive_name = zfs_volume["name"]
                    device_name = zfs_volume["device_name"]
                    zfs = True

            # Add the drive to the selection display
            drives.append(
                {
                    "name": drive_name,
                    "size": drive["size"],
                    "vendor": drive["vendor"],
                    "model": drive["model"],
                    "selected": selected,
                    "device_name": device_name,
                    "zfs": zfs,
                }
            )
    return sorted(drives, key=lambda entry: entry["name"])


def get_selected_devices() -> list:
    global drive_list

    selection_list = []
    for row in drive_list:
        if row["selected"]:
            if row["zfs"]:
                selection_list.append("/dev/zvol/" + row["name"])
            else:
                selection_list.append(row["name"])

    return selection_list


def get_zfs_volumes() -> list:
    """
    If zfs is present, then this function returns a list of the zfs block devices along with some useful metadata
    :return:
    """

    # Check if the zfs command line utility is present
    try:
        os.stat(CMD_ZFS, follow_symlinks=True)
    except FileNotFoundError:
        # ZFS doesn't seem to be installed
        if DEBUG:
            print("zfs command not found")
        return []

    # Retrieve the list of ZFS data sets
    try:
        cmd_output = subprocess.Popen(
            [CMD_ZFS, "list", "-p", "-H"],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )
        zfs_list_datasets = cmd_output.stdout.readlines()
    except subprocess.CalledProcessError:
        print("Calling zfs list failed.  Aborting!")
        return []
    zfs_datasets = []
    for line in zfs_list_datasets:
        zfs_datasets.append(line.split()[0:1][0])
    if DEBUG:
        print(zfs_datasets)

    # Separate out the list of ZFS volumes
    zfs_volumes = []
    zfs_list_volumes = []
    for dataset in zfs_datasets:
        try:
            cmd_output = subprocess.Popen(
                [CMD_ZFS, "get", "-H", "volsize", dataset],
                stdout=subprocess.PIPE,
                universal_newlines=True,
            )
            zfs_list_volumes.append(cmd_output.stdout.read())
        except subprocess.CalledProcessError:
            print("Calling zfs list failed.  Aborting!")
            return []
    for volume in zfs_list_volumes:
        z = volume.split()
        if z[2] != "-":
            # Split the volume name into pool/dataset components
            pool = z[0].split("/")[0]
            dataset_path = z[0].removeprefix(pool + "/")

            # Retrieve the underlying storage device name.  ie "/dev/zd0" as "zd0"
            zvol_path = os.path.join("/", "dev", "zvol", z[0])
            mode = os.stat(zvol_path, follow_symlinks=False).st_mode
            device_name = ""
            if S_ISLNK(mode):
                device_name = os.path.basename(os.readlink(zvol_path))
            else:
                print(
                    f"Something went wrong when attempting to retrieve info for {zvol_path}"
                )
                return []

            zfs_volumes.append(
                {
                    "name": z[0],
                    "size": z[2],
                    "pool": pool,
                    "path": dataset_path,
                    "device_name": device_name,
                }
            )
    if DEBUG:
        print(zfs_volumes)
    return zfs_volumes


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
        return False

    # Get the total size of the device
    try:
        device_size = int(
            subprocess.check_output([CMD_BLOCKDEV, "--getsize64", device]).strip()
        )
    except subprocess.CalledProcessError:
        print(f"Couldn't run blockdev on {device}.  Aborting!")
        return False
    except Exception as e:
        print("Something went wrong when trying to run blockdev")
        print(e.with_traceback(None))
        return False

    if DEBUG:
        print(f"Total size of '{device}': {round(device_size / (1024 * 1024))} MB")

    # Get block size to use for the transfers
    try:
        # 'blockdev --getss' gets logical block size, and 'blockdev --getpbsz' gets physical block size.  Not sure which
        # one is better for this program, but lets start with physical and see how it goes
        physical_block_size = int(
            subprocess.check_output([CMD_BLOCKDEV, "--getpbsz", device]).strip()
        )
    except Exception as e:
        print(e.with_traceback())
        return False

    if DEBUG:
        print(f"Physical block size of device: {physical_block_size} bytes")

    num_blocks_in_device = round(device_size / physical_block_size)

    # Open the device for read-write operations
    try:
        # To add Windows support, apparently "os.O_BINARY" would need to be added
        device_handle = os.open(path=device, flags=os.O_DIRECT | os.O_RDWR)
    except FileNotFoundError:
        print(
            f"Can't access device '{device}'. Permissions problem, or wrong device name maybe?"
        )
        return False
    except PermissionError:
        print(f"Trying to open '{device}' for writing failed. Did you forget sudo?")
        return False
    file_handle = os.fdopen(fd=device_handle, mode="rb+", buffering=0)

    # Test the device with various characters
    verify_succeeded = True
    for test_byte in [
        bytearray.fromhex("aa"),
        bytearray.fromhex("55"),
        bytearray.fromhex("ff"),
        bytearray.fromhex("00"),
    ]:
        # Create a byte array of the character being tested
        test_array = bytearray()
        for i in range(physical_block_size):
            test_array += test_byte

        # Write to the device
        write_status = False
        try:
            write_status = write_disk(
                list_element,
                file_handle,
                physical_block_size,
                num_blocks_in_device,
                test_array,
                test_byte,
            )
        except Exception as e:
            print(e.with_traceback(None))
            return False
        if not write_status:
            print(f"Writing {test_byte} to {device} failed!")
            return False

        # Verify writing to the device worked
        # TODO: This feels like a dodgy way to check the verification worked?
        # verify_status = False
        # try:
        # TODO: Put this back into a try block when I have some idea about the errors that can be returned
        verify_status = verify_disk(
            list_element,
            file_handle,
            physical_block_size,
            num_blocks_in_device,
            test_array,
            test_byte,
        )
        # except:
        #     print(f"Reading from '{device}' failed!")
        #     verify_succeeded = False
        #     break
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


def update_drive_selection():
    """
    Updates the selection status of drives in the drive_list global array
    :return:
    """
    global CURSOR_ROW
    global drive_list
    current_row = 0
    new_list = []
    for row in drive_list:
        new_selected = row["selected"]
        if current_row == CURSOR_ROW:
            new_selected = not row["selected"]
        new_list.append(
            {
                "name": row["name"],
                "size": row["size"],
                "vendor": row["vendor"],
                "model": row["model"],
                "selected": new_selected,
                "device_name": row["device_name"],
                "zfs": row["zfs"],
            }
        )
        current_row += 1
    drive_list = new_list


def verify_disk(
    list_element,
    file_to_be_read,
    physical_block_size,
    num_blocks_in_device,
    comparison_array,
    expected_byte,
) -> bool:
    global DEVICE_LIST, DEVICE_PROGRESS, DEVICE_STATUS
    if expected_byte == bytearray.fromhex("aa"):
        DEVICE_STATUS[list_element] = 2
    elif expected_byte == bytearray.fromhex("55"):
        DEVICE_STATUS[list_element] = 4
    elif expected_byte == bytearray.fromhex("ff"):
        DEVICE_STATUS[list_element] = 6
    elif expected_byte == bytearray.fromhex("00"):
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
        m = mmap.mmap(
            fileno=file_to_be_read.fileno(),
            length=physical_block_size,
            offset=seek_position,
        )
        read_buffer = m.read(physical_block_size)
        if read_buffer != comparison_array:
            # TODO: Better reporting of verification failure(s)
            return False
        m.close()
    return True


def write_disk(
    list_element,
    file_to_be_read,
    physical_block_size,
    num_blocks_in_device,
    array_to_write,
    test_byte,
) -> bool:
    global DEVICE_LIST, DEVICE_PROGRESS, DEVICE_STATUS
    if test_byte == bytearray.fromhex("aa"):
        DEVICE_STATUS[list_element] = 1
    elif test_byte == bytearray.fromhex("55"):
        DEVICE_STATUS[list_element] = 3
    elif test_byte == bytearray.fromhex("ff"):
        DEVICE_STATUS[list_element] = 5
    elif test_byte == bytearray.fromhex("00"):
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
            m = mmap.mmap(
                fileno=file_to_be_read.fileno(),
                length=physical_block_size,
                offset=seek_position,
            )
            m.write(array_to_write)
            m.close()
    except Exception as e:
        # TODO: Better reporting of write failures
        print(e.with_traceback(None))
        return False

    return True


def main():
    global \
        CURSOR_ROW, \
        CURSOR_VISIBLE, \
        DEVICE_LIST, \
        DEVICE_PROGRESS, \
        DEVICE_STATUS, \
        TASK_LIST
    global drive_list, layout

    # Get the device name(s) to test
    parser = argparse.ArgumentParser(
        description="Badblocks, but in Python and able to test multiple devices simultaneously"
    )
    parser.add_argument(
        "-d",
        "--devices",
        help="device to test (multiple occurrences is allowed)",
        type=pathlib.Path,
        action="append",
        required=False,
    )
    args = parser.parse_args()

    # TODO: Require running as super-user

    # Create main window layout
    layout.split(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
    )
    layout["main"].split_row(Layout(name="left"), Layout(name="right"))

    # Create header
    grid = Table.grid(expand=True)
    grid.add_column(justify="center", ratio=1)
    grid.add_row(f"Destructive disk testing utility v{VERSION}")
    grid.add_row("Started: " + datetime.now().ctime().replace(":", "[blink]:[/]"))
    # TODO: Add or complete a "Finished" line when the program finishes
    # grid.add_row("Finished: " + datetime.now().ctime().replace(":", "[blink]:[/]"))
    layout["header"].update(grid)

    # TODO: We should probably also handle being passed zfs pool/dataset on the command line (ie "-d pool/dataset1"),
    #       without needing the "/dev/zvol/" text fragment at the start of the device name

    # Determine the number of devices passed on the command line
    if args.devices:
        drive_list = get_drive_list(args.devices)
        CURSOR_VISIBLE = False
    else:
        drive_list = get_drive_list()

    # Create the list of drives in the left panel
    choice_table = build_drive_list_table(CURSOR_ROW)
    layout["left"].update(choice_table)
    print(layout)

    # If no devices were provided on the command line, then use the drive selection dialog
    # Keys: cursor up/down, space to select, and enter to continue
    if not args.devices:
        prompt(key_bindings=bindings)
        CURSOR_VISIBLE = False
        choice_table = build_drive_list_table(CURSOR_ROW)
        layout["left"].update(choice_table)
        print(layout)
    selected_devices = get_selected_devices()
    if not selected_devices:
        print("No drives were selected.  Aborting.")
        sys.exit(1)

    # TODO: Check if any of the selected devices are currently mounted, and if so then handle it.  Refuse to proceed?

    # Size the progress information arrays appropriately
    num_drives = len(selected_devices)
    DEVICE_PROGRESS = Array("I", range(num_drives))
    DEVICE_STATUS = Array("B", range(num_drives))
    TASK_LIST = Array("I", range(num_drives))

    # Create the progress bar in the right panel
    progress = Progress()
    layout["right"].update(progress)

    cnt = 0
    for device in selected_devices:
        DEVICE_LIST.append(device)
        friendly_name = str(device).split("/")[-1:][0]
        TASK_LIST[cnt] = progress.add_task(
            f"[cyan]{friendly_name} writing...", total=100
        )
        cnt += 1

    # Launch the background drive read/verify tasks
    with Pool(processes=len(DEVICE_LIST)) as pool:
        pool.imap(test_disk, DEVICE_LIST)

        # This main process just reports the results until the tasks are finished
        finished = False
        with Live(layout, refresh_per_second=10, screen=False):
            while not finished:
                time.sleep(0.1)

                maybe_finished = True
                for idx, task in enumerate(TASK_LIST):
                    # Determine the friendly name for the current task's device
                    device = DEVICE_LIST[idx]
                    friendly_name = str(device).split("/")[-1:][0]

                    # Update the task name in the rich progress output
                    if DEVICE_STATUS[idx] == 1:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} writing 'aa'",
                        )
                    elif DEVICE_STATUS[idx] == 2:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} verifying 'aa'",
                        )
                    elif DEVICE_STATUS[idx] == 3:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} writing '55'",
                        )
                    elif DEVICE_STATUS[idx] == 4:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} verifying '55'",
                        )
                    elif DEVICE_STATUS[idx] == 5:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} writing 'ff'",
                        )
                    elif DEVICE_STATUS[idx] == 6:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} verifying 'ff'",
                        )
                    elif DEVICE_STATUS[idx] == 7:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} writing '00'",
                        )
                    elif DEVICE_STATUS[idx] == 8:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} verifying '00'",
                        )
                    elif DEVICE_STATUS[idx] == 20:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} verification failed",
                        )
                    elif DEVICE_STATUS[idx] == 30:
                        progress.update(
                            task_id=task,
                            description=f"[cyan]{friendly_name} completed successfully",
                        )
                    else:
                        progress.update(
                            task_id=task, description=f"[cyan]{friendly_name} unknown?"
                        )

                    # Determine if any tasks are still progressing
                    # TODO: The individual tasks have a boolean "finished" attribute which seems like it should be
                    #       better for this
                    if DEVICE_STATUS[idx] < 20:
                        maybe_finished = False
                        progress.update(
                            task_id=task, completed=DEVICE_PROGRESS[idx], refresh=True
                        )
                    else:
                        progress.update(task_id=task, completed=100, refresh=True)

                if maybe_finished is True:
                    finished = True


if __name__ == "__main__":
    main()
