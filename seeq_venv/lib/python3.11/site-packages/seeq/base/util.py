from __future__ import annotations

import glob
import os
import pathlib
import re
import shutil
import sys
import tempfile
import time
import warnings
from inspect import stack
from typing import Union


def remove_regex_capture_group_names(regex):
    # PostgreSQL will choke on named capture groups
    return re.sub(r'\?<\w+>', '', regex)


def pythonize_regex_capture_group_names(regex):
    # Unlike standard regex syntax, Python capture groups have a capital P:  (?P<group_name>.*?)
    return re.sub(r'\?<(\w+)>', r'?P<\1>', regex)


def replace_tokens_in_regex(regex, group_dict, escape=True):
    for name, value in group_dict.items():
        regex = regex.replace('${%s}' % name, re.escape(value) if escape else value)

    return regex


def os_lock(name, target, args=(), kwargs=None, timeout=7.0, retry_period=0.1):
    """
    Adapted from https://github.com/ses4j/backup/blob/master/oslockedaction.py
    To use, just wrap your python call with an os_lock() call.  Instead of:
    >>> time.sleep(5.0)
    Do:
    >>> os_lock(name="mylock", target=time.sleep, args=(5.0,))
    Nobody else using the same lockfile will be able to run at the same time.
    If timeout is None, it will never timeout if the lockfile exists.
    Otherwise it will just try to remove the lockfile after "timeout" seconds and do 'action()' anyway.
    retry_period is a float in seconds to wait between retries.
    """
    lockfile = os.path.abspath(os.path.join(tempfile.tempdir, f'{name}.lock'))

    f = None
    started_at = time.time()
    kwargs = dict() if kwargs is None else kwargs

    try:
        while True:
            if timeout is not None and time.time() > started_at + timeout:
                # timed out, it must be stale... (i hope)  so remove it.

                if time.time() > (started_at + (timeout * 2)):
                    raise RuntimeError(f"FAIL: os_lock() can't acquire directory lock, because {lockfile} was not "
                                       "cleaned up. Delete it manually if the process is finished.")

                try:
                    os.remove(lockfile)
                except OSError:
                    time.sleep(retry_period)
                    continue

            try:
                f = os.open(lockfile, os.O_RDWR | os.O_CREAT | os.O_EXCL)
                os.write(f, bytes(f'{str(__file__)} made this, called from {sys.argv[0]}', encoding='utf-8'))
                break
            except OSError:
                time.sleep(retry_period)

        target(*args, **kwargs)
    finally:
        if f:
            os.close(f)
            os.remove(lockfile)


def deprecation_warning(message, *, stack_index=2):
    frame_info = stack()
    if len(frame_info) > stack_index:
        warnings.warn(
            f'{message} (from line {frame_info[stack_index].lineno} in file "{frame_info[stack_index].filename}")')
    else:
        warnings.warn(message)


def get_platform():
    import platform
    plat = platform.platform()
    if plat.lower().find('windows') != -1:
        return 'windows'
    if plat.lower().find('linux') != -1:
        return 'linux'
    if plat.lower().find('darwin') != -1 or plat.lower().find('macos') != -1:
        return 'osx'


def is_windows():
    return get_platform() == 'windows'


def is_linux():
    return get_platform() == 'linux'


def is_osx():
    return get_platform() == 'osx'


def is_osx_aarch64():
    import platform
    return get_platform() == 'osx' and platform.platform().lower().find('arm64') != -1


def get_home_dir(user=None):
    if is_windows():
        home = os.environ['USERPROFILE']
    else:
        user_to_expand = user if user else ''
        home = os.path.expanduser(f'~{user_to_expand}')

    return home


def get_test_with_file():
    return os.path.normpath(
        os.path.join(os.path.dirname(__file__), '..', 'sdk', 'test-with.txt'))


def get_test_with_root_dir():
    test_with_file = get_test_with_file()
    test_with = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    if safe_exists(test_with_file):
        with safe_open(test_with_file, 'r') as f:
            lines = f.readlines()
            test_with = lines[0]

    return test_with


def do_with_retry(func, timeout_sec=5, retry_sleep_sec=0.1):
    import time
    timer = time.time()
    while True:
        # noinspection PyBroadException
        try:
            func()
            break
        except Exception:
            if time.time() - timer > timeout_sec:
                raise

            time.sleep(retry_sleep_sec)


def safe_remove(path, *args, **kwargs):
    return os.remove(handle_long_filenames(path), *args, **kwargs)


def safe_rmtree(path, *args, **kwargs):
    return shutil.rmtree(handle_long_filenames(path), *args, **kwargs)


def safe_open(filename, *args, **kwargs):
    return open(handle_long_filenames(filename), *args, **kwargs)


def safe_makedirs(filename, *args, **kwargs):
    filename = handle_long_filenames(filename)
    # Empirically, it looks like os.makedirs() cannot handle long filenames, so throw a half-decent exception
    if len(filename) > 260:
        raise RuntimeError(f'Filename "{filename}" is too long.  Windows has a 260 character limit.')

    return os.makedirs(handle_long_filenames(filename), *args, **kwargs)


def safe_walk(filename, *args, **kwargs):
    return os.walk(handle_long_filenames(filename), *args, **kwargs)


def safe_copy(src, dst, *args, **kwargs):
    return shutil.copy(handle_long_filenames(src), handle_long_filenames(dst), *args, **kwargs)


def safe_copytree(src, dst, *args, **kwargs):
    return shutil.copytree(handle_long_filenames(src), handle_long_filenames(dst), *args, **kwargs)


def safe_isfile(filename):
    return os.path.isfile(handle_long_filenames(filename))


def safe_isdir(filename):
    return os.path.isdir(handle_long_filenames(filename))


def safe_isabs(filename):
    return os.path.isabs(handle_long_filenames(filename))


def safe_abspath(filename):
    return os.path.abspath(handle_long_filenames(filename))


def safe_relpath(path, start):
    return os.path.relpath(handle_long_filenames(path), handle_long_filenames(start))


def safe_exists(filename):
    return os.path.exists(handle_long_filenames(filename))


def safe_glob(filename, *args, **kwargs):
    return glob.glob(handle_long_filenames(filename), *args, **kwargs)


def handle_long_filenames(path: Union[str, pathlib.Path]):
    # Make sure it is a string (could have been a pathlib.Path)
    path = str(path)

    if not is_windows():
        return path

    # Make sure the path is absolute so we can add the \\?\ prefix
    path = os.path.abspath(path)

    # Get rid of extra/unnecessary /../ or /./ sections of the path. Note that normpath does not work properly with
    # long filenames, so we have to convert to short paths and then call this before cleaning.
    path = os.path.normpath(re.sub(r'^([/\\][/\\]\?[/\\])?(\w:)', r'\2', path))

    # First replace all forward slashes with backslashes like Windows wants.
    path = re.sub(r'/', r'\\', path)

    # Windows 'long paths' are of the form \\?\D:\Foo\Bar (instead of the
    # regular D:\Foo\Bar). So we have to do a RegEx replacement that involves
    # A LOT of backslashes.
    path = re.sub(r'^(\w):\\', r'\\\\?\\\1:\\', path)

    return path


def cleanse_filename(filename, replacement_char='_'):
    return re.sub(r'[:"%\\/<>^|?*&\[\]]', replacement_char, filename)
