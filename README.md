# dirsplit

Command-line utility to split a directory to multiple parts of mostly equal size.

There are several supported targets:

  * `scan` estimates volumes and their sizes (a dry run);

  * `copy` copies the source files to several directories,
    each approximately the desired target size;

  * `link` works same as `copy`, but hard links files
    (source and destination directory should be on the same
    device/mount point);

  * `symlink` works same as `copy`, but uses symbolic links;

  * `iso` creates an optical disc image with aliases `cd74`, `dvd`
    and `bd`, that assign target size matching to 74-minute CD-R,
    single-layer DVD-R or BD-R media respectively;

  * `tar`, `tgz` or `txz` creates (optionally compressed) tarball;

  * `listfile` create text files with new-line delimited file
    paths.


## Usage

To prepare your photos collection for DVD-R backup run

    # dirsplit ~/Photos dvd ~/Photos%#.iso

`%#` placeholder will be replaced with volume number.
