#include <sys/types.h>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <err.h>
#include <fcntl.h>
#include <getopt.h>
#include <list>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sysexits.h>
#include <unistd.h>
#include <vector>

struct Volume;
struct Directory;
struct File {
	Directory *parent = 0;
	std::string name;
	off_t size; // rounded-up to block size
	struct stat st;
	Volume *volume = 0;

	typedef bool (*OrderFn)(const File &, const File &);
};
static bool
file_name_less(const File &lhs, const File &rhs)
{
	return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
}

struct Directory {
	Directory *parent = 0;
	std::string path; // /-terminated, relative to ds.inpath
	std::string name;
	off_t tot_size = 0;
	typedef std::list<Directory> Subdirs;
	Subdirs subdirs;
	typedef std::list<File> Files;
	Files files;
	struct stat st;
	time_t newf_mtime = 0; // mtime of newest file inside

	bool is_root(void) const { return !parent; }
	bool is_leaf(void) const { return subdirs.empty(); }
	typedef bool (*OrderFn)(const Directory &, const Directory &);
};
static bool
dir_name_less(const Directory &lhs, const Directory &rhs)
{
	return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
}

enum TransferMode { xfer_copy, xfer_symlink, xfer_hardlink };

struct Volume {
	std::list<File *> files;
	off_t tot_size = 0;
	size_t volume_no = 0;

	void create_listfile(const std::string &path) const;
	bool transfer_files(const std::string &base_dir,
			    const std::string &dest_dir,
			    TransferMode mode) const;
};

struct DirSplit {
	std::string inpath;
	std::string target;
	std::string outpath;

	off_t block_size = 512; // to multiple file sizes to (bytes)
	off_t volume_size = 0;
	off_t per_volume_overhead = 0;
	// TODO: need per-directory overhead
	off_t per_file_overhead = 0;

	bool abort_on_err = false;
	bool depth1st = true; // file enumeration strategy
	Directory::OrderFn dir_order = &dir_name_less;
	File::OrderFn file_order = &file_name_less;

	Directory root;
	std::list<File *> all_files;
	std::list<Volume> volumes;

	// Enumerates files and sub-directories in dir, recursively
	int enumerate_dir(int base_fd, Directory &dir);
	// Appends to all_files recursively
	void enumerate_files(Directory &dir);
	// Processes all_files to assign File.volume_no and discover num_volumes
	void split_to_volumes(void);
	// Return outpath replacing %# placeholder with the value of volume_no
	const std::string get_out_path(size_t volume_no) const;

	int create_tar(void);
	int create_iso(void);
	int transfer(void);
	int create_listfiles(void);
	int dry_run(void);
};

static off_t
parse_si_scale(const char *s)
{
	if (!*s) {
		return 1;
	} else if (!strcmp(s, "k") || !strcmp(s, "K")) {
		return 1024;
	} else if (!strcmp(s, "M")) {
		return 1024 * 1024;
	} else if (!strcmp(s, "G")) {
		return 1024 * 1024 * 1024;
	} else {
		return 0;
	}
}

static off_t
parse_size(const char *s)
{
	char *endp;
	long n = strtol(s, &endp, 0);
	off_t scale;
	if (*endp && *endp == '.') {
		// Assume a floating point number
		double d = strtod(s, &endp);
		if (d < 0 || (scale = parse_si_scale(endp)) == 0)
			errx(EX_USAGE, "illegal size -- %s", s);
		return off_t(d * double(scale));
	}
	if (n < 0 || (scale = parse_si_scale(endp)) == 0)
		errx(EX_USAGE, "illegal size -- %s", s);
	return n * scale;
}

int
DirSplit::enumerate_dir(int base_fd, Directory &dir)
{
	const char *base_path = inpath.c_str();
	const bool root_dir = dir.is_root();
	const char *path = dir.path.c_str();
	// base_fd is fd for ds.inpath already
	int fd = !root_dir ?
		  openat(base_fd, dir.name.c_str(), O_RDONLY | O_DIRECTORY) :
		  base_fd;
	if (fd == -1) {
		if (abort_on_err)
			err(EX_OSERR, "opendir '%s/%s'", base_path, path);
		warn("opendir '%s/%s'", base_path, path);
		return -1;
	}
	DIR *dirp = fdopendir(fd); // XXX: leaked upon exception
	if (!dirp) {
		if (abort_on_err)
			err(EX_OSERR, "opendir '%s/%s'", base_path, path);
		warn("opendir '%s/%s'", base_path, path);
		close(fd);
		return -1;
	}
	int res = 0;
	dirent *entry;
	while ((entry = readdir(dirp)) != nullptr) {
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;
		struct stat st;
		if (fstatat(fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) ==
		    -1) {
			if (abort_on_err)
				err(EX_OSERR, "stat '%s/%s/%s'", base_path,
				    path, entry->d_name);
			warn("stat '%s/%s/%s'", base_path, path, entry->d_name);
			res = 1;
			continue;
		}
		const bool is_subdir = S_ISDIR(st.st_mode);
		const bool is_file = S_ISREG(st.st_mode) || S_ISLNK(st.st_mode);
		if (is_subdir) {
			dir.subdirs.push_back(Directory());
			auto &subdir = dir.subdirs.back();
			subdir.parent = &dir;
			if (!root_dir)
				subdir.path.assign(path);
			subdir.path.append(entry->d_name).append(1, '/');
			subdir.name = entry->d_name;
			subdir.st = st;
			res |= enumerate_dir(fd, subdir);
			dir.tot_size += subdir.tot_size;
			dir.newf_mtime =
			    (std::max)(dir.newf_mtime, subdir.newf_mtime);
		} else if (is_file) {
			dir.files.push_back(File());
			auto &file = dir.files.back();
			file.parent = &dir;
			file.name = entry->d_name;
			file.size = st.st_size;
			if (block_size) {
				file.size = (file.size + block_size - 1) &
				    ~(block_size - 1);
				assert((file.size % block_size) == 0);
			}
			file.st = st;

			dir.tot_size += file.size;
			dir.newf_mtime =
			    (std::max)(dir.newf_mtime, st.st_mtime);
		}
	}
	// closedir(3) closes fd, as well
	closedir(dirp);

	if (dir_order)
		dir.subdirs.sort(dir_order);
	if (file_order)
		dir.files.sort(file_order);

	return res;
}

void
DirSplit::enumerate_files(Directory &dir)
{
	for (size_t step = 1; step <= 2; ++step) {
		if ((depth1st && step == 1) || (!depth1st && step == 2)) {
			for (auto &subdir : dir.subdirs)
				enumerate_files(subdir);
		} else {
			for (auto &file : dir.files)
				all_files.push_back(&file);
		}
	}
}

void
print_leaves(const Directory &dir)
{
	for (const auto &subdir : dir.subdirs) {
		if (subdir.is_leaf()) {
			printf("%12ld\t%s\n", long(subdir.tot_size),
			       subdir.path.c_str());
		} else {
			print_leaves(subdir);
		}
	}
}

void
DirSplit::split_to_volumes(void)
{
	// Start with incrementing of ds.volume_no below
	off_t size = volume_size;
	Volume *volume = nullptr;
	for (auto *file : all_files) {
		const off_t file_size = per_file_overhead + file->size;
		if (file_size > volume_size)
			warnx("file '%s/%s/%s' is larger than the target size",
			      inpath.c_str(), file->parent->path.c_str(),
			      file->name.c_str());
		if (size + file_size > volume_size) {
			if (volume)
				volume->tot_size = size;
			volumes.push_back(Volume());
			volume = &volumes.back();
			volume->volume_no = volumes.size();
			size = per_volume_overhead;
		}
		file->volume = volume;
		volume->files.push_back(file);
		size += file_size;
	}
	if (volume)
		volume->tot_size = size;
}

const std::string
DirSplit::get_out_path(size_t volume_no) const
{
	// Extend volume_no with zeros to make all output paths' width uniform
	size_t num_digits = 1, test = 9;
	const size_t num_volumes = volumes.size();
	while (num_volumes > test) {
		++num_digits;
		test = test * 10 + 9;
	}
	char buf[32];
	snprintf(buf, sizeof(buf), "%0*u", int(num_digits),
		 unsigned(volume_no));

	std::string res;
	size_t start = 0, pos;
	while ((pos = outpath.find("%#", start)) != outpath.npos) {
		res.append(outpath, start, pos - start).append(buf);
		start = pos + 2;
	}
	res.append(outpath, start, outpath.length() - start);
	return res;
}

static const std::string
get_temp_file_path(const std::string &path)
{
	return std::string(path).append(1, '#');
}

static const std::string
create_temp_file(void)
{
	char listfile[] = { "/tmp/dirsplit.XXXXXX" };
	int fd = mkstemp(listfile);
	if (fd == -1)
		err(EX_OSERR, "mkstemp");
	close(fd);
	return listfile;
}

// Either commit temp_file to final_path or roll it back (delete it)
static void
signoff_file(const std::string &temp_path, const std::string &final_path,
	     bool commit)
{
	if (commit) {
		if (rename(temp_path.c_str(), final_path.c_str()) == -1)
			err(EX_OSERR, "rename '%s' to '%s'", temp_path.c_str(),
			    final_path.c_str());
	} else {
		if (unlink(temp_path.c_str()) == -1)
			err(EX_OSERR, "unlink '%s'", temp_path.c_str());
	}
}

void
Volume::create_listfile(const std::string &path) const
{
	FILE *fp = fopen(path.c_str(), "w");
	if (!fp)
		err(EX_OSERR, "create '%s'", path.c_str());
	for (auto *file : files)
		fprintf(fp, "%s%s\n", file->parent->path.c_str(),
			file->name.c_str());
	if (fclose(fp) != 0) {
		// Don't leak corrupted file, preserve last error
		const auto eno = errno;
		(void)unlink(path.c_str());
		errno = eno;
		err(EX_OSERR, "close '%s'", path.c_str());
	}
}

static std::string
get_full_path(const Directory &dir)
{
	if (dir.parent && !dir.parent->is_root())
		return get_full_path(*dir.parent)
		    .append(1, '/')
		    .append(dir.name);
	return dir.name;
}

static const std::string
get_full_path(const File &file)
{
	assert(file.parent);
	if (file.parent && !file.parent->is_root())
		return get_full_path(*file.parent)
		    .append(1, '/')
		    .append(file.name);
	return file.name;
}

static bool
make_dirs(const std::string &base_path, const std::string &rel_file_path)
{
	const mode_t dirmode = S_IRWXU | S_IRGRP | S_IXGRP;
	std::string path;
	path.append(base_path);
	size_t start = 0, pos;
	while ((pos = rel_file_path.find('/', start)) != rel_file_path.npos) {
		path.append(1, '/').append(rel_file_path, start, pos - start);
		if (mkdir(path.c_str(), dirmode) == -1 && errno != EEXIST) {
			warn("mkdir '%s'", path.c_str());
			return false;
		}
		start = pos + 1;
	}
	return true;
}

static bool
copy_file(const std::string &srcpath, const File &file,
	  const std::string &destpath)
{
	const std::string tempfile = get_temp_file_path(destpath);
	std::vector<uint8_t> buffer(128 * 1024);
	uint8_t *buf = &buffer[0];

	int infd = open(srcpath.c_str(), O_RDONLY);
	if (infd == -1) {
		warn("open '%s'", srcpath.c_str());
		return false;
	}

	int outfd = creat(tempfile.c_str(), file.st.st_mode);
	if (outfd == -1) {
		warn("create '%s'", tempfile.c_str());
		close(infd);
		return false;
	}

	bool res = true;
	ssize_t br;
	while ((br = read(infd, buf, buffer.size())) > 0) {
		ssize_t bw = write(outfd, buf, size_t(br));
		if (bw == -1) {
			warn("write '%s'", tempfile.c_str());
			res = false;
			break;
		}
	}
	if (br == -1) {
		warn("read '%s'", srcpath.c_str());
		res = false;
	}

	// Preserve flags
	if (res && fchflags(outfd, file.st.st_flags) == -1) {
		warn("chflags '%s'", tempfile.c_str());
		res = false;
	}

	// Preserve ownership
	if (res && fchown(outfd, file.st.st_uid, file.st.st_gid) == -1) {
		warn("chown '%s'", tempfile.c_str());
		res = false;
	}

	// Preserve access and modification times
	timespec ts[2] = { file.st.st_atim, file.st.st_mtim };
	if (res && futimens(outfd, ts) == -1) {
		warn("utimens '%s'", tempfile.c_str());
		res = false;
	}

	if (close(outfd) == -1) {
		warn("close '%s'", tempfile.c_str());
		res = false;
	}
	close(infd);

	signoff_file(tempfile, destpath, res);
	return res;
}

// Recursive rm -fR <dirpath>, except it will NOT delete the <dirpath> itself
static bool
deltree(int base_fd, const std::string &base_path, const std::string &dirname)
{
	assert(dirname != "/");
	if (dirname == "/")
		return false;

	assert(base_path.empty() || base_path.back() == '/');
	int fd = openat(base_fd, dirname.c_str(), O_RDONLY | O_DIRECTORY);
	if (fd == -1) {
		warn("opendir '%s%s'", base_path.c_str(), dirname.c_str());
		return false;
	}
	DIR *dirp = fdopendir(fd); // XXX: leaked upon exception
	if (!dirp) {
		warn("opendir '%s%s'", base_path.c_str(), dirname.c_str());
		close(fd);
		return false;
	}
	bool res = true;
	dirent *entry;
	std::string new_base_path;
	new_base_path.append(base_path).append(dirname).append(1, '/');
	while ((entry = readdir(dirp)) != nullptr) {
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;
		struct stat st;
		if (fstatat(fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) ==
		    -1) {
			warn("stat '%s%s/%s'", base_path.c_str(),
			     dirname.c_str(), entry->d_name);
			res = false;
			continue;
		}
		const bool is_subdir = S_ISDIR(st.st_mode);
		if (is_subdir) {
			if (!deltree(fd, new_base_path, entry->d_name))
				res = false;
		} else {
			if (unlinkat(fd, entry->d_name, 0) == -1) {
				warn("unlink '%s%s/%s'", base_path.c_str(),
				     dirname.c_str(), entry->d_name);
			}
		}
	}

	if (!base_path.empty() &&
	    unlinkat(base_fd, dirname.c_str(), AT_REMOVEDIR) == -1) {
		warn("rmdir '%s%s'", base_path.c_str(), dirname.c_str());
		res = false;
	}

	// closedir(3) closes fd, as well
	closedir(dirp);

	return res;
}

bool
Volume::transfer_files(const std::string &base_dir, const std::string &dest_dir,
		       TransferMode mode) const
{
	const mode_t dirmode = S_IRWXU | S_IRGRP | S_IXGRP;
	if (mkdir(dest_dir.c_str(), dirmode) == -1 && errno != EEXIST)
		err(EX_OSERR, "mkdir '%s'", dest_dir.c_str());

	Directory *prev_parent = 0;
	std::string path, srcpath, destpath;
	srcpath.assign(base_dir).append(1, '/');
	destpath.assign(dest_dir).append(1, '/');
	for (const auto *file : files) {
		path = get_full_path(*file);
		// Adjacent files could have different parent directories
		if (file->parent != prev_parent) {
			make_dirs(dest_dir, path);
			prev_parent = file->parent;
		}
		srcpath.erase(base_dir.length() + 1).append(path);
		destpath.erase(dest_dir.length() + 1).append(path);
		switch (mode) {
		case xfer_copy:
			if (!copy_file(srcpath, *file, destpath))
				return false;
			break;
		case xfer_hardlink:
			if (link(srcpath.c_str(), destpath.c_str()) == -1) {
				warn("link '%s' to '%s'", srcpath.c_str(),
				     destpath.c_str());
				return false;
			}
			break;
		case xfer_symlink:
			if (symlink(srcpath.c_str(), destpath.c_str()) == -1) {
				warn("symlink '%s' to '%s'", srcpath.c_str(),
				     destpath.c_str());
				return false;
			}
			break;
		}
	}
	return true;
}

int
DirSplit::create_listfiles(void)
{
	for (const auto &volume : volumes) {
		const std::string path = get_out_path(volume.volume_no);
		std::string tempfile = get_temp_file_path(path);
		volume.create_listfile(tempfile);
		signoff_file(tempfile, path, true);
		printf("%s created\n", path.c_str());
	}
	return 0;
}

int
DirSplit::dry_run(void)
{
	for (const auto &volume : volumes)
		printf("volume %u is %ld bytes in %u files (%.1f%% full)\n",
		       unsigned(volume.volume_no), long(volume.tot_size),
		       unsigned(volume.files.size()),
		       double(volume.tot_size) * 100.0 / double(volume_size));
	return 0;
}

// Runs procname with given args and returns true on zero exit status
static bool
run_subproc(const std::string &procname, const std::vector<std::string> &args)
{
	pid_t pid = fork();
	if (pid == -1) {
		warn("fork");
		return false;
	} else if (pid == 0) {
		std::vector<char *> va;
		va.push_back(const_cast<char *>(procname.c_str()));
		for (auto &a : args)
			va.push_back(const_cast<char *>(a.c_str()));
		va.push_back(NULL);
		execvp(procname.c_str(), &va[0]);
		err(EX_OSERR, "exec %s", procname.c_str());
	} else {
		int status;
		pid_t xpid = waitpid(pid, &status, WEXITED);
		if (xpid == -1) {
			warn("waitpid");
			return false;
		} else if (WIFSIGNALED(status)) {
			warnx("%s exitted via signal %d", procname.c_str(),
			      WTERMSIG(status));
		} else if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
			warnx("%s exitted with %d", procname.c_str(),
			      WEXITSTATUS(status));
		}
		return WIFEXITED(status) && WEXITSTATUS(status) == 0;
	}
}
static bool
run_subproc(const std::string &procname,
	    const std::initializer_list<std::string> &args)
{
	return run_subproc(procname, std::vector<std::string>(args));
}

int
DirSplit::create_tar(void)
{
	const char *create = "-cf";
	if (target == "tgz") {
		create = "-czf";
	} else if (target == "txz") {
		create = "-cJf";
	}
	int res = 0;
	const std::string listfile = create_temp_file();
	for (const auto &volume : volumes) {
		volume.create_listfile(listfile);
		std::string path = get_out_path(volume.volume_no);
		std::string tempfile = get_temp_file_path(path);
		const bool success = run_subproc("tar",
						 { create, tempfile, "-T",
						   listfile, "-C", inpath });
		signoff_file(tempfile, path, success);
		if (success) {
			printf("%s created\n", path.c_str());
		} else {
			res = 1;
		}
	}
	(void)unlink(listfile.c_str());
	return res;
}

int
DirSplit::create_iso(void)
{
	// mkisofs(8) places all files given via -path-list in the media's root
	// directory. As a work around create a temporary directory populated
	// with symbolic links to source files and apply -follow-links option.
	// However this hack prohibits including symbolic links in the image
	char tempdir[] = { "/tmp/dirsplit.XXXXXX" };
	if (mkdtemp(tempdir) == NULL)
		err(EX_OSERR, "mkdtemp");

	// Paths in listfile are relative to inpath, but mkisofs does not
	// support command-line option similar to tar's -C, hence chdir(2)
	if (chdir(tempdir) == -1) {
		rmdir(tempdir);
		err(EX_OSERR, "chdir '%s'", tempdir);
	}

	int res = 0;
	for (const auto &volume : volumes) {
		if (!volume.transfer_files(inpath, tempdir, xfer_symlink)) {
			deltree(AT_FDCWD, "", tempdir);
			res = 1;
			break;
		}

		std::string path = get_out_path(volume.volume_no);

		const char *v = strrchr(path.c_str(), '/');
		v = v ? v + 1 : path.c_str();
		const char *endv = strchr(v, '.');
		endv = endv ? endv : v + strlen(v);
		std::string volumelbl(v, size_t(endv - v));

		std::string tempfile = get_temp_file_path(path);
		const bool success =
		    run_subproc("mkisofs",
				{ "-r", "-J", "-udf", "-V", volumelbl,
				  "-follow-links", "-o", tempfile, "-quiet",
				  "." });
		signoff_file(tempfile, path, success);
		if (success) {
			printf("%s created\n", path.c_str());
		} else {
			res = 1;
		}
		deltree(AT_FDCWD, "", tempdir); // Doesn't delete tempdir itself
	}
	if (rmdir(tempdir) == -1)
		warn("rmdir '%s'", tempdir);
	return res;
}

int
DirSplit::transfer(void)
{
	int res = 0;
	TransferMode mode = xfer_copy;
	if (target == "link") {
		// Hardlinks require input and output directory to be located on
		// the same device; abort here if that is not so
		struct stat inst, outst;

		// Last component from outpath contains %# placeholder and is
		// not an existing directory; ignore it
		std::string outloc(outpath);
		size_t pos = outloc.rfind('/');
		if (pos != outloc.npos)
			outloc.erase(pos);

		if (stat(inpath.c_str(), &inst) == -1)
			err(EX_OSERR, "stat '%s'", inpath.c_str());
		if (stat(outloc.c_str(), &outst) == -1)
			err(EX_OSERR, "stat '%s'", outloc.c_str());
		if (inst.st_dev != outst.st_dev)
			errx(EX_USAGE,
			     "hard links require same device/mount point");

		mode = xfer_hardlink;
	} else if (target == "symlink") {
		mode = xfer_symlink;
	}
	for (const auto &volume : volumes) {
		std::string path = get_out_path(volume.volume_no);
		if (volume.transfer_files(inpath, path, mode)) {
			printf("%s created\n", path.c_str());
		} else {
			res = 1;
			break;
		}
	}
	return res;
}

static const std::string
get_absolute_path(const char *path)
{
	// path can be absolute ("/path/file"), explicitly ("./path/file",
	// "path/file") or implicitly ("file") relative
	assert(path);
	if (*path == '/')
		return path;

	const char *slash = strrchr(path, '/');
	std::string res;
	if (slash) {
		res.assign(path, slash);
	} else {
		res.assign(".");
	}
	char *abspath = realpath(res.c_str(), 0);
	if (!abspath)
		err(EX_OSERR, "realpath '%s'", res.c_str());
	res.assign(abspath);
	free(abspath);

	if (slash) {
		res.append(slash);
	} else {
		res.append(1, '/').append(path);
	}
	return res;
}

static void
usage(void)
{
	puts(
	    "usage: dirsplit [-h] [-s volsize] <in-dir> iso|cd74|dvd|bd <name>_%#.iso");
	puts(
	    "                                           tar|tgz|txz <name>_%#.tar");
	puts(
	    "                                           copy|link|symlink <name>_%#");
	puts(
	    "                                           listfile <name>_%#.txt");
	puts("                                           scan");
}

int
main(int argc, char *argv[])
{
	DirSplit ds;
	int opt;
	while ((opt = getopt(argc, argv, "hs:")) != -1) {
		switch (opt) {
		case 'h': usage(); return 0;
		case 's': ds.volume_size = parse_size(optarg); break;
		}
	}
	argc -= optind;
	argv += optind;
	if (argc < 2) {
		usage();
		return EX_USAGE;
	}

	ds.inpath = get_absolute_path(argv[0]);

	int (DirSplit::*target_proc)(void) = 0;
	ds.target = argv[1];
	bool need_output = true;
	if (ds.target == "iso" || ds.target == "cd74" || ds.target == "dvd" ||
	    ds.target == "bd") {
		target_proc = &DirSplit::create_iso;
		ds.block_size = 2048;
		ds.per_volume_overhead = 1024 * 1024;
		ds.per_file_overhead = 2048;
		off_t autosize_mb = 0;
		if (ds.target == "cd74") {
			autosize_mb = 650;
		} else if (ds.target == "dvd") {
			autosize_mb = 4474;
		} else if (ds.target == "bd") {
			autosize_mb = 23828;
		}
		if (!ds.volume_size && autosize_mb)
			ds.volume_size = autosize_mb * 1024 * 1024;
	} else if (ds.target == "tar" || ds.target == "tgz" ||
		   ds.target == "txz") {
		target_proc = &DirSplit::create_tar;
		ds.per_file_overhead = 512;
	} else if (ds.target == "copy" || ds.target == "link" ||
		   ds.target == "symlink") {
		target_proc = &DirSplit::transfer;
	} else if (ds.target == "listfile") {
		target_proc = &DirSplit::create_listfiles;
	} else if (ds.target == "scan") {
		target_proc = &DirSplit::dry_run;
		need_output = false;
	} else {
		errx(EX_USAGE, "illegal target -- %s", ds.target.c_str());
	}
	if (!ds.volume_size)
		errx(EX_USAGE, "%s target requires a volume size (-s)",
		     ds.target.c_str());
	if (need_output && argc == 2)
		errx(EX_USAGE, "%s target requires an output path",
		     ds.target.c_str());

	if (need_output) {
		ds.outpath = get_absolute_path(argv[2]);
		if (ds.outpath.find("%#") == ds.outpath.npos)
			errx(
			    EX_USAGE,
			    "illegal output path '%s' -- missing %%# placeholder",
			    argv[2]);
	}

	int fd = open(ds.inpath.c_str(), O_RDONLY | O_DIRECTORY);
	if (fd == -1)
		err(EX_OSERR, "open '%s'", ds.inpath.c_str());
	// ds.root.path, .name and .parent are left empty for the root dir
	if (ds.enumerate_dir(fd, ds.root) != 0)
		warnx("errors occured while scanning '%s'", ds.inpath.c_str());
	// printf("total usage for '%s' is %ld\n", ds.inpath.c_str(),
	//        long(ds.root.tot_size));

	// print_leaves(ds.root);
	ds.enumerate_files(ds.root);
	ds.split_to_volumes();
	return (ds.*target_proc)();
}
