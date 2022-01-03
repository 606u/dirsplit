#include <sys/types.h>
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

template <typename T, typename Dtor> struct Guard { // RAII guard
        T handle;
        Dtor dtor;
        Guard(T handle, Dtor dtor)
            : handle(handle)
            , dtor(dtor)
        {
        }
        Guard(const Guard &) = delete;
        Guard(Guard &&src)
            : handle(src.handle)
            , dtor(src.dtor)
        {
                src.handle = 0;
        }
        ~Guard()
        {
                if (handle)
                        dtor(handle);
        }
        Guard &operator=(const Guard &) = delete;
};
template <typename T, typename Dtor>
static inline Guard<T, Dtor>
autofree(T handle, Dtor dtor)
{
        return Guard<T, Dtor>(handle, dtor);
}

struct Directory;
struct File {
	Directory *parent = 0;
	std::string name;
	off_t size;		// rounded-up to block size
	struct stat st;
	size_t volume_no = 0;
	typedef bool (*OrderFn)(const File&, const File&);
};
static bool
file_name_less(const File &lhs, const File &rhs)
{
	return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) == -1;
}

struct Directory {
	Directory *parent = 0;
	std::string path;	// /-terminated
	std::string name;
	off_t tot_size = 0;
	size_t volume_no = 0;	// 0 if split across >1 volume
	typedef std::list<Directory> Subdirs;
	typedef std::list<File> Files;
	Subdirs subdirs;
	Files files;
	struct stat st;
	time_t newf_mtime = 0;	// mtime of newest file inside
	bool is_root(void) const { return name.empty(); }
	bool is_leaf(void) const { return subdirs.empty(); }
	typedef bool (*OrderFn)(const Directory&, const Directory&);
};
static bool
dir_name_less(const Directory &lhs, const Directory &rhs)
{
	return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) == -1;
}

struct ScanOpts {
	size_t block_size = 512;
	bool abort_on_err = false;
	Directory::OrderFn dir_order = &dir_name_less;
	File::OrderFn file_order = &file_name_less;
};

struct Job {
	std::string inpath;
	std::string target;
	std::string outpath;
	off_t volume_size = 0;
	size_t per_file_overhead = 0;
	ScanOpts scan_opts;
	size_t volume_no = 0;

	Directory root;
	std::list<File*> all_files;
	typedef int (*HandlerFn)(Job &job);
	HandlerFn handler = 0;
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
	long n = strtoul(s, &endp, 0);
	off_t scale;
	if (*endp && *endp == '.') {
		// Assume a floating point number
		double d = strtod(s, &endp);
		if ((scale = parse_si_scale(endp)) == 0)
			errx(EX_USAGE, "illegal size -- %s", s);
		return off_t(d * double(scale));
	}
	if ((scale = parse_si_scale(endp)) == 0)
		errx(EX_USAGE, "illegal size -- %s", s);
	return n * scale;
}

static int
scan_dir(const char *base_path, int base_fd, Directory *dir, ScanOpts *opts)
{
	const bool root_dir = dir->is_root();
	const char *path = dir->path.c_str();
	const bool aoe = opts && opts->abort_on_err;
	int fd = !root_dir ? openat(base_fd, dir->name.c_str(), O_RDONLY | O_DIRECTORY) : base_fd;
	if (fd == -1) {
		if (aoe) err(EX_OSERR, "opendir '%s/%s'", base_path, path);
		warn("opendir '%s/%s'", base_path, path);
		return -1;
	}
	DIR *dirp = fdopendir(fd); // XXX: leaked upon exception
	if (!dirp) {
		if (aoe) err(EX_OSERR, "opendir '%s/%s'", base_path, path);
		warn("opendir '%s/%s'", base_path, path);
		close(fd);
		return -1;
	}
	// closedir(3) closes fd, as well
	auto dirp_guard = autofree(dirp, closedir);
	int res = 0;
	dirent *entry;
	while ((entry = readdir(dirp)) != nullptr) {
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;
		struct stat st;
		if (fstatat(fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) == -1) {
			if (aoe)
				err(EX_OSERR, "stat '%s/%s/%s'", base_path, path,
				    entry->d_name);
			warn("stat '%s/%s/%s'", base_path, path, entry->d_name);
			res = 1;
			continue;
		}
		const bool is_subdir = S_ISDIR(st.st_mode);
		const bool is_file = S_ISREG(st.st_mode) || S_ISLNK(st.st_mode);
		if (is_subdir) {
			dir->subdirs.push_back(Directory());
			auto &subdir = dir->subdirs.back();
			subdir.parent = dir;
			if (!root_dir) {
				subdir.path.assign(path).append(1, '/')
					.append(entry->d_name).append(1, '/');
				subdir.name = subdir.path.c_str() + strlen(path) + 1;
			} else {
				subdir.path.assign(entry->d_name).append(1, '/');
				subdir.name = subdir.path.c_str();
			}
			subdir.st = st;
			res |= scan_dir(base_path, fd, &subdir, opts);
			dir->tot_size += subdir.tot_size;
			dir->newf_mtime =
				(std::max)(dir->newf_mtime, subdir.newf_mtime);
		} else if (is_file) {
			dir->files.push_back(File());
			auto &file = dir->files.back();
			file.parent = dir;
			file.name = entry->d_name;
			file.size = st.st_size;
			if (opts)
				file.size = (file.size + opts->block_size - 1) &
					~opts->block_size;
			file.st = st;

			dir->tot_size += file.size;
			dir->newf_mtime = (std::max)(dir->newf_mtime, st.st_mtime);
		}
	}

	if (opts && opts->dir_order)
		dir->subdirs.sort(opts->dir_order);
	if (opts && opts->file_order)
		dir->files.sort(opts->file_order);

	return res;
}

static void
unfold_depth1st(Directory &dir, std::list<File*> *all_files)
{
	for (auto &subdir : dir.subdirs)
		unfold_depth1st(subdir, all_files);
	for (auto &file : dir.files)
		all_files->push_back(&file);
}

static void
print_leaves(const Directory &dir)
{
	for (const auto &subdir : dir.subdirs) {
		if (subdir.is_leaf()) {
			printf("%12ld\t%s\n",
			       long(subdir.tot_size), subdir.path.c_str());
		} else {
			print_leaves(subdir);
		}
	}
}

static void
assign_volumes(Job &job)
{
	// Start with incrementing of job.volume_no below
	off_t size = job.volume_size;
	for (auto *file : job.all_files) {
		const off_t file_size = job.per_file_overhead + file->size;
		if (file_size > job.volume_size)
			warnx("file '%s/%s/%s' is larger than the target size",
			      job.inpath.c_str(), file->parent->path.c_str(),
			      file->name.c_str());
		if (size + file_size > job.volume_size) {
			++job.volume_no;
			size = 0;
		}
		file->volume_no = job.volume_no;
		size += file_size;
	}
}

// Return outpath replacing %# placeholder with the value of volume_no
static const std::string
expand_outpath(const std::string &outpath, size_t volume_no, size_t num_volumes)
{
	// Make sure volume_no is extended with zeroes so all volume
	// numbers are with uniform width
	size_t num_digits = 1, test = 9;
	while (num_volumes > test) {
		++num_digits;
		test = test * 10 + 9;
	}
	char buf[32];
	snprintf(buf, sizeof(buf), "%0*u", int(num_digits), unsigned(volume_no));

	std::string res;
	size_t start = 0, pos;
	while ((pos = outpath.find("%#", start)) != outpath.npos) {
		res.append(outpath, start, pos - start).append(buf);
		start = pos + 2;
	}
	res.append(outpath, start, outpath.length() - start);
	return res;
}

static bool
create_listfile(Job &job, const char *path, size_t volume_no)
{
	std::string tempfile = path;
	tempfile.append(1, '#');
	FILE *fp = fopen(tempfile.c_str(), "w");
	bool empty = true;
	if (fp) {
		auto fp_guard = autofree(fp, fclose);
		for (auto *file : job.all_files) {
			if (file->volume_no != volume_no)
				continue;
			fprintf(fp, "%s%s\n", file->parent->path.c_str(),
				file->name.c_str());
			empty = false;
		}
		if (fflush(fp) != 0)
			err(EX_OSERR, "flush '%s'", tempfile.c_str());
	} else {
		err(EX_OSERR, "create '%s'", tempfile.c_str());
	}
	if (!empty) {
		if (rename(tempfile.c_str(), path) == -1)
			err(EX_OSERR, "rename '%s' to '%s'", tempfile.c_str(), path);
	} else {
		if (unlink(tempfile.c_str()) == -1)
			err(EX_OSERR, "unlink '%s'", tempfile.c_str());
	}
	return !empty;
}

static int
create_listfiles(Job &job)
{
	for (size_t i = 1; i <= job.volume_no; ++i) {
		const std::string path = expand_outpath(job.outpath, i, job.volume_no);
		create_listfile(job, path.c_str(), i);
		printf("%s created\n", path.c_str());
	}
	return 0;
}

static int
scan_inpath(Job &job)
{
	size_t volume_no = 1, n_files = 0;
	off_t tot_size = 0;
	for (const auto *file : job.all_files) {
		if (file->volume_no != volume_no) {
			printf("volume %u is %ld bytes in %u files (%.1f%% full)\n",
			       unsigned(volume_no), long(tot_size), unsigned(n_files),
			       tot_size * 100.0 / job.volume_size);
			++volume_no;
			n_files = 0;
			tot_size = 0;
		}
		++n_files;
		tot_size += file->size;
	}
	printf("volume %u is %ld bytes in %u files (%.1f%% full)\n",
	       unsigned(volume_no), long(tot_size), unsigned(n_files),
	       tot_size * 100.0 / job.volume_size);
	return 0;
}

typedef void (*SubProcessFn)(Job&, const char*listfile, const char *outpath,
			     size_t volume_id);
static int
subprocess(Job &job, const char *procname, SubProcessFn handler)
{
	char listfile[] = { "/tmp/dirsplit.XXXXXX" };
	int fd = mkstemp(listfile);
	if (fd == -1)
		err(EX_OSERR, "mkstemp");
	close(fd);
	for (size_t i = 1; i <= job.volume_no; ++i) {
		create_listfile(job, listfile, i);
		std::string path = expand_outpath(job.outpath, i, job.volume_no);
		std::string temppath(path); temppath.append(1, '#');
		pid_t pid = fork();
		if (pid == -1) {
			err(EX_OSERR, "fork");
		} else if (pid == 0) {
			// Not expected to return
			handler(job, listfile, temppath.c_str(), i);
			err(EX_OSERR, "exec %s", procname);
		} else {
			int status;
			pid_t xpid = waitpid(pid, &status, WEXITED);
			if (xpid == -1) {
				warn("waitpid");
			} if (WIFSIGNALED(status)) {
				warnx("%s exitted via signal %d", procname,
				      WTERMSIG(status));
			} else if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
				warnx("%s exitted with %d", procname,
				      WEXITSTATUS(status));
			}

			if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
				// Keep file on success
				if (rename(temppath.c_str(), path.c_str()) != -1) {
					printf("%s created\n", path.c_str());
				} else {
					err(EX_OSERR, "rename '%s' to '%s'",
					    temppath.c_str(), path.c_str());
				}
			} else {
				// Drop file on error
				if (unlink(temppath.c_str()) == -1 && errno != ENOENT)
					warn("unlink '%s'", temppath.c_str());
			}
		}
	}
	(void)unlink(listfile);
	return 0;
}

static void
tar_subproc(Job &job, const char *listfile, const char *outpath, size_t)
{
	// cmdline: tar -cf <name> -T <listfile> -C <inpath>
	const char *create = "-cf";
	if (job.target == "tgz") {
		create = "-czf";
	} else if (job.target == "txz") {
		create = "-cJf";
	}
	execlp("tar", "tar", create, outpath, "-T", listfile, "-C", job.inpath.c_str(),
	       nullptr);
}
static int
create_tar(Job &job)
{
	return subprocess(job, "tar", tar_subproc);
}

static void
mkisofs_subproc(Job &job, const char *listfile, const char *outpath, size_t)
{
	const char *v = strrchr(outpath, '/');
	v = v ? v + 1 : outpath;
	const char *endv = strchr(v, '.');
	endv = endv ? endv : v + strlen(v);
	std::string volume(v, endv - v);

	if (chdir(job.inpath.c_str()) == -1)
		err(EX_OSERR, "chdir '%s'", job.inpath.c_str());

	// cmdline: mkisofs -r -J -udf -V <label> -path-list <listfile> -o <outpath>
	execlp("mkisofs", "mkisofs", "-r", "-J", "-udf", "-V", volume.c_str(),
	       "-path-list", listfile, "-o", outpath, "-quiet", nullptr);
}
static int
create_iso(Job &job)
{
	return subprocess(job, "mkisofs", mkisofs_subproc);
}

static void
usage(void)
{
	puts("usage: dirsplit [-h] [-s volsize] <in-dir> iso|cd74|dvd|bd <name>_%#.iso");
	puts("                                           tar|tgz|txz <name>_%#.tar");
	puts("                                           dir <name>_%#");
	puts("                                           listfile <name>_%#.txt");
	puts("                                           scan");
}

int
main(int argc, char *argv[])
{
	Job job;
	int opt;
	while ((opt = getopt(argc, argv, "hs:")) != -1) {
		switch (opt) {
		case 'h': usage(); return 0;
		case 's': job.volume_size = parse_size(optarg); break;
		}
	}
	argc -= optind;
	argv += optind;
	if (argc < 2) {
		usage();
		return EX_USAGE;
	}

	job.inpath = argv[0];

	job.target = argv[1];
	bool need_output = true;
	off_t autosize_mb = 0;
	if (job.target == "iso") {
		job.handler = &create_iso;
	} else if (job.target == "cd74") {
		job.handler = &create_iso;
		autosize_mb = 650;
	} else if (job.target == "dvd") {
		job.handler = &create_iso;
		autosize_mb = 4474;
	} else if (job.target == "bd") {
		job.handler = &create_iso;
		autosize_mb = 23828;
	} else if (job.target == "tar" || job.target == "tgz" || job.target == "txz") {
		job.handler = &create_tar;
	} else if (job.target == "listfile") {
		job.handler = &create_listfiles;
	} else if (job.target == "scan") {
		job.handler = &scan_inpath;
		need_output = false;
	} else {
		errx(EX_USAGE, "illegal target -- %s", job.target.c_str());
	}
	if (!job.volume_size && autosize_mb)
		job.volume_size = autosize_mb * 1024;
	if (!job.volume_size)
		errx(EX_USAGE, "%s target requires a volume size (-s)",
		     job.target.c_str());
	if (need_output && argc == 2)
		errx(EX_USAGE, "%s target requires an output path", job.target.c_str());

	if (need_output) {
		job.outpath = argv[2];
		if (job.outpath.find("%#") == job.outpath.npos)
			errx(EX_USAGE,
			     "illegal output path '%s' -- no %%# placeholder found",
			     job.outpath.c_str());
		if (job.outpath[0] != '/') {
			// Convert output path to absolute, because working
			// directory is changed for some targets.  outpath
			// might be explicitly (./dir/name) or implicitly
			// (dir/name) relative
			const char *slash = strrchr(job.outpath.c_str(), '/');
			std::string outdir;
			if (slash) {
				outdir.assign(job.outpath.c_str(), slash);
			} else {
				outdir.assign(".");
			}
			char *abspath = realpath(outdir.c_str(), 0);
			if (!abspath)
				err(EX_OSERR, "realpath '%s'", outdir.c_str());
			outdir.assign(abspath);
			free(abspath);
			if (slash) {
				outdir.append(slash);
			} else {
				outdir.append(1, '/').append(job.outpath);
			}
			job.outpath = outdir;
		}
	}

	int fd = open(job.inpath.c_str(), O_RDONLY | O_DIRECTORY);
	if (fd == -1)
		err(EX_OSERR, "open '%s'", job.inpath.c_str());
	// job.root.path and .name are left empty for the root dir
	int res = scan_dir(job.inpath.c_str(), fd, &job.root, &job.scan_opts);
	if (res)
		warnx("errors occured while scanning '%s'", job.inpath.c_str());
	//printf("total usage for '%s' is %ld\n", job.inpath.c_str(),
	//       long(job.root.tot_size));

	print_leaves(job.root);
	unfold_depth1st(job.root, &job.all_files);
	assign_volumes(job);
	return job.handler(job);
}
