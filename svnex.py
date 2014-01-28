#! /usr/bin/env python3

"""Converts from a remote Subversion repository to Git's "fast-import" format

The program is written to:

* use Subversion's remote access protocol
* minimise traffic from the Subversion server by
    * skipping revisions that do not affect the branch
    * skipping paths that are outside the branch
    * requesting deltas rather than full copies of files where practical
    * requesting exclusion of deltas for ignored files
* follow branch copies
* produce identical commits to "git-svn", except that it
    * optionally drops commits that are simple branch copies
* be run incrementally
* handle Subversion merge tracking information

It does not:

* handle or correlate multiple trunks, branches, or tags
* handle symbolic links, although it does handle executable files
* do anything with special Subversion file or revision properties
"""

from subvertpy.ra import RemoteAccess
from subvertpy.delta import apply_txdelta_window
from subvertpy.properties import (
    PROP_REVISION_DATE,
    PROP_REVISION_AUTHOR,
    PROP_REVISION_LOG,
    PROP_EXECUTABLE,
    PROP_MERGEINFO,
)
from sys import stderr, argv, exc_info
from subvertpy.properties import time_from_cstring
from io import SEEK_END
from subvertpy import SubversionException
from subprocess import Popen
import subprocess
import argparse
from funcparams import splitdoc
from errno import EPIPE
from contextlib import contextmanager
import subvertpy.ra
from collections import defaultdict
from bisect import bisect_right, bisect_left
from contextlib import closing
from subvertpy.properties import parse_mergeinfo_property
from misc import Context

INVALID_REVNUM = -1

def main():
    (summary, _) = splitdoc(__doc__)
    parser = argparse.ArgumentParser(description=summary)
    
    parser.add_argument("url", metavar="url[@rev]", help="subversion URL")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file",
        metavar="FILENAME", help="fast import file")
    group.add_argument("importer", nargs="*", default=(),
        help="command to pipe fast import stream to")
    
    parser.add_argument("--git-ref", required=True, metavar="REFNAME",
        help="Git ref name to export to (e.g. refs/remotes/svn/trunk)")
    parser.add_argument("--rev-map", metavar="FILENAME",
        help="""file mapping from Subversion paths and revisions
        to existing Git revisions,
        each line formatted as PATH@SVN-REV (space) GIT-REV""")
    parser.add_argument("-A", "--authors-file", metavar="FILENAME", help=
        'file mapping Subversion user names to Git authors, like "git-svn"')
    parser.add_argument("--rewrite-root",
        metavar="URL", help="subversion URL to store in the metadata")
    parser.add_argument("--ignore", action="append", default=list(),
        metavar="PATH", help="add a path to be excluded from export")
    parser.add_argument("--export-copies", action="store_true",
        help="export simple branch copies even when no files were modified")
    parser.add_argument("-q", "--quiet", action="store_true",
        help="suppress progress messages")
    
    args = parser.parse_args()
    
    url = args.url.rsplit("@", 1)
    if len(url) > 1:
        peg_rev = url.pop()
        if peg_rev:
            peg_rev = int(peg_rev)
        else:
            peg_rev = INVALID_REVNUM
    else:
        peg_rev = INVALID_REVNUM
    (url,) = url
    
    rev_map = defaultdict(dict)
    if args.rev_map is not None:
        with open(args.rev_map, "rt") as file:
            for s in file:
                (s, gitrev) = s.rstrip("\n").rsplit(" ", 1)
                (branch, svnrev) = s.rsplit("@", 1)
                svnrev = int(svnrev)
                rev_map[branch][svnrev] = gitrev
    
    if args.authors_file is not None:
        author_map = dict()
        with open(args.authors_file, "rt") as file:
            for line in file:
                if line.endswith("\n"):
                    line = line[:-1]
                (svn, git) = line.split(" = ", 1)
                author_map[svn] = git
    else:
        author_map = None
    
    if args.importer:
        output = FastExportPipe(args.importer)
    else:
        output = FastExportFile(args.file)
    with output:
        try:
            exporter = Exporter(url, output,
                rev_map=rev_map,
                author_map=author_map,
                root=args.rewrite_root,
                ignore=args.ignore,
                export_copies=args.export_copies,
                quiet=args.quiet,
            )
            exporter.export(args.git_ref, rev=peg_rev)
        except SubversionException as err:
            (msg, num) = err.args
            raise SystemExit("E{}: {}".format(num, msg))

class Exporter:
    def __init__(self, url, output,
        rev_map={},
        author_map=None,
        root=None,
        ignore=(),
        export_copies=False,
        quiet=False,
    ):
        self.output = output
        self.author_map = author_map
        self.ignore = ignore
        self.export_copies = export_copies
        
        self.known_branches = dict()
        for (branch, revs) in rev_map.items():
            branch = branch.lstrip("/")
            starts = list()
            runs = list()
            self.known_branches[branch] = (starts, runs)
            start = None
            run = ()
            for (svnrev, gitrev) in sorted(revs.items()):
                if svnrev - len(run) != start:
                    start = svnrev
                    starts.append(start)
                    run = list()
                    runs.append(run)
                run.append(gitrev)
        
        self.quiet = quiet
        if self.quiet:
            self.progress = dummycontext
        else:
            self.progress = progresscontext
        
        auth = subvertpy.ra.Auth((
            # Avoids the following error for diffs on local (file:) URLs:
            # "No provider registered for 'svn.username' credentials"
            subvertpy.ra.get_username_provider(),
            
            # Avoids RemoteAccess() failing for HTTPS URLs with
            # "Unable to connect to a repository at URL"
            # and error code 215001 ("No authentication provider available")
            subvertpy.ra.get_ssl_server_trust_file_provider(),
        ))
        
        with self.progress("connecting to ", url):
            self.ra = RemoteAccess(url, auth=auth)
            self.url = url
        
        self.repos_root = self.ra.get_repos_root()
        if root is None:
            self.root = self.repos_root
        else:
            self.root = root
        
        self.uuid = self.ra.get_uuid()
    
    def export(self, git_ref, branch=None, rev=INVALID_REVNUM):
        if branch is None:
            branch = self.url[len(self.repos_root) + 1:]
        segments = PendingSegments(self, branch, rev)
        
        # Not using RemoteAccess.get_file_revs() because it does not work on
        # directories
        
        # TODO: Use RemoteAccess.replay_range() for initial location segment
        # and trailing parts of subsequent segments. Would require
        # remembering all versions of files received.
        
        (base_rev, base_path) = segments.base
        if base_rev:
            gitrev = segments.git_base
        else:
            gitrev = None
        
        init_export = True
        for (base, end, path) in segments:
            path = "/" + path
            prefix = path.rstrip("/") + "/"
            url = (self.repos_root + path).rstrip("/")
            with iter_revs(self, path, base, end) as revs:
                for (rev, date, author, log, paths) in revs:
                    if not init_export and base_path != path[1:]:
                        # Base revision is at a different branch location.
                        # Will have to diff the base location against the
                        # current location. Have to switch root because the
                        # diff reporter does not accept link_path() on the
                        # top-level directory.
                        self.url = self.repos_root + "/" + base_path
                        self.url = self.url.rstrip("/")
                        self.ra.reparent(self.url)
                    
                    if not self.quiet:
                        stderr.write(":")
                        stderr.flush()
                    editor = RevEditor(self.output, self.quiet)
                    
                    # Diff editor does not convey deletions when starting
                    # from scratch
                    if init_export:
                        dir = DirEditor(editor)
                        for (file, (action, _, _)) in paths.items():
                            if (not file.startswith(prefix) or
                            action not in "DR"):
                                continue
                            file = file[len(prefix):]
                            for p in self.ignore:
                                if (file == p or
                                file.startswith((p + "/").lstrip("/"))):
                                    break
                            else:
                                dir.delete_entry(file)
                    
                    reporter = self.ra.do_diff(rev, "", url, editor,
                        True, True, True)
                    if init_export:
                        reporter.set_path("", rev, True)
                    else:
                        reporter.set_path("", base_rev, False)
                    
                    for p in self.ignore:
                        reporter.set_path(p, INVALID_REVNUM, True, None,
                            subvertpy.ra.DEPTH_EXCLUDE)
                    
                    reporter.finish()
                    # Assume the editor calls are all completed now
                    
                    merges = list()
                    if editor.mergeinfo:
                        if not self.quiet:
                            print(file=stderr)
                        ancestors = Ancestors(self)
                        if base_rev:
                            ancestors.add_natural(base_path, base_rev)
                        merged = RevisionSet()
                        merged.update(ancestors)
                        mergeinfo = editor.mergeinfo.items()
                        for (branch, ranges) in mergeinfo:
                            branch = branch.lstrip("/")
                            for (start, end, _) in ranges:
                                merged.add_segment(branch, start, end)
                                ancestors.add_natural(branch, end)
                        if ancestors == merged:
                            # TODO: minimise so that only independent branch heads are listed
                            # i.e. do not explicitly merge C if also merging A and B, and C is an ancestor of both A and B
                            for (branch, ranges) in mergeinfo:
                                branch = branch.lstrip("/")
                                for (_, end, _) in ranges:
                                    ancestor = self.export(git_ref,
                                        branch, end)
                                    if ancestor is not None:
                                        merges.append(ancestor)
                    
                    line = "commit {}\n".format(git_ref)
                    self.output.file.write(line.encode("utf-8"))
                    
                    mark = self.output.newmark()
                    line = "mark {}\n".format(mark)
                    self.output.file.write(line.encode("ascii"))
                    
                    date = time_from_cstring(date) // 10**6
                    
                    if self.author_map is None:
                        author = "{author} <{author}@{uuid}>".format(
                            author=author, uuid=self.uuid)
                    else:
                        author = self.author_map[author]
                    
                    line = "committer {} {} +0000\n".format(author, date)
                    self.output.file.write(line.encode("utf-8"))
                    
                    log = "{}\n\ngit-svn-id: {}{}@{} {}\n".format(
                        log, self.root, path.rstrip("/"), rev, self.uuid)
                    log = log.encode("utf-8")
                    line = "data {}\n".format(len(log))
                    self.output.file.write(line.encode("ascii"))
                    self.output.file.writelines((log, b"\n"))
                    
                    if (init_export or merges) and gitrev is not None:
                        line = "from {}\n".format(gitrev)
                        self.output.file.write(line.encode("utf-8"))
                    for ancestor in merges:
                        line = "merge {}\n".format(ancestor)
                        self.output.file.write(line.encode("utf-8"))
                    
                    for line in editor.edits:
                        line = line.encode("utf-8")
                        self.output.file.writelines((line, b"\n"))
                    self.output.file.write(b"\n")
                    
                    base_rev = rev
                    base_path = path[1:]
                    init_export = False
                    gitrev = mark
        
        return gitrev

class PendingSegments:
    def __init__(self, exporter, branch, rev):
        self.exporter = exporter
        
        # List of (base, end, path), from youngest to oldest segment.
        # All revisions in each segment need importing.
        self.segments = list()
        
        self.base = (0, "")  # Default if no revisions are already exported
        get_location_segments(self.exporter, self.on_segment, branch, rev)
    
    def on_segment(self, start, end, path):
        (kstarts, runs) = self.exporter.known_branches.get(path, ((), ()))
        i = bisect_right(kstarts, end)
        # Only kstarts[:i] are all <= end. If it exists, kstart[i] > end.
        
        if i:
            # Already imported segment at index i - 1 might overlap
            run = runs[i - 1]
            base = kstarts[i - 1] + len(run) - 1  # Last imported revision
            if base >= start:
                # Not all revisions in segment are younger than base revision
                
                if base < end:
                    # Part of segment is younger, thus still needs importing
                    self.segments.append((base, end, path))
                # else: No part of segment is younger
                
                self.base = (base, path)
                self.git_base = run[-1]
                raise StopIteration()
            # else: Entire segment is younger: import all revisions
        # else: Nothing imported yet
        
        self.segments.append((start - 1, end, path))
    
    def __iter__(self):
        return reversed(self.segments)

def iter_revs(*pos, **kw):
    return closing(iter(ExportRevs(*pos, **kw)))

class ExportRevs:
    def __init__(self, exporter, path, base, end):
        self.exporter = exporter
        self.path = path
        self.prefix = self.path.rstrip("/") + "/"
        self.url = (self.exporter.repos_root + self.path).rstrip("/")
        self.rev = max(base, 0)
        self.end = end
    
    def __iter__(self):
        """
        Always ensures the RA object is parented at the branch location of
        interest before yielding."""
        
        while self.rev < self.end:
            with self.exporter.progress(self.path):
                if self.exporter.url != self.url:
                    self.exporter.ra.reparent(self.url)
                    self.exporter.url = self.url
                
                if not self.exporter.quiet:
                    stderr.write("@")
                    stderr.flush()
                next = self.rev + 1
                self.rev = None
                self.exporter.ra.get_log(self.on_revision,
                    strict_node_history=False, paths=None,
                    start=next, end=self.end, limit=1,
                    
                    # TODO: Changed paths only needed for the first revision
                    # in each segment
                    discover_changed_paths=True,
                )
                
                if self.rev is None:
                    if not self.exporter.quiet:
                        stderr.write("(none)")
                    break
                if not self.exporter.quiet:
                    stderr.write(format(self.rev))
                    stderr.flush()
                
                if (not self.exporter.export_copies and
                not any(path.startswith(self.prefix) and
                path != self.prefix for path in self.paths.keys())):
                    default = (None, None, None)
                    (_, src, _) = self.paths.get(self.path, default)
                    if src is not None:
                        continue
                
                yield (self.rev, self.date, self.author, self.log,
                    self.paths)
    
    def on_revision(self, paths, rev, props, children=False):
        self.paths = paths
        self.rev = rev
        self.date = props[PROP_REVISION_DATE]
        self.author = props.get(PROP_REVISION_AUTHOR, "(no author)")
        self.log = props[PROP_REVISION_LOG]

class RevisionSet:
    def __init__(self):
        self.branches = defaultdict(list)
    
    def update(self, other):
        for (branch, ranges) in other.branches.items():
            self.branches[branch] = list(ranges)
    
    def add_segment(self, branch, start, end):
        ranges = self.branches[branch]
        i = bisect_left(ranges, (start, 0, True))
        
        starti = i
        if i > 0:
            (rstart, rend, _) = ranges[i - 1]
            if rend + 1 >= start:
                start = rstart
                end = max(end, rend)
                starti = i - 1
        
        stopi = i
        if i < len(ranges):
            (rstart, rend, _) = ranges[i]
            if rstart <= end + 1:
                end = max(end, rend)
                stopi = i + 1
        
        ranges[starti:stopi] = ((start, end, True),)
    
    def __eq__(self, other):
        return self.branches == other.branches

class Ancestors(RevisionSet):
    def __init__(self, exporter):
        self.exporter = exporter
        RevisionSet.__init__(self)
    
    def add_natural(self, branch, rev):
        # TODO: global cache
        get_location_segments(self.exporter, self.on_segment, branch, rev)
    
    def on_segment(self, start, end, path):
        ranges = self.branches[path]
        i = bisect_left(ranges, (start, 0, True))
        # If it exists, ranges[i] is the first >= start
        if i < len(ranges):
            (rstart, rend, inheritable) = ranges[i]
            if rstart == start:
                ranges[i] = (rstart, max(rend, end), inheritable)
                raise StopIteration()
        
        ranges.insert(i, (start, end, True))

class get_location_segments:
    def __init__(self, exporter, callback, path="", rev=INVALID_REVNUM):
        self.exporter = exporter
        self.callback = callback
        
        with self.exporter.progress("location history:"):
            root_len = len(self.exporter.repos_root)
            prefix = (self.exporter.url + "/")[root_len + 1:]
            if (path + "/").startswith(prefix):
                path = path[len(prefix):]
            else:
                self.exporter.ra.reparent(self.exporter.repos_root)
                self.exporter.url = self.exporter.repos_root
            
            try:
                self.cancelled = False
                self.exporter.ra.get_location_segments(path, rev,
                    rev, INVALID_REVNUM, self.on_segment)
            except StopIteration:
                pass
    
    def on_segment(self, start, end, path):
        if self.cancelled or path is None:
            return
        if not self.exporter.quiet:
            stderr.write("\n  /{}:{}-{}".format(path, start, end))
            stderr.flush()
        try:
            self.callback(start, end, path)
        except StopIteration:
            # Do not actually cancel the Subversion operation,
            # because its Serf implementation does not handle cancellation
            # very well
            self.cancelled = True

class FastExport(Context):
    def __init__(self, *pos, **kw):
        try:
            self.nextmark = 1
            self.files = dict()
            self.open(*pos, **kw)
        except:
            self.__exit__(*exc_info())
            raise
    def open(self):
        pass
    
    def newmark(self):
        mark = ":{}".format(self.nextmark)
        self.nextmark += 1
        return mark
    
    def blob(self, path, buf):
        blob = self.blob_header(path, buf)
        self.file.writelines((buf, b"\n"))
        return blob
    
    def blob_header(self, path, buf):
        (mark, _) = self.files.get(path, (None, None))
        if mark is None:
            mark = self.newmark()
            self.files[path] = (mark,)
        
        self.file.write(b"blob\n")
        self.file.write("mark {}\n".format(mark).encode("ascii"))
        self.file.write("data {}\n".format(len(buf)).encode("ascii"))
        return mark
    
    def __setitem__(self, path, value):
        self.files[path] = value
    def __getitem__(self, path):
        return self.files[path]

class FastExportFile(FastExport):
    def __init__(self, file):
        self.filedata = dict()
        self.file = open(file, "w+b")
        FastExport.__init__(self)
    def close(self):
        return self.file.close()
    
    def blob(self, path, buf):
        self.file.seek(0, SEEK_END)
        blob = self.blob_header(path, buf)
        filedata = FileArray(self.file, self.file.tell(), len(buf))
        self.filedata[blob] = filedata
        self.file.writelines((buf, b"\n"))
        return blob
    
    def cat_blob(self, blob):
        return self.filedata[blob]

class FastExportPipe(FastExport):
    def __init__(self, importer):
        self.proc = Popen(importer,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=-1)
        FastExport.__init__(self)
    def open(self):
        self.file = self.proc.stdin
        self.file.write(b"feature done\n")
        self.file.write(b"feature cat-blob\n")
    
    def __exit__(self, type, value, traceback):
        try:
            if not value:
                self.file.write(b"done\n")
        except BaseException as err:
            value = err
        finally:
            self.proc.stdout.close()
            
            try:
                self.proc.stdin.close()
            except EnvironmentError as err:
                if err.errno != EPIPE:
                    raise
                # Underlying file descriptor seems to get closed anyway,
                # despite the broken pipe exception
                
                returncode = self.proc.wait()
                if not value and not returncode:
                    raise
            else:
                returncode = self.proc.wait()
            if (not value or isinstance(value, EnvironmentError)
            and value.errno == EPIPE) and returncode:
                raise SystemExit(returncode)
    
    def cat_blob(self, blob):
        self.file.write("cat-blob {}\n".format(blob).encode("ascii"))
        self.file.flush()
        size = int(self.proc.stdout.readline().split(b" ", 3)[2])
        data = self.proc.stdout.read(size)
        self.proc.stdout.readline()
        return data

class Editor(object):
    def close(self):
        pass

class RevEditor(Editor):
    def __init__(self, output, quiet):
        self.output = output
        self.quiet = quiet
        self.edits = list()
        self.mergeinfo = dict()
    
    def set_target_revision(self, rev):
        pass
    def open_root(self, base):
        return RootEditor(self)
    def abort(self):
        pass

class NodeEditor(Editor):
    def __init__(self, rev):
        self.rev = rev
    def change_prop(self, name, value):
        pass

class DirEditor(NodeEditor):
    def add_directory(self, path):
        if not self.rev.quiet:
            stderr.writelines(("\n  A ", path, "/"))
            stderr.flush()
        return self
    def open_directory(self, path, base):
        if not self.rev.quiet:
            stderr.writelines(("\n  M ", path, "/"))
            stderr.flush()
        return self
    
    def add_file(self, path):
        if not self.rev.quiet:
            stderr.writelines(("\n  A ", path))
            stderr.flush()
        return FileEditor(path, self.rev)
    
    def open_file(self, path, base):
        if not self.rev.quiet:
            stderr.writelines(("\n  M ", path))
            stderr.flush()
        return FileEditor(path, self.rev, original=self.rev.output[path])
    
    def delete_entry(self, path, rev=None):
        if not self.rev.quiet:
            stderr.writelines(("\n  D ", path))
            stderr.flush()
        self.rev.edits.append("D {}".format(path))

class RootEditor(DirEditor):
    def change_prop(self, name, value):
        if name == PROP_MERGEINFO:
            for (path, ranges) in parse_mergeinfo_property(value).items():
                inhranges = list()
                for range in ranges:
                    (_, _, inheritable) = range
                    if inheritable:
                        inhranges.append(range)
                if inhranges:
                    self.rev.mergeinfo[path] = inhranges

class FileEditor(NodeEditor):
    def __init__(self, path, rev, original=(None, "644")):
        NodeEditor.__init__(self, rev)
        self.path = path
        (self.blob, self.mode) = original
    
    def change_prop(self, name, value):
        if name == PROP_EXECUTABLE:
            if value:
                self.mode = "755"
            else:
                self.mode = "644"
    
    def apply_textdelta(self, base_sum):
        return DeltaWindowHandler(self)
    
    def close(self):
        self.rev.output[self.path] = (self.blob, self.mode)
        self.rev.edits.append("M {0.mode} {0.blob} {0.path}".format(self))

class DeltaWindowHandler(object):
    def __init__(self, file):
        self.file = file
        if self.file.blob:
            self.sbuf = self.file.rev.output.cat_blob(self.file.blob)
        else:
            self.sbuf = bytes()
        self.target_buf = bytearray()
    
    def __call__(self, chunk):
        if chunk is None:
            self.file.blob = self.file.rev.output.blob(self.file.path,
                self.target_buf)
        else:
            chunk = apply_txdelta_window(self.sbuf, chunk)
            self.target_buf.extend(chunk)

class FileArray(object):
    def __init__(self, file, pos, len):
        self.file = file
        self.pos = pos
        self.len = len
    
    def __getitem__(self, slice):
        return FileArray(self.file,
            self.pos + slice.start, slice.stop - slice.start)
    
    def __iter__(self):
        self.file.seek(self.pos)
        return iter(self.file.read(self.len))
    
    def __repr__(self):
        return "{}({}, {}, {})".format(type(self).__name__,
            self.file, self.pos, self.len)

@contextmanager
def progresscontext(*args):
    stderr.writelines(args)
    stderr.flush()
    try:
        yield
    finally:
        print(file=stderr)

@contextmanager
def dummycontext(*pos, **kw):
    yield

if __name__ == "__main__":
    main()
