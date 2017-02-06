#! /usr/bin/env python3

'''See main() function'''

#~ from subvertpy.properties import (
    #~ PROP_EXECUTABLE,
    #~ PROP_MERGEINFO,
#~ )
from sys import stderr, exc_info, stdin
from io import SEEK_END
from subprocess import Popen
import subprocess
from errno import EPIPE
from contextlib import contextmanager
from collections import defaultdict
from bisect import bisect_right, bisect_left
from contextlib import closing
#~ from subvertpy.properties import parse_mergeinfo_property
#~ from subvertpy.properties import generate_mergeinfo_property
from misc import Context
from xml.etree import ElementTree
from _common import parse_path, read_record
from datetime import datetime, timezone
from io import BytesIO
from hashlib import md5

def main(
    dump: dict(help="Subversion dump filename"),
    branch: dict(metavar="/path[@rev]", help="Subversion branch"),
    importer: dict(mutex_required="output",
        help="command to pipe fast import stream to") = (),
    *,
    file: dict(mutex_required="output",
        metavar="FILENAME", help="fast import file") = None,
    git_ref: dict(metavar="REFNAME",
        help="Git ref name to export to (e.g. refs/remotes/svn/trunk)"),
    rev_map: dict(metavar="FILENAME",
        help="""file mapping from Subversion paths and revisions
        to existing Git revisions,
        each line formatted as PATH@SVN-REV (space) GIT-REV""") = None,
    authors_file: dict(short="-A", metavar="FILENAME", help=
        'file mapping Subversion user names to Git authors, like "git-svn"')
        = None,
    rewrite_root: dict(metavar="URL",
        help="Subversion URL to store in the metadata") = "",
    git_svn: dict(help="include git-svn-id lines") = False,
    ignore: dict(
        metavar="PATH", help="add a path to be excluded from export") = (),
    export_copies: dict(help='''export simple branch copies even when no
        files were modified''') = False,
    quiet: dict(short="-q", help="suppress progress messages") = False,
):
    '''Converts a Subversion repository to Git's "fast-import" format
    
    The program can:
    
    * follow branch copies
    * produce identical commits to "git-svn", except that it
        * does not merge new branches and tags with deleted paths
        * optionally drops commits that are simple branch copies
    * be run incrementally???
    * handle Subversion merge tracking information
    
    It does not (yet):
    
    * handle or correlate multiple trunks, branches, or tags
    * handle symbolic links, although it does handle executable files
    * do anything else with special Subversion file or revision properties
    '''
    
    branch = branch.rsplit("@", 1)
    if len(branch) > 1:
        peg_rev = branch.pop()
        if peg_rev:
            peg_rev = int(peg_rev)
        else:
            peg_rev = INVALID_REVNUM
    else:
        peg_rev = INVALID_REVNUM
    [branch] = branch
    branch = branch.lstrip("/")
    
    rev_map_data = defaultdict(dict)
    if rev_map is not None:
        with open(rev_map, "rt") as file:
            for s in file:
                (s, gitrev) = s.rstrip("\n").rsplit(" ", 1)
                (branch, svnrev) = s.rsplit("@", 1)
                svnrev = int(svnrev)
                rev_map_data[branch][svnrev] = gitrev
    
    if authors_file is not None:
        author_map = dict()
        with open(authors_file, "rt") as f:
            for line in f:
                if line.endswith("\n"):
                    line = line[:-1]
                (svn, git) = line.split(" = ", 1)
                author_map[svn] = git
    else:
        author_map = None
    
    if importer:
        output = FastExportPipe(importer)
    else:
        output = FastExportFile(file)
    with output, open(dump, "rb") as dump:
        exporter = Exporter(dump, output,
            rev_map=rev_map_data,
            author_map=author_map,
            root=rewrite_root,
            ignore=ignore,
            git_svn=git_svn, export_copies=export_copies,
            quiet=quiet,
        )
        exporter.export(git_ref, branch, peg_rev)

class Exporter:
    def __init__(self, dump, output,
        rev_map={},
        author_map=None,
        root="",
        ignore=(),
        git_svn=False, export_copies=False,
        quiet=False,
    ):
        self.output = output
        self.author_map = author_map
        self.ignore = ignore
        self.git_svn = git_svn
        self.export_copies = export_copies
        
        self.known_branches = defaultdict(lambda: (list(), list()))
        for (branch, revs) in rev_map.items():
            branch = branch.lstrip("/")
            (starts, runs) = self.known_branches[branch]
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
        
        with self.progress("loading log:"):
            self._svnlog = ElementTree.parse(stdin.buffer).getroot()
            first = self._svnlog[0].get("revision")
            last = self._svnlog[-1].get("revision")
            self.log(f" r{first}:{last}")
        
        self.root = root
        
        self.dump = dump
        [header, content] = read_record(dump)
        assert header.keys() == ["SVN-fs-dump-format-version"]
        [header, content] = read_record(dump)
        [[field, self.uuid]] = header.items()
        assert field == "UUID"
        self._header = None
    
    def export(self, git_ref, branch="", rev=None):
        self.git_ref = git_ref
        segments = PendingSegments(self, branch, rev)
        
        (base_rev, base_path) = segments.base
        if base_rev:
            gitrev = segments.git_base
        else:
            gitrev = None
        
        init_export = True
        for (base, end, path) in segments:
            path = "/" + path
            prefix = path.rstrip("/") + "/"
            with iter_revs(self, path, base, end) as revs:
                for (svnrev, date, author, self.paths) in revs:
                    commit = self.export_copies
                    
                    # Assuming we are only interested in "trunk":
                    # A /proj2/trunk from /proj1/trunk -> no commit
                    # A /proj2 from /proj1 -> no commit
                    # A /trunk without copy -> commit
                    # A /proj/trunk from /proj/branch -> no commit
                    commit = commit or any(path.startswith(prefix) and
                        path > prefix for path in self.paths.keys())
                    if not commit:
                        default = (None, None, None)
                        (_, src, _) = self.paths.get(path, default)
                        commit = src is None
                    
                    if commit:
                        new = self.commit(svnrev, date, author,
                            init_export=init_export,
                            base_rev=base_rev, base_path=base_path,
                            gitrev=gitrev,
                            path=path, prefix=prefix,
                        )
                        if new:
                            gitrev = new
                            init_export = False
                    else:
                        self.log(": no changes")
                        self.output.printf("reset {}", git_ref)
                        self.output.printf("from {}", gitrev)
                    
                    base_rev = svnrev
                    base_path = path[1:]
                    
                    # Remember newly exported Git revision
                    (svnstarts, gitruns) = self.known_branches[base_path]
                    i = bisect_left(svnstarts, base_rev)
                    if (i > 0 and
                    svnstarts[i - 1] + len(gitruns[i - 1]) == base_rev):
                        gitruns[i - 1].append(gitrev)
                    else:
                        svnstarts.insert(i, base_rev)
                        gitruns.insert(i, [gitrev])
        
        return gitrev
    
    def commit(self, rev, date, author, *,
    init_export, base_rev, base_path, gitrev, path, prefix):
        self.log(":")
        edits = list()
        mergeinfo = dict()
        
        for (file, (action, _, _)) in self.paths.items():
            if not file.startswith(prefix) or action not in "DR":
                continue
            file = file[len(prefix):]
            for p in self.ignore:
                if file == p or file.startswith((p + "/").lstrip("/")):
                    break
            else:
                dir.delete_entry(file)
        
        r = None
        while True:
            if self._header is None:
                [header, self._content] = read_record(self.dump)
            else:
                header = self._header
                self._header = None
            # Tolerate concatenated dumps
            if header.items() == [("SVN-fs-dump-format-version", "3")]:
                [header, content] = read_record(self.dump)
                assert header.items() == [("UUID", self.uuid)]
                [header, self._content] = read_record(self.dump)
            if "Node-path" in header:
                continue
            r = int(header["Revision-number"])
            if r >= rev:
                break
        if r != rev:
            raise LookupError(f"Revision {rev} not found in dump file")
        revprops = self._content
        while True:
            if not revprops.startswith(b"K "):
                break
            [length, revprops] = revprops[2:].split(b"\n", 1)
            length = int(length)
            name = revprops[:length]
            assert revprops.startswith(b"\nV ", length)
            [length, revprops] = revprops[length + 3:].split(b"\n", 1)
            length = int(length)
            if name == b"svn:log":
                log = revprops[:length].decode("ascii")
            assert revprops.startswith(b"\n", length)
            revprops = revprops[length + 1:]
        assert revprops == b"PROPS-END\n"
        
        for p in self.ignore:
            reporter.set_path(p, INVALID_REVNUM, True, None,
                subvertpy.ra.DEPTH_EXCLUDE)
        
        while True:
            [self._header, self._content] = read_record(self.dump)
            p = self._header.get_all("Node-path")
            if not p:
                break
            
            [p] = p
            p = "/" + p
            [action, from_path, from_rev] = self.paths.pop(p)
            if not p.startswith(prefix) and p != path:
                continue
            assert frozenset(self._header.keys()) < {
                "Node-path", "Node-kind", "Node-action",
                "Node-copyfrom-path", "Node-copyfrom-rev", "Prop-delta",
                "Text-delta", "Text-delta-base-md5", "Text-content-md5",
                "Prop-content-length", "Text-content-length",
                "Content-length",
            }
            assert action == {"add": "A", "change": "M"}[self._header.get("Node-action")]
            assert from_path is from_rev is None
            [kind] = self._header.get_all("Node-kind")
            if kind == "dir":
                assert action == "A"
                if not self.quiet:
                    stderr.writelines(("\n  A ", p, "/"))
            else:
                assert kind == "file"
                if not self.quiet:
                    stderr.write(f"\n  {action} {p}")
                p = p[len(prefix):]
                [target] = self._header.get_all("Prop-content-length", (0,))
                target = self._content[int(target):]
                if self._header.get("Text-delta") == "true":
                    if action == "M":
                        [source, mode] = self.output[p]
                        source = self.output.cat_blob(source)
                        [hash] = self._header.get_all("Text-delta-base-md5")
                        assert md5(source).hexdigest() == hash
                    else:
                        source = None
                    delta = BytesIO(target)
                    header = delta.read(4)
                    assert header == b"SVN\x00"
                    source_offset = read_int(delta)
                    assert source_offset == 0
                    source_length = read_int(delta)
                    target = read_int(delta)
                    instr_length = read_int(delta)
                    data = read_int(delta)
                    instr_data = delta.read(instr_length)
                    assert len(instr_data) == instr_length
                    instr_data = BytesIO(instr_data)
                    [instr] = instr_data.read(1)
                    assert not instr & 0x3F
                    copy = read_int(instr_data)
                    instr >>= 6
                    SOURCE = 0
                    NEW = 2
                    if instr == SOURCE:
                        offset = read_int(instr_data)
                        assert offset == 0
                        target = source[:copy]
                    else:
                        assert instr == NEW
                        assert copy == data
                        target = delta.read(copy)
                        assert len(target) == copy
                    assert not instr_data.read(1)
                    assert not delta.read(1)
                    [hash] = self._header.get_all("Text-content-md5")
                    assert md5(target).hexdigest() == hash
                blob = self.output.blob(p, target)
                self.output[p] = (blob, "644")
                edits.append(f"M 644 {blob} {p}")
            stderr.flush()
        if not edits:
            self.log("\n  => commit skipped")
            return None
        assert not self.paths
        
        merges = list()
        if mergeinfo:
            self.log("\n")
            basehist = Ancestors(self)
            if base_rev:
                basehist.add_natural(base_path, base_rev)
            merged = RevisionSet()
            ancestors = Ancestors(self)
            merged.update(basehist)
            mergeinfo = mergeinfo.items()
            for (branch, ranges) in mergeinfo:
                for (start, end, _) in ranges:
                    merged.add_segment(branch, start, end)
                    ancestors.add_natural(branch, end)
            if merged != basehist and ancestors == merged:
                # TODO: minimise so that only independent branch heads are listed
                # i.e. do not explicitly merge C if also merging A and B, and C is an ancestor of both A and B
                for (branch, ranges) in mergeinfo:
                    branch = branch.lstrip("/")
                    for (_, end, _) in ranges:
                        ancestor = self.export(self.git_ref, branch, end)
                        if ancestor is not None:
                            merges.append(ancestor)
        
        self.output.printf("commit {}", self.git_ref)
        
        mark = self.output.newmark()
        self.output.printf("mark {}", mark)
        
        date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        date = int(date.replace(tzinfo=timezone.utc).timestamp())
        
        if self.author_map is None:
            author = "{author} <{author}@{uuid}>".format(
                author=author, uuid=self.uuid)
        else:
            author = self.author_map[author]
        
        self.output.printf("committer {} {} +0000", author, date)
        
        if self.git_svn:
            log = "{}\n\ngit-svn-id: {}{}@{} {}\n".format(
                log, self.root, path.rstrip("/"), rev, self.uuid)
        log = log.encode("utf-8")
        self.output.printf("data {}", len(log))
        self.output.file.write(log)
        self.output.printf("")
        
        if (init_export or merges) and gitrev is not None:
            self.output.printf("from {}", gitrev)
        for ancestor in merges:
            self.output.printf("merge {}", ancestor)
        
        for line in edits:
            self.output.printf("{}", line)
        self.output.printf("")
        
        return mark
    
    def log(self, message):
        if not self.quiet:
            stderr.write(message)
            stderr.flush()

class PendingSegments:
    def __init__(self, exporter, branch, rev=None):
        # List of (base, end, path), from youngest to oldest segment.
        # All revisions in each segment need importing.
        self.segments = list()
        
        self.base = (0, "")  # Default if no revisions are already exported
        segments = iter_location_segments(exporter, branch, rev)
        for [start, end, path] in segments:
            (kstarts, runs) = exporter.known_branches.get(path, ((), ()))
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
                    return
                # else: Entire segment is younger: import all revisions
            # else: Nothing imported yet
            
            self.segments.append((start - 1, end, path))
    
    def __iter__(self):
        return reversed(self.segments)
    
    def __repr__(self):
        segs = ("{}:{}->{}".format(path, base, end) for
            (base, end, path) in self)
        return "<{} {}>".format(type(self).__name__, ", ".join(segs))

def iter_revs(*pos, **kw):
    return closing(iter(ExportRevs(*pos, **kw)))

def ExportRevs(exporter, path, base, end):
    prefix = path.rstrip("/") + "/"
    rev = max(base, 0)
    
    path_tuple = parse_path(path)
    entries = reversed(exporter._svnlog)
    while rev < end:
        with exporter.progress(path):
            exporter.log("@")
            next = rev + 1
            found = False
            for entry in entries:
                rev = int(entry.get("revision"))
                if rev < next:
                    continue
                paths = entry.find("paths")
                if paths is None:
                    continue
                if rev > end:
                    break
                if any(parse_path(p.text)[:len(path_tuple)] == path_tuple
                        for p in paths):
                    found = True
                    break
            
            if not found:
                exporter.log("(none)")
                break
            exporter.log(format(rev))
            
            author = entry.find("author")
            if author is None:
                author = "(no author)"
            else:
                author = author.text
            path_map = dict()
            for p in paths:
                path_map[p.text] = (p.get("action"),
                    p.get("copyfrom-path"), p.get("copyfrom-rev"))
            yield (rev, entry.find("date").text, author, path_map)

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
    
    def __repr__(self):
        return generate_mergeinfo_property(self.branches)

class Ancestors(RevisionSet):
    def __init__(self, exporter):
        self.exporter = exporter
        RevisionSet.__init__(self)
    
    def add_natural(self, branch, rev):
        branch = branch.lstrip("/")
        # TODO: global cache
        get_location_segments(self.exporter, self.on_segment, branch, rev)
    
    def on_segment(self, start, end, path):
        path = "/" + path
        ranges = self.branches[path]
        i = bisect_left(ranges, (start, 0, True))
        # If it exists, ranges[i] is the first >= start
        if i < len(ranges):
            (rstart, rend, inheritable) = ranges[i]
            if rstart == start:
                ranges[i] = (rstart, max(rend, end), inheritable)
                raise StopIteration()
        
        ranges.insert(i, (start, end, True))

def iter_location_segments(exporter, path="", rev=None):
    loc = f"/{path}"
    if rev is not None:
        loc = f"{loc}@{rev}"
    with exporter.progress(f"{loc} location history:"):
        found = None
        for entry in exporter._svnlog:
            entry_rev = int(entry.get("revision"))
            if rev is None:
                rev = entry_rev
                loc = f"{loc}@{rev}"
            if entry_rev > rev:
                continue
            paths = entry.find("paths")
            if paths is None:
                continue
            for p in paths:
                if p.text.lstrip("/") == path:
                    assert found is None
                    found = p
            if found is not None:
                break
        else:
            if path == "":
                entry_rev = 0
            else:
                raise LookupError(f"Location {loc} not found")
        if found is not None:
            assert found.get("action") == "A"
            assert found.get("copyfrom-rev") is None
        exporter.log("\n  /{}:{}-{}".format(path, entry_rev, rev))
        yield (entry_rev, rev, path)

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
    
    def printf(self, format, *pos, **kw):
        line = format.format(*pos, **kw).encode("utf-8")
        self.file.writelines((line, b"\n"))
    
    def blob(self, path, buf):
        blob = self.blob_header(path, buf)
        self.file.write(buf)
        self.printf("")
        return blob
    
    def blob_header(self, path, buf):
        (mark, _) = self.files.get(path, (None, None))
        if mark is None:
            mark = self.newmark()
            self.files[path] = (mark,)
        
        self.printf("blob")
        self.printf("mark {}", mark)
        self.printf("data {}", len(buf))
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
        self.file.write(buf)
        self.printf("")
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
        self.printf("feature done")
        self.printf("feature cat-blob")
    
    def __exit__(self, type, value, traceback):
        try:
            if not value:
                self.printf("done")
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
        self.printf("cat-blob {}", blob)
        self.file.flush()
        size = int(self.proc.stdout.readline().split(b" ", 3)[2])
        data = self.proc.stdout.read(size)
        self.proc.stdout.readline()
        return data

class DirEditor:
    def open_directory(self, path, base):
        if not self.rev.quiet:
            stderr.writelines(("\n  M ", path, "/"))
            stderr.flush()
        return self
    
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

class FileEditor:
    def change_prop(self, name, value):
        if name == PROP_EXECUTABLE:
            if value:
                self.mode = "755"
            else:
                self.mode = "644"

def read_int(stream):
    i = 0
    while True:
        [byte] = stream.read(1)
        i = i << 7 | byte & 0x7F
        if not byte & 0x80:
            return i

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
    from _common import run_cli
    run_cli(main)
