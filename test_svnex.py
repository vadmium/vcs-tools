#! /usr/bin/env python3

from unittest import TestCase
from tempfile import TemporaryDirectory
import subprocess
import os.path
import svnex
from subprocess import Popen
from email.message import Message
from io import BytesIO, TextIOWrapper
from email.generator import BytesGenerator
#~ import subvertpy.delta
from functools import partial
from unittest.mock import patch
import sys
from xml.sax import saxutils

class TempDirTest(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        
        tempdir = TemporaryDirectory(prefix="svnex-")
        self.addCleanup(tempdir.cleanup)
        self.dir = tempdir.name

class RepoTests(TempDirTest):
    def make_repo(self, revs):
        dump = BytesIO()
        dump_message(dump, (("SVN-fs-dump-format-version", "2"),))
        dump_message(dump, (
            ("UUID", "00000000-0000-0000-0000-000000000000"),))
        
        for (i, rev) in enumerate(revs, 1):
            props = {
                "svn:date": "1970-01-01T00:00:00.000000Z",
                "svn:log": "",
            }
            props.update(rev.setdefault("props", dict()))
            headers = (("Revision-number", format(i)),)
            dump_message(dump, headers, props=props)
            
            for node in rev.setdefault("nodes", {}):
                headers = list()
                for name in (
                    "action", "kind", "path",
                    "copyfrom-path", "copyfrom-rev",
                ):
                    value = node.get(name.replace("-", "_"))
                    if value is not None:
                        headers.append(("Node-" + name, format(value)))
                dump_message(dump, headers,
                    props=node.get("props"), content=node.get("content"))
        dump.seek(0)
        
        log = TextIOWrapper(BytesIO(), "ascii")
        log.write("<log>")
        for [i, rev] in enumerate(reversed(revs)):
            i = format(len(revs) - i)
            log.write(f"<logentry revision={saxutils.quoteattr(i)}>")
            author = rev["props"].get("svn:author")
            if author is not None:
                log.write(f"<author>{saxutils.escape(author)}</author>")
            log.write("<date>1970-01-01T00:00:00.000000Z</date><paths>")
            for node in rev["nodes"]:
                action = {"add": "A", "change": "M", "delete": "D"}[node['action']]
                log.write(f"<path action={saxutils.quoteattr(action)}>/{saxutils.escape(node['path'])}</path>")
            log.write("</paths></logentry>")
        log.write("</log>")
        log.seek(0)
        
        return (dump, patch("svnex.stdin", log))
    
    def test_modify_branch(self):
        """Modification of branch directory properties"""
        [dump, log] = self.make_repo((
            dict(nodes=(dict(action="add", path="trunk", kind="dir"),)),
            dict(nodes=(
                dict(action="change", path="trunk", props={"name": "value"}),
            )),
        ))
        output = os.path.join(self.dir, "output")
        with svnex.FastExportFile(output) as fex, log:
            exporter = svnex.Exporter(fex, root="", quiet=True)
            exporter.export("refs/ref", "trunk")
        with open(output, "r", encoding="ascii") as output:
            self.assertMultiLineEqual("""\
commit refs/ref
mark :1
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@1 00000000-0000-0000-0000-000000000000


commit refs/ref
mark :2
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@2 00000000-0000-0000-0000-000000000000


""",
                output.read())
    
    def test_authors(self):
        """Authors mapping"""
        [dump, log] = self.make_repo((
            dict(props={"svn:author": "user"}, nodes=(
                dict(action="add", path="file", kind="file", content=b""),
            )),
        ))
        output = os.path.join(self.dir, "output")
        authors = {"user": "user <user>"}
        with svnex.FastExportFile(output) as output, log:
            exporter = svnex.Exporter(output,
                author_map=authors, quiet=True)
            exporter.export("refs/ref")
    
    def test_first_delete(self):
        """Detection of deletion in first commit"""
        [dump, log] = self.make_repo((
            dict(nodes=(
                dict(action="add", path="file", kind="file", content=b""),
                dict(action="add", path="igfile", kind="file", content=b""),
                dict(action="add", path="igdir/", kind="dir"),
                dict(action="add", path="igdir/file", kind="file",
                    content=b""),
            )),
            dict(nodes=(
                dict(action="delete", path="file"),
                dict(action="delete", path="igfile"),
                dict(action="delete", path="igdir/file"),
            )),
        ))
        output = os.path.join(self.dir, "output")
        with svnex.FastExportFile(output) as fex, log:
            exporter = svnex.Exporter(fex, root="",
                rev_map={"": {1: "refs/ref"}}, ignore=("igfile", "igdir"),
                quiet=True)
            exporter.export("refs/ref")
        with open(output, "r", encoding="ascii") as output:
            self.assertMultiLineEqual("""\
commit refs/ref
mark :1
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 54


git-svn-id: @2 00000000-0000-0000-0000-000000000000

from refs/ref
D file

""",
                output.read())
    
    def test_multiple(self):
        """Modification of multiple files"""
        [dump, log] = self.make_repo((
            dict(nodes=(
                dict(action="add", path="file1", kind="file", content=b""),
                dict(action="add", path="file2", kind="file", content=b""),
            )),
            dict(nodes=(
                dict(action="change", path="file1", content=b"mod 1\n"),
                dict(action="change", path="file2", content=b"mod 2\n"),
            )),
        ))
        git = os.path.join(self.dir, "git")
        subprocess.check_call(("git", "init", "--quiet", "--", git))
        script = 'cd "$1" && git fast-import --quiet'
        importer = ("sh", "-c", script, "--", git)
        with svnex.FastExportPipe(importer) as importer, log:
            exporter = svnex.Exporter(importer, root="", quiet=True)
            exporter.export("refs/heads/master")
        cmd = ("git", "rev-parse", "--verify", "refs/heads/master")
        rev = subprocess.check_output(cmd, cwd=git).decode("ascii").strip()
        self.assertEqual("82aeb20279a1269f048243a603b141ee0ea204e9", rev)
    
    def test_executable(self):
        """Order of setting file mode and contents should not matter"""
        with patch("svnex.RemoteAccess", ExecutableRa):
            output = os.path.join(self.dir, "output")
            stdin = BytesIO(b'<log><logentry revision="100"/></log>')
            with svnex.FastExportFile(output) as fex, \
                    patch("svnex.stdin", TextIOWrapper(stdin, "ascii")):
                exporter = svnex.Exporter(fex, quiet=True)
                exporter.export("refs/ref")
            with open(output, "r", encoding="ascii") as output:
                self.assertMultiLineEqual("""\
blob
mark :1
data 0

blob
mark :2
data 0

commit refs/ref
mark :3
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 66


git-svn-id: file:///repo@1 00000000-0000-0000-0000-000000000000

M 755 :1 file1
M 755 :2 file2

blob
mark :1
data 8
content

blob
mark :2
data 8
content

commit refs/ref
mark :4
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 66


git-svn-id: file:///repo@2 00000000-0000-0000-0000-000000000000

M 755 :1 file1
M 755 :2 file2

""",
                    output.read())
    
    def test_export_copies(self):
        """Test the "--export-copies" mode"""
        [dump, log] = self.make_repo((
            dict(nodes=(
                dict(action="add", path="trunk", kind="dir"),
                dict(action="add", path="trunk/file", kind="file",
                    content=b""),
            )),
            dict(nodes=(dict(action="add", path="branch",
                copyfrom_path="trunk", copyfrom_rev=1),)),
            dict(nodes=(dict(action="change", path="branch/file",
                content=b"mod\n"),)),
        ))
        output = os.path.join(self.dir, "output")
        with svnex.FastExportFile(output) as fex, log:
            exporter = svnex.Exporter(fex, root="",
                export_copies=True, quiet=True)
            exporter.export("refs/branch", "branch")
        with open(output, "r", encoding="ascii") as output:
            self.assertMultiLineEqual("""\
blob
mark :1
data 0

commit refs/branch
mark :2
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@1 00000000-0000-0000-0000-000000000000

M 644 :1 file

commit refs/branch
mark :3
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 61


git-svn-id: /branch@2 00000000-0000-0000-0000-000000000000


blob
mark :1
data 4
mod

commit refs/branch
mark :4
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 61


git-svn-id: /branch@3 00000000-0000-0000-0000-000000000000

M 644 :1 file

""",
                output.read())
    
    def test_branch_no_commit(self):
        """Test branching when no commits are involved"""
        [dump, log] = self.make_repo((
            dict(nodes=(
                dict(action="add", path="trunk", kind="dir"),
                dict(action="add", path="branches", kind="dir"),
                dict(action="add", path="trunk/file", kind="file",
                    content=b""),
            )),
            dict(nodes=(
                dict(action="add", path="branches/branch",
                    copyfrom_path="trunk", copyfrom_rev=1),
            )),
        ))
        output = os.path.join(self.dir, "output")
        with svnex.FastExportFile(output) as fex, log:
            rev_map = {"trunk": {1: "trunk"}}
            exporter = svnex.Exporter(fex, root="", rev_map=rev_map,
                quiet=True)
            exporter.export("refs/heads/branch", "branches/branch")
        with open(output, "r", encoding="ascii") as output:
            self.assertMultiLineEqual("""\
reset refs/heads/branch
from trunk
""",
                output.read())
    
    def test_merge(self):
        """Test a merge followed by a normal commit"""
        [dump, log] = self.make_repo((
            dict(nodes=(
                dict(action="add", path="trunk", kind="dir"),
                dict(action="add", path="trunk/file", kind="file",
                    content=b"original\n"),
            )),
            dict(nodes=(
                dict(action="add", path="branch",
                    copyfrom_path="trunk", copyfrom_rev=1),
                dict(action="change", path="branch/file",
                    content=b"branched\n"),
            )),
            dict(nodes=(
                dict(action="change", path="trunk",
                    props={"svn:mergeinfo": "/branch:2"}),
                dict(action="change", path="trunk/file",
                    content=b"branched\n"),
            )),
            dict(nodes=(
                dict(action="change", path="trunk/file",
                    content=b"normal\n"),
            )),
        ))
        output = os.path.join(self.dir, "output")
        with svnex.FastExportFile(output) as fex, log:
            exporter = svnex.Exporter(fex, root="", quiet=True)
            exporter.export("refs/trunk", "trunk")
        with open(output, "r", encoding="ascii") as output:
            self.assertMultiLineEqual("""\
blob
mark :1
data 9
original

commit refs/trunk
mark :2
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@1 00000000-0000-0000-0000-000000000000

M 644 :1 file

blob
mark :1
data 9
branched

blob
mark :1
data 9
branched

commit refs/trunk
mark :3
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 61


git-svn-id: /branch@2 00000000-0000-0000-0000-000000000000

from :2
M 644 :1 file

commit refs/trunk
mark :4
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@3 00000000-0000-0000-0000-000000000000

from :2
merge :3
M 644 :1 file

blob
mark :1
data 7
normal

commit refs/trunk
mark :5
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@4 00000000-0000-0000-0000-000000000000

M 644 :1 file

""",
                output.read())
    
    def test_first_mergeinfo(self):
        """Handling of mergeinfo in first exported commit"""
        [dump, log] = self.make_repo((
            dict(nodes=(
                dict(action="add", path="trunk", kind="dir"),
                dict(action="add", path="trunk/file", kind="file",
                    content=b"original\n"),
            )),
            dict(nodes=(
                dict(action="add", path="branch",
                    copyfrom_path="trunk", copyfrom_rev=1),
                dict(action="change", path="branch/file",
                    content=b"branched\n"),
            )),
            dict(nodes=(
                dict(action="change", path="trunk",
                    props={"svn:mergeinfo": "/branch:2"}),
                dict(action="change", path="trunk/file",
                    content=b"branched\n"),
            )),
            dict(nodes=(
                dict(action="change", path="trunk/file", content=b"new\n"),
            )),
        ))
        output = os.path.join(self.dir, "output")
        with svnex.FastExportFile(output) as fex, log:
            exporter = svnex.Exporter(fex, root="",
                rev_map={"/trunk": {3: "refs/trunk"}}, quiet=True)
            exporter.export("refs/trunk", "trunk")
        with open(output, "r", encoding="ascii") as output:
            self.assertMultiLineEqual("""\
blob
mark :1
data 4
new

commit refs/trunk
mark :2
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@4 00000000-0000-0000-0000-000000000000

from refs/trunk
M 644 :1 file

""",
                output.read())
    
    def test_first_branch(self):
        """Handling of branch copy as first exported commit"""
        [dump, log] = self.make_repo((
            dict(nodes=(
                dict(action="add", path="trunk", kind="dir"),
                dict(action="add", path="branches", kind="dir"),
                dict(action="add", path="trunk/file", kind="file",
                    content=b"initial\n"),
            )),
            dict(nodes=(
                dict(action="add", path="branch",
                    copyfrom_path="trunk", copyfrom_rev=1),
            )),
            dict(nodes=(
                dict(action="change", path="branch/file",
                    content=b"branched\n"),
            )),
        ))
        output = os.path.join(self.dir, "output")
        with svnex.FastExportFile(output) as fex, log:
            rev_map = {"trunk": {1: "trunk"}}
            exporter = svnex.Exporter(fex, root="", rev_map=rev_map,
                quiet=True)
            exporter.export("refs/branch", "branch")
        with open(output, "r", encoding="ascii") as output:
            self.assertMultiLineEqual("""\
reset refs/branch
from trunk
blob
mark :1
data 9
branched

commit refs/branch
mark :2
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 61


git-svn-id: /branch@3 00000000-0000-0000-0000-000000000000

from trunk
M 644 :1 file

""",
                output.read())

def executable_add(editor):
    root = editor.open_root(0)
    
    file = root.add_file("file1")
    delta = file.apply_textdelta(None)
    subvertpy.delta.send_stream(BytesIO(b""), delta)
    file.change_prop("svn:executable", "*")
    file.close()
    
    file = root.add_file("file2")
    file.change_prop("svn:executable", "*")
    delta = file.apply_textdelta(None)
    subvertpy.delta.send_stream(BytesIO(b""), delta)
    file.close()
    
    root.close()
    editor.close()

def executable_modify(editor):
    root = editor.open_root(1)
    
    file = root.open_file("file1", 1)
    delta = file.apply_textdelta(None)
    subvertpy.delta.send_stream(BytesIO(b"content\n"), delta)
    file.close()
    
    file = root.open_file("file2", 1)
    delta = file.apply_textdelta(None)
    subvertpy.delta.send_stream(BytesIO(b"content\n"), delta)
    file.close()
    
    root.close()
    editor.close()

class ExecutableRa:
    revs = (
        dict(
            changes={"/file": ("A", None, None)},
            diff=executable_add,
        ),
        dict(
            changes={"/file": ("M", None, None)},
            diff=executable_modify,
        ),
    )
    revprops = {
        "svn:date": "1970-01-01T00:00:00.000000Z",
        "svn:log": "",
    }
    
    def __init__(self, url, *pos, **kw):
        self.url = url
    
    def get_repos_root(self):
        return self.url
    def get_uuid(self):
        return "00000000-0000-0000-0000-000000000000"
    def get_latest_revnum(self):
        return len(self.revs)
    def reparent(self, *pos):
        pass
    
    def iter_log(self, *pos, **kw):
        revnum = len(self.revs)
        for rev in reversed(self.revs):
            changes = dict()
            for (path, value) in rev["changes"].items():
                changes[path] = value + (None,)
            yield (changes, revnum, self.revprops, False)
            revnum -= 1
    
    def get_location_segments(self, path, peg, start, end, rcvr):
        rcvr(0, 2, "")
    
    def get_log(self, callback, paths, start, *pos, **kw):
        changes = self.revs[start - 1]["changes"]
        callback(changes, start, self.revprops, False)
    
    def do_diff(self, targetrev, path, targeturl, editor, *pos):
        diff = self.revs[targetrev - 1]["diff"]
        return ExecutableReporter(partial(diff, editor))

class ExecutableReporter:
    def __init__(self, diff):
        self.finish = diff
    def set_path(self, *pos):
        pass

class TestAuthorsFile(TempDirTest):
    """Parsing authors file"""
    def runTest(self):
        authors = os.path.join(self.dir, "authors")
        with open(authors, "w") as file:
            file.write(
                "user = Some Body <whoever@where.ever>\n"
                "tricky = E = mc squared\n"
            )
        
        output = os.path.join(self.dir, "output")
        stdin = BytesIO(b'<log><logentry revision="100"/></log>')
        with patch("svnex.Exporter", self.Exporter), \
                patch("svnex.stdin", TextIOWrapper(stdin, "ascii")):
            svnex.main("dummy",
                file=output, git_ref="refs/ref", authors_file=authors)
        
        self.assertEqual(dict(
            user="Some Body <whoever@where.ever>",
            tricky="E = mc squared",
        ), self.author_map)
    
    def Exporter(self, *pos, author_map, **kw):
        self.author_map = author_map
        return self.MockExporter()
    
    class MockExporter:
        def export(self, *pos, **kw):
            pass

def dump_message(file, headers, props=None, content=None):
    msg = Message()
    for (name, value) in headers:
        msg[name] = value
    payload = BytesIO()
    
    if props is not None:
        start = payload.tell()
        for (key, value) in props.items():
            payload.write("K {}\n".format(len(key)).encode("ascii"))
            payload.writelines((key.encode("ascii"), b"\n"))
            payload.write("V {}\n".format(len(value)).encode("ascii"))
            payload.writelines((value.encode("ascii"), b"\n"))
        payload.write(b"PROPS-END\n")
        
        msg["Prop-content-length"] = format(payload.tell() - start)
    
    if content is not None:
        msg["Text-content-length"] = format(len(content))
        payload.write(content)
    
    if props is not None or content is not None:
        payload = payload.getvalue()
        msg["Content-length"] = format(len(payload))
        
        # Workaround for Python issue 18324, "set_payload doe snot handle
        # binary payloads correctly", http://bugs.python.org/issue18324
        msg.set_payload(payload.decode("ascii", "surrogateescape"))
    
    BytesGenerator(file, mangle_from_=False).flatten(msg)

if __name__ == "__main__":
    import unittest
    unittest.main()
