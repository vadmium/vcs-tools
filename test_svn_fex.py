#! /usr/bin/env python3

from unittest import TestCase
from tempfile import mkdtemp
from shutil import rmtree
import subprocess
import os.path
from urllib.request import pathname2url
import runpy
from subprocess import Popen
from subvertpy.properties import (
    PROP_REVISION_DATE,
    PROP_REVISION_AUTHOR,
    PROP_REVISION_LOG,
    PROP_EXECUTABLE, PROP_EXECUTABLE_VALUE,
)
from email.message import Message
from io import BytesIO
from email.generator import BytesGenerator
import subvertpy.delta
from functools import partial
from shorthand import substattr
import sys

class BaseTest(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self.svn_fex = runpy.run_path("svn-fex")
        
        # Massive hack to restore the module's global variables, which are
        # probably all nulled out by garbage collection
        self.svn_fex["main"].__globals__.update(self.svn_fex)
        
        self.dir = mkdtemp(prefix="svn-fex-")
    
    def tearDown(self):
        rmtree(self.dir)
        TestCase.tearDown(self)

class Test(BaseTest):
    def make_repo(self, revs):
        repo = os.path.join(self.dir, "repo")
        subprocess.check_call(("svnadmin", "create", repo))
        proc = Popen(("svnadmin", "load", "--quiet", repo),
            stdin=subprocess.PIPE)
        dump_message(proc.stdin, (("SVN-fs-dump-format-version", "2"),))
        dump_message(proc.stdin, (
            ("UUID", "00000000-0000-0000-0000-000000000000"),))
        
        for (i, rev) in enumerate(revs, 1):
            props = {
                PROP_REVISION_DATE: "1970-01-01T00:00:00.000000Z",
                PROP_REVISION_LOG: "",
            }
            props.update(rev.get("props", ()))
            headers = (("Revision-number", format(i)),)
            dump_message(proc.stdin, headers, props=props)
            
            for node in rev.get("nodes", {}):
                headers = list()
                for name in (
                    "action", "kind", "path",
                    "copyfrom-path", "copyfrom-rev",
                ):
                    value = node.get(name.replace("-", "_"))
                    if value is not None:
                        headers.append(("Node-" + name, format(value)))
                dump_message(proc.stdin, headers,
                    props=node.get("props"), content=node.get("content"))
        
        proc.communicate()
        if proc.returncode:
            raise SystemExit(proc.returncode)
        return repo
    
    def test_modify_branch(self):
        """Modification of branch directory properties"""
        repo = self.make_repo((
            dict(nodes=(dict(action="add", path="trunk", kind="dir"),)),
            dict(nodes=(
                dict(action="change", path="trunk", props={"name": "value"}),
            )),
        ))
        url = "file://{}/trunk".format(pathname2url(repo))
        export = os.path.join(self.dir, "export")
        self.svn_fex["Repo"](url, "ref", file=export, root="", quiet=True)
        with open(export, "r", encoding="ascii") as export:
            self.assertMultiLineEqual("""\
commit refs/ref
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@1 00000000-0000-0000-0000-000000000000


commit refs/ref
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@2 00000000-0000-0000-0000-000000000000


""",
                export.read())
    
    def test_authors(self):
        """Authors mapping"""
        repo = self.make_repo((
            dict(props={PROP_REVISION_AUTHOR: "user"}, nodes=(
                dict(action="add", path="file", kind="file", content=b""),
            )),
        ))
        url = "file://{}".format(pathname2url(repo))
        export = os.path.join(self.dir, "export")
        authors = {"user": "user <user>"}
        self.svn_fex["Repo"](url, "ref", file=export, author_map=authors,
            quiet=True)
    
    def test_first_delete(self):
        """Detection of deletion in first commit"""
        repo = self.make_repo((
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
        url = "file://{}".format(pathname2url(repo))
        export = os.path.join(self.dir, "export")
        self.svn_fex["Repo"](url, "ref", file=export, root="", base_rev=1,
            ignore=("igfile", "igdir"), quiet=True)
        with open(export, "r", encoding="ascii") as export:
            self.assertMultiLineEqual("""\
commit refs/ref
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 54


git-svn-id: @2 00000000-0000-0000-0000-000000000000

from ref
D file

""",
                export.read())
    
    def test_multiple(self):
        """Modification of multiple files"""
        repo = self.make_repo((
            dict(nodes=(
                dict(action="add", path="file1", kind="file", content=b""),
                dict(action="add", path="file2", kind="file", content=b""),
            )),
            dict(nodes=(
                dict(action="change", path="file1", content=b"mod 1\n"),
                dict(action="change", path="file2", content=b"mod 2\n"),
            )),
        ))
        url = "file://{}".format(pathname2url(repo))
        git = os.path.join(self.dir, "git")
        subprocess.check_call(("git", "init", "--quiet", "--", git))
        script = 'cd "$1" && git fast-import --quiet'
        importer = ("sh", "-c", script, "--", git)
        self.svn_fex["Repo"](url, "heads/master", importer=importer, root="",
            quiet=True)
        cmd = ("git", "rev-parse", "--verify", "refs/heads/master")
        rev = subprocess.check_output(cmd, cwd=git).decode("ascii").strip()
        self.assertEqual("82aeb20279a1269f048243a603b141ee0ea204e9", rev)
    
    def test_executable(self):
        """Order of setting file mode and contents should not matter"""
        self.svn_fex["main"].__globals__["RemoteAccess"] = ExecutableRa
        export = os.path.join(self.dir, "export")
        self.svn_fex["Repo"]("file:///repo", "ref", file=export, quiet=True)
        with open(export, "r", encoding="ascii") as export:
            self.assertMultiLineEqual("""\
blob
mark :1
data 0

blob
mark :2
data 0

commit refs/ref
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
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 66


git-svn-id: file:///repo@2 00000000-0000-0000-0000-000000000000

M 755 :1 file1
M 755 :2 file2

""",
                export.read())
    
    def test_export_copies(self):
        """Test the "--export-copies" mode"""
        repo = self.make_repo((
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
        url = "file://{}/branch".format(pathname2url(repo))
        export = os.path.join(self.dir, "export")
        self.svn_fex["Repo"](url, "branch", file=export, root="",
            export_copies=True, quiet=True)
        with open(export, "r", encoding="ascii") as export:
            self.assertMultiLineEqual("""\
blob
mark :1
data 0

commit refs/branch
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@1 00000000-0000-0000-0000-000000000000

M 644 :1 file

commit refs/branch
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 61


git-svn-id: /branch@2 00000000-0000-0000-0000-000000000000


blob
mark :1
data 4
mod

commit refs/branch
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 61


git-svn-id: /branch@3 00000000-0000-0000-0000-000000000000

M 644 :1 file

""",
                export.read())

def executable_add(editor):
    root = editor.open_root(0)
    
    file = root.add_file("file1")
    delta = file.apply_textdelta(None)
    subvertpy.delta.send_stream(BytesIO(b""), delta)
    file.change_prop(PROP_EXECUTABLE, PROP_EXECUTABLE_VALUE)
    file.close()
    
    file = root.add_file("file2")
    file.change_prop(PROP_EXECUTABLE, PROP_EXECUTABLE_VALUE)
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
        PROP_REVISION_DATE: "1970-01-01T00:00:00.000000Z",
        PROP_REVISION_LOG: "",
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

class TestAuthorsFile(BaseTest):
    """Parsing authors file"""
    def runTest(self):
        authors = os.path.join(self.dir, "authors")
        with open(authors, "w") as file:
            file.write(
                "user = Some Body <whoever@where.ever>\n"
                "tricky = E = mc squared\n"
            )
        
        output = os.path.join(self.dir, "output")
        argv = ["svn-fex", "--git-ref", "refs/ref", "--authors", authors,
            "--file", output, "file:///dummy"]
        with substattr(sys, "argv", argv):
            self.svn_fex["main"].__globals__["Repo"] = self.Repo
            self.svn_fex["main"]()
        
        self.assertEqual(dict(
            user="Some Body <whoever@where.ever>",
            tricky="E = mc squared",
        ), self.author_map)
    
    def Repo(self, *pos, author_map, **kw):
        self.author_map = author_map

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
