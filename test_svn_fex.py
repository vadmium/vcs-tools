#! /usr/bin/env python2
from __future__ import print_function

from unittest import TestCase
from tempfile import mkdtemp
from shutil import rmtree
import subprocess
import os.path
from urllib import pathname2url
import runpy
from subprocess import Popen
from subvertpy.properties import (
    PROP_REVISION_DATE,
    PROP_REVISION_AUTHOR,
    PROP_REVISION_LOG,
)
from email.message import Message
from io import BytesIO
from email.generator import Generator

class Test(TestCase):
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
            headers = (("Revision-number", str(i)),)
            dump_message(proc.stdin, headers, props=props)
            
            for node in rev.get("nodes", {}):
                headers = list()
                for name in ("action", "kind", "path"):
                    value = node.get(name)
                    if value is not None:
                        headers.append(("Node-" + name, value))
                dump_message(proc.stdin, headers,
                    props=node.get("props"), content=node.get("content"))
        
        proc.communicate()
        if proc.returncode:
            raise SystemExit(proc.returncode)
        #~ subprocess.check_call(("svnadmin", "dump", "--quiet", repo))
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
        with open(export, "rb") as export:
            self.assertMultiLineEqual(export.read(), b"""\
commit refs/ref
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@1 00000000-0000-0000-0000-000000000000


commit refs/ref
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 60


git-svn-id: /trunk@2 00000000-0000-0000-0000-000000000000


""")
    
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
            )),
            dict(nodes=(dict(action="delete", path="file"),)),
        ))
        url = "file://{}".format(pathname2url(repo))
        export = os.path.join(self.dir, "export")
        self.svn_fex["Repo"](url, "ref", file=export, root="", base_rev=1,
            quiet=True)
        with open(export, "rb") as export:
            self.assertMultiLineEqual(export.read(), b"""\
commit refs/ref
committer (no author) <(no author)@00000000-0000-0000-0000-000000000000> 0 +0000
data 54


git-svn-id: @2 00000000-0000-0000-0000-000000000000

from ref
D file

""")
    
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
        rev = subprocess.check_output(cmd, cwd=git).strip()
        self.assertEqual(rev, "82aeb20279a1269f048243a603b141ee0ea204e9")

def dump_message(file, headers, props=None, content=None):
    msg = Message()
    for (name, value) in headers:
        msg[name] = value
    payload = BytesIO()
    
    if props is not None:
        start = payload.tell()
        for (key, value) in props.items():
            print(b"K", len(key), file=payload)
            print(key, file=payload)
            print(b"V", len(value), file=payload)
            print(value, file=payload)
        print(b"PROPS-END", file=payload)
        
        msg["Prop-content-length"] = str(payload.tell() - start)
    
    if content is not None:
        msg["Text-content-length"] = str(len(content))
        payload.write(content)
    
    if props is not None or content is not None:
        payload = payload.getvalue()
        msg["Content-length"] = str(len(payload))
        msg.set_payload(payload)
    Generator(file, mangle_from_=False).flatten(msg)

if __name__ == "__main__":
    import unittest
    unittest.main()
