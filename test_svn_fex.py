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
            dump_message(proc.stdin, (("Revision-number", str(i)),), props)
            
            for node in rev.get("nodes", {}):
                headers = list()
                for name in ("action", "kind", "path"):
                    value = node.get(name)
                    if value is not None:
                        headers.append(("Node-" + name, value))
                dump_message(proc.stdin, headers, node.get("props"))
        
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
        self.svn_fex["main"](url, export, "ref", root="", quiet=True)
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
                dict(action="add", path="file", kind="file"),
            )),
        ))
        url = "file://{}".format(pathname2url(repo))
        export = os.path.join(self.dir, "export")
        
        authors = os.path.join(self.dir, "authors")
        with open(authors, "wt") as file:
            print("user = user <user>", file=file)
        
        self.svn_fex["main"](url, export, "ref", authors=authors, quiet=True)
    
    def test_first_delete(self):
        """Detection of deletion in first commit"""
        repo = self.make_repo((
            dict(nodes=(dict(action="add", path="file", kind="file"),)),
            dict(nodes=(dict(action="delete", path="file"),)),
        ))
        url = "file://{}".format(pathname2url(repo))
        export = os.path.join(self.dir, "export")
        self.svn_fex["main"](url, export, "ref", root="", revision="1:HEAD",
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

def dump_message(file, headers, props=None):
    msg = Message()
    for (name, value) in headers:
        msg[name] = value
    
    if props is not None:
        payload = BytesIO()
        for (key, value) in props.items():
            print(b"K", len(key), file=payload)
            print(key, file=payload)
            print(b"V", len(value), file=payload)
            print(value, file=payload)
        print(b"PROPS-END", file=payload)
        payload = payload.getvalue()
        
        msg["Prop-content-length"] = str(len(payload))
        msg["Content-length"] = str(len(payload))
        msg.set_payload(payload)
    
    Generator(file, mangle_from_=False).flatten(msg)

if __name__ == "__main__":
    import unittest
    unittest.main()
