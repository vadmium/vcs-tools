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

class Test(TestCase):
    def setUp(self):
        self.svn_fex = runpy.run_path("svn-fex")
        
        # Massive hack to restore the module's global variables, which are
        # probably all nulled out by garbage collection
        self.svn_fex["main"].__globals__.update(self.svn_fex)
        
        self.dir = mkdtemp(prefix="svn-fex-")
    
    def tearDown(self):
        rmtree(self.dir)
    
    def test_modify_branch(self):
        """Modification of branch directory properties"""
        repo = os.path.join(self.dir, "repo")
        subprocess.check_call(("svnadmin", "create", repo))
        proc = Popen(("svnadmin", "load", "--quiet", repo),
            stdin=subprocess.PIPE)
        proc.communicate(b"""\
SVN-fs-dump-format-version: 2

UUID: 00000000-0000-0000-0000-000000000000

Revision-number: 1
Prop-content-length: 73
Content-length: 73

K 8
svn:date
V 27
1970-01-01T00:00:00.000000Z
K 7
svn:log
V 0

PROPS-END

Node-action: add
Node-kind: dir
Node-path: trunk

Revision-number: 2
Prop-content-length: 73
Content-length: 73

K 8
svn:date
V 27
1970-01-01T00:00:00.000000Z
K 7
svn:log
V 0

PROPS-END

Node-action: change
Node-kind: dir
Node-path: trunk
Prop-content-length: 29
Content-length: 29

K 4
name
V 5
value
PROPS-END

""")
        if proc.returncode:
            raise SystemExit(proc.returncode)
        #~ subprocess.check_call(("svnadmin", "dump", "--quiet", repo))
        
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
        repo = os.path.join(self.dir, "repo")
        subprocess.check_call(("svnadmin", "create", repo))
        proc = Popen(("svnadmin", "load", "--quiet", repo),
            stdin=subprocess.PIPE)
        proc.communicate(b"""\
SVN-fs-dump-format-version: 2

UUID: 00000000-0000-0000-0000-000000000000

Revision-number: 1
Prop-content-length: 98
Content-length: 98

K 8
svn:date
V 27
1970-01-01T00:00:00.000000Z
K 7
svn:log
V 0

K 10
svn:author
V 4
user
PROPS-END

Node-action: add
Node-kind: file
Node-path: file

""")
        if proc.returncode:
            raise SystemExit(proc.returncode)
        #~ subprocess.check_call(("svnadmin", "dump", "--quiet", repo))
        
        url = "file://{}".format(pathname2url(repo))
        export = os.path.join(self.dir, "export")
        
        authors = os.path.join(self.dir, "authors")
        with open(authors, "wt") as file:
            print("user = user <user>", file=file)
        
        self.svn_fex["main"](url, export, "ref", authors=authors, quiet=True)

if __name__ == "__main__":
    import unittest
    unittest.main()
