#! /usr/bin/env python3

import subprocess
import sys
from uuid import UUID

def main(*roots, uuid=()):
    """Generates a file for "svnex --rev-map" from a Git repository
    
    roots: Collection of repository root URLs
    uuid: Collection of repository UUIDs to match
    
    Standard input should be a list of Git revisions, such as from "git
    rev-list --all"."""
    
    if not roots:
        raise SystemExit("Need at least one root URL to match against")
    uuids = set(map(UUID, uuid))
    
    input = sys.stdin.detach()
    catfile = ("git", "cat-file", "--batch")
    with subprocess.Popen(catfile,
    stdin=input, stdout=subprocess.PIPE, bufsize=-1) as catfile:
        input.close()
        
        fail = False
        for header in catfile.stdout:
            (rev, type, length) = header.split(maxsplit=3)[:3]
            if type != b"commit":
                msg = "Unexpected Git object type {!r}"
                raise SystemExit(msg.format(type.decode("ascii")))
            rev = rev.decode("ascii")
            length = int(length)
            
            while True:
                line = next(catfile.stdout)
                length -= len(line)
                if not line.strip():
                    break
            
            for line in catfile.stdout.read(length).splitlines():
                prefix = b"git-svn-id:"
                if not line.lower().startswith(prefix):
                    continue
                line = line[len(prefix):].strip()
                
                (url, uuid) = line.split(maxsplit=2)[:2]
                uuid = uuid.decode("ascii")
                if uuids and UUID(uuid) not in uuids:
                    continue
                
                url = url.decode("ascii")
                for root in roots:
                    if url.startswith(root):
                        break
                else:
                    msg = "Cannot determine root: {}".format(url)
                    print(msg, file=sys.stderr)
                    fail = True
                    continue
                path = url[len(root):]
                
                if not path.rindex("@"):
                    path = "/" + path
                
                print("{} {}".format(path, rev))
            
            if catfile.stdout.readline().strip():
                raise SystemExit("No blank line following Git object")
        
        if fail:
            raise SystemExit(fail)

if __name__ == "__main__":
    from funcparams import command
    command()
