from __future__ import generator_stop

from sys import stdin
from datetime import datetime
from xml.dom import pulldom, minidom
from collections import namedtuple

def main(*,
    starting=0, before=None, copies=False,
    only_to="/", only_from="/", not_from=(),
):
    only_to = parse_path(only_to)
    not_from = tuple(map(parse_path, not_from))
    only_from = parse_path(only_from)
    
    prev = None
    for log in iter_svnlog(stdin.buffer):
        assert prev is None or log.revision == prev - 1
        prev = log.revision
        if before is not None and log.revision >= before:
            continue
        if log.revision < starting:
            break
        if copies:
            show_copies(log.revision, log.paths,
                only_to=only_to, only_from=only_from, not_from=not_from)
        else:
            show_rev(log)
    else:
        assert prev in (None, 1)

def show_rev(log):
    print("---")
    if log.author is None:
        author = ""
    else:
        author = f" | {log.author}"
    print(f"r{log.revision}{author} | {log.date}")
    if log.paths is not None:
        print("Changed paths:")
        for path in log.paths:
            action = ("MA", "DR")[path.is_delete][path.is_add]
            if path.copyfrom_rev is None:
                copyfrom = ""
            else:
                from_path = "/".join(path.copyfrom_path)
                copyfrom = f" (from {from_path}:{path.copyfrom_rev})"
            print(f"   {action} /{'/'.join(path.path)}{copyfrom}")

def show_copies(rev, paths, *, only_to, only_from, not_from):
    if paths is None:
        return
    for path in paths:
        if (
            path.copyfrom_rev is None or
            path.path[:len(only_to)] != only_to[:len(path.path)]
        ):
            continue
        from_path = path.copyfrom_path
        from_rev = path.copyfrom_rev
        if (
            from_path[:len(only_from)] != only_from[:len(from_path)] or
            any(from_path[:len(x)] == x for x in not_from)
        ):
            continue
        
        max_common = min(len(path.path), len(from_path))
        for i in range(max_common):
            if path.path[i] != from_path[i]:
                break
        else:
            i = max_common
        prefix = path.path[:i]
        
        max_common -= i
        for i in range(max_common):
            if path.path[-1 - i] != from_path[-1 - i]:
                break
        else:
            i = max_common
        suffix = path.path[len(path.path) - i:]
        
        path = path.path[len(prefix):len(path.path) - len(suffix)]
        path = "/".join(path)
        from_path = from_path[len(prefix):len(from_path) - len(suffix)]
        from_path = "/".join(from_path)
        if prefix:
            prefix = "/" + "/".join(prefix)
        else:
            prefix = ""
        copy = f"{prefix}/({path}@{rev} <- {from_path}@{from_rev})"
        if suffix:
            copy = f"{copy}/{'/'.join(suffix)}"
        print(copy)

def iter_svnlog(stream):
    stream = pulldom.parse(stream)
    [event, node] = next_content(stream)
    with node:
        assert event == pulldom.START_DOCUMENT
    [event, node] = next_content(stream)
    assert event == pulldom.START_ELEMENT and node.tagName == "log"
    while True:
        [event, node] = next_content(stream)
        if event != pulldom.START_ELEMENT:
            break
        assert node.tagName == "logentry"
        rev = int(node.getAttribute("revision"))
        
        [event, node] = next_content(stream)
        assert event == pulldom.START_ELEMENT
        if node.tagName == "author":
            stream.expandNode(node)
            node.normalize()
            [node] = node.childNodes
            assert isinstance(node, minidom.Text)
            author = node.data
            [event, node] = next_content(stream)
            assert event == pulldom.START_ELEMENT
        else:
            author = None
        
        assert node.tagName == "date"
        stream.expandNode(node)
        node.normalize()
        [node] = node.childNodes
        assert isinstance(node, minidom.Text)
        date = datetime.strptime(node.data, "%Y-%m-%dT%H:%M:%S.%fZ")
        
        [event, node] = next_content(stream)
        # A commit without paths is strange, but possible
        if event == pulldom.START_ELEMENT:
            assert node.tagName == "paths"
            paths = list()
            parents = set()
            while True:
                [event, path_node] = next_content(stream)
                if event != pulldom.START_ELEMENT:
                    break
                assert path_node.tagName == "path"
                stream.expandNode(path_node)
                path_node.normalize()
                action = path_node.getAttribute("action")
                assert action in set("AMRD")
                is_add = action in set("AR")
                is_copy = path_node.hasAttribute("copyfrom-rev")
                assert is_copy == path_node.hasAttribute("copyfrom-path")
                if is_copy:
                    assert is_add
                    from_rev = int(path_node.getAttribute("copyfrom-rev"))
                    assert from_rev < rev
                    from_path = path_node.getAttribute("copyfrom-path")
                    from_path = parse_path(from_path)
                else:
                    from_rev = None
                    from_path = None
                [node] = path_node.childNodes
                assert isinstance(node, minidom.Text)
                path_split = parse_path(node.data)
                assert path_split not in parents
                for n in range(len(path_split)):
                    parents.add(path_split[:n])
                parents.add(path_split)
                paths.append(PathLog(path_split,
                    is_delete=action in set("DR"), is_add=is_add,
                    copyfrom_rev=from_rev, copyfrom_path=from_path))
            assert event == pulldom.END_ELEMENT
            [event, _] = next_content(stream)
        else:
            paths = None
        assert event == pulldom.END_ELEMENT
        yield Log(rev, author=author, date=date, paths=paths)

Log = namedtuple("Log", ("revision", "author", "date", "paths"))
PathLog = namedtuple("PathLog",
    ("path", "is_delete", "is_add", "copyfrom_rev", "copyfrom_path"))

def parse_path(path):
    if path == "/":
        return ()
    assert path.startswith("/")
    return tuple(path[1:].split("/"))

def next_content(stream):
    skip = {
        pulldom.COMMENT, pulldom.CHARACTERS, pulldom.PROCESSING_INSTRUCTION,
        pulldom.IGNORABLE_WHITESPACE,
    }
    while True:
        result = next(stream)
        [event, _] = result
        if event not in skip:
            return result

if __name__ == "__main__":
    from signal import signal, SIGINT, SIG_DFL
    from os import kill, getpid
    try:
        from argparse import ArgumentParser
        
        parser = ArgumentParser()
        parser.add_argument("--starting", type=int, default=0)
        parser.add_argument("--before", type=int)
        parser.add_argument("--copies", action="store_true")
        parser.add_argument("--only-to", default="/")
        from_group = parser.add_mutually_exclusive_group()
        from_group.add_argument("--only-from", default="/")
        from_group.add_argument("--not-from",
            action="append", default=list())
        args = parser.parse_args()
        main(
            starting=args.starting, before=args.before, copies=args.copies,
            only_to=args.only_to,
            only_from=args.only_from, not_from=args.not_from,
        )
    except KeyboardInterrupt:
        signal(SIGINT, SIG_DFL)
        kill(getpid(), SIGINT)
