from __future__ import generator_stop

from sys import stdin
from datetime import datetime
from xml.dom import pulldom, minidom

def main(*, only_to="/", only_from="/", not_from=()):
    only_to = parse_path(only_to)
    not_from = tuple(map(parse_path, not_from))
    only_from = parse_path(only_from)
    
    for [rev, copies] in iter_svn_copies(stdin.buffer):
        show_copies(rev, copies,
            only_to=only_to, only_from=only_from, not_from=not_from)

def show_copies(rev, copies, *, only_to, only_from, not_from):
    if copies is None:
        return
    for [path, from_rev, from_path] in copies:
        if (
            path[:len(only_to)] != only_to[:len(path)] or
            from_path[:len(only_from)] != only_from[:len(from_path)] or
            any(from_path[:len(x)] == x for x in not_from)
        ):
            continue
        
        max_common = min(len(path), len(from_path))
        for i in range(max_common):
            if path[i] != from_path[i]:
                break
        else:
            i = max_common
        prefix = path[:i]
        
        max_common -= i
        for i in range(max_common):
            if path[-1 - i] != from_path[-1 - i]:
                break
        else:
            i = max_common
        suffix = path[len(path) - i:]
        
        path = path[len(prefix):len(path) - len(suffix)]
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

def iter_svn_copies(stream):
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
            node.data
            [event, node] = next_content(stream)
            assert event == pulldom.START_ELEMENT
        
        assert node.tagName == "date"
        stream.expandNode(node)
        node.normalize()
        [node] = node.childNodes
        assert isinstance(node, minidom.Text)
        datetime.strptime(node.data, "%Y-%m-%dT%H:%M:%S.%fZ")
        
        [event, node] = next_content(stream)
        # A commit without paths is strange, but possible
        if event == pulldom.START_ELEMENT:
            assert node.tagName == "paths"
            copies = list()
            while True:
                [event, path_node] = next_content(stream)
                if event != pulldom.START_ELEMENT:
                    break
                assert path_node.tagName == "path"
                stream.expandNode(path_node)
                path_node.normalize()
                if not path_node.hasAttribute("copyfrom-rev"):
                    continue
                assert path_node.getAttribute("action") in set("AR")
                
                [node] = path_node.childNodes
                assert isinstance(node, minidom.Text)
                path_split = parse_path(node.data)
                
                from_rev = path_node.getAttribute("copyfrom-rev")
                from_path = path_node.getAttribute("copyfrom-path")
                from_path = parse_path(from_path)
                copies.append((path_split, from_rev, from_path))
            assert event == pulldom.END_ELEMENT
            [event, _] = next_content(stream)
        else:
            copies = None
        assert event == pulldom.END_ELEMENT
        yield (rev, copies)

def parse_path(path):
    if path == "/":
        return []
    assert path.startswith("/")
    return path[1:].split("/")

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
        parser.add_argument("--only-to", default="/")
        from_group = parser.add_mutually_exclusive_group()
        from_group.add_argument("--only-from", default="/")
        from_group.add_argument("--not-from",
            action="append", default=list())
        args = parser.parse_args()
        main(only_to=args.only_to,
            only_from=args.only_from, not_from=args.not_from)
    except KeyboardInterrupt:
        signal(SIGINT, SIG_DFL)
        kill(getpid(), SIGINT)
