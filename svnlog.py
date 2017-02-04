from __future__ import generator_stop

from sys import stdin
from datetime import datetime
from xml.dom import pulldom, minidom
from collections import namedtuple

def main(*,
    starting: dict(type=int, help="minimum revision") = 0,
    before: dict(type=int, help="exclude this and higher revisions") = None,
    updating: dict(help="only report results that affect this absolute path")
        = None,
    copies: dict(mutex="mode",
        help="report file copies rather than individual revisions") = False,
    only_from: dict(mutex="from", help="only report copies from files "
        "matching this absolute path") = "/",
    not_from: dict(mutex="from",
        help="exclude copies from files matching these absolute paths") = (),
    summarize: dict(mutex="mode", help="report the common path "
        "rather than reporting each individual file action") = False,
    rel_path: dict(help="only include revisions matching this relative path")
        = None,
):
    if updating is not None:
        updating = parse_path(updating)
    not_from = tuple(map(parse_path, not_from))
    only_from = parse_path(only_from)
    if rel_path is not None:
        rel_path = tuple(rel_path.split("/"))
    
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
                updating=updating, only_from=only_from, not_from=not_from)
        else:
            if rel_path is not None:
                if log.paths is None:
                    continue
                for path in log.paths:
                    if path.path[-len(rel_path):] == rel_path:
                        match = True
                        break
                else:
                    match = False
                if not match:
                    continue
            show_rev(log, updating=updating, summarize=summarize)
    else:
        assert prev in (None, 1)

def show_rev(log, *, updating, summarize):
    if updating is not None and (log.paths is None
            or not any(p.path[:len(updating)] == updating[:len(p.path)]
            for p in log.paths)):
        return
    print("---")
    if log.author is None:
        author = ""
    else:
        author = f" | {log.author}"
    print(f"r{log.revision}{author} | {log.date}")
    if log.paths is not None:
        if summarize:
            paths = iter(log.paths)
            summary = next(paths, None)
            if summary is None:
                return
            common = summary.path
            for path in paths:
                common = common_prefix(common, path.path)
            if common < summary.path:
                summary = PathLog(common, is_delete=False, is_add=False,
                    copyfrom_rev=None, copyfrom_path=None)
            paths = (summary,)
        else:
            print("Changed paths:")
            paths = log.paths
        for path in paths:
            action = ("MA", "DR")[path.is_delete][path.is_add]
            if path.copyfrom_rev is None:
                copyfrom = ""
            else:
                from_path = "/".join(path.copyfrom_path)
                copyfrom = f" (from {from_path}:{path.copyfrom_rev})"
            print(f"   {action} /{'/'.join(path.path)}{copyfrom}")

def show_copies(rev, paths, *, updating, only_from, not_from):
    if paths is None:
        return
    for path in paths:
        if (
            path.copyfrom_rev is None or
            updating is not None
                and path.path[:len(updating)] != updating[:len(path.path)]
        ):
            continue
        from_path = path.copyfrom_path
        from_rev = path.copyfrom_rev
        if (
            from_path[:len(only_from)] != only_from[:len(from_path)] or
            any(from_path[:len(x)] == x for x in not_from)
        ):
            continue
        
        prefix = common_prefix(path.path, from_path)
        max_common = min(len(path.path), len(from_path)) - len(prefix)
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

def common_prefix(a, b):
    max_common = min(len(a), len(b))
    for i in range(max_common):
        if a[i] != b[i]:
            break
    else:
        i = max_common
    return a[:i]

if __name__ == "__main__":
    from signal import signal, SIGINT, SIGPIPE, SIG_DFL
    from os import kill, getpid
    from argparse import ArgumentParser
    from inspect import signature, Parameter
    
    try:
        parser = ArgumentParser()
        
        groups = dict()
        paired = set()
        for param in signature(main).parameters.values():
            assert param.kind == Parameter.KEYWORD_ONLY
            attrs = param.annotation
            if attrs is Parameter.empty:
                attrs = dict()
            default = param.default
            assert default is not Parameter.empty
            if default is False:
                action = "store_true"
            else:
                action = "store"
                if isinstance(default, int):
                    attrs.setdefault("type", int)
                elif isinstance(default, (list, tuple, set, frozenset)) \
                        and not default:
                    action = "append"
                    default = list()
            
            group_id = attrs.pop("mutex", None)
            if group_id is None:
                group = parser
            else:
                try:
                    group = groups[group_id]
                    paired.add(group_id)
                except LookupError:
                    group = parser.add_mutually_exclusive_group()
                    groups[group_id] = group
            
            name = "--" + param.name.replace("_", "-")
            group.add_argument(name, action=action, default=default, **attrs)
        assert paired == groups.keys()
        
        args = parser.parse_args()
        main(**vars(args))
    except KeyboardInterrupt:
        signal(SIGINT, SIG_DFL)
        kill(getpid(), SIGINT)
    except BrokenPipeError:
        signal(SIGPIPE, SIG_DFL)
        kill(getpid(), SIGPIPE)
