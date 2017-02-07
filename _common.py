from signal import signal, SIGINT, SIGPIPE, SIG_DFL
from os import kill, getpid
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from inspect import signature, Parameter
from clifunc import splitdoc
import email.parser
from warnings import warn

def run_cli(main):
    try:
        [summary, details] = splitdoc(main.__doc__)
        parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
            description=summary, epilog=details)
        
        groups = dict()
        paired = set()
        pos = None
        for param in signature(main).parameters.values():
            attrs = param.annotation
            if attrs is Parameter.empty:
                attrs = dict()
            kw = dict(default=param.default)
            if param.kind == Parameter.KEYWORD_ONLY:
                name = "--" + param.name
                short = attrs.pop("short", None)
                if short is None:
                    short = ()
                else:
                    short = (short,)
                if kw["default"] is Parameter.empty:
                    attrs.setdefault("required", True)
            else:
                name = param.name
                short = ()
                if param.kind == Parameter.VAR_POSITIONAL:
                    pos = param.name
                    attrs.setdefault("nargs", "*")
                else:
                    assert param.kind == Parameter.POSITIONAL_OR_KEYWORD
            if param.kind == Parameter.KEYWORD_ONLY \
                    and kw["default"] is False:
                kw.update(action="store_true")
            else:
                if isinstance(kw["default"], int):
                    assert param.kind == Parameter.KEYWORD_ONLY
                    attrs.setdefault("type", int)
                elif isinstance(kw["default"],
                        (list, tuple, set, frozenset)) and not kw["default"]:
                    if param.kind == Parameter.KEYWORD_ONLY:
                        kw.update(action="append", default=list())
                    else:
                        attrs.setdefault("nargs", "*")
                elif kw["default"] is Parameter.empty:
                    del kw["default"]
                else:
                    assert param.kind == Parameter.KEYWORD_ONLY
            
            required = attrs.pop("mutex_required", None)
            group_id = attrs.pop("mutex", required)
            if group_id is None:
                group = parser
            else:
                try:
                    group = groups[group_id]
                    paired.add(group_id)
                except LookupError:
                    group = parser.add_mutually_exclusive_group(
                        required=required is not None)
                    groups[group_id] = group
            
            name = name.replace("_", "-")
            group.add_argument(*short, name, **kw, **attrs)
        assert paired == groups.keys()
        
        args = dict(vars(parser.parse_args()))
        if pos is None:
            pos = ()
        else:
            pos = args.pop(pos)
        main(*pos, **args)
    except KeyboardInterrupt:
        signal(SIGINT, SIG_DFL)
        kill(getpid(), SIGINT)
    except BrokenPipeError:
        signal(SIGPIPE, SIG_DFL)
        kill(getpid(), SIGPIPE)

def parse_path(path):
    if path == "/":
        return ()
    assert path.startswith("/")
    assert not path.endswith("/")
    return tuple(path[1:].split("/"))

def read_message_header(stream):
    parser = email.parser.BytesFeedParser()
    while True:
        line = stream.readline()
        parser.feed(line)
        if not line.rstrip(b"\r\n"):
            break
    message = parser.close()
    for defect in message.defects:
        warn(f"{stream.name}: {defect!r}\n")
    return message
