from signal import signal, SIGINT, SIGPIPE, SIG_DFL
from os import kill, getpid
from argparse import ArgumentParser
from inspect import signature, Parameter
from clifunc import splitdoc

def run_cli(main):
    try:
        [summary, _] = splitdoc(main.__doc__)
        parser = ArgumentParser(description=summary)
        
        groups = dict()
        paired = set()
        for param in signature(main).parameters.values():
            attrs = param.annotation
            if attrs is Parameter.empty:
                attrs = dict()
            default = param.default
            if param.kind == Parameter.POSITIONAL_OR_KEYWORD:
                name = param.name
                short = ()
            else:
                assert param.kind == Parameter.KEYWORD_ONLY
                name = "--" + param.name
                short = attrs.pop("short", None)
                if short is None:
                    short = ()
                else:
                    short = (short,)
                if default is Parameter.empty:
                    attrs.setdefault("required", True)
            if param.kind == Parameter.KEYWORD_ONLY and default is False:
                action = "store_true"
            else:
                action = "store"
                if isinstance(default, int):
                    assert param.kind == Parameter.KEYWORD_ONLY
                    attrs.setdefault("type", int)
                elif isinstance(default, (list, tuple, set, frozenset)) \
                        and not default:
                    if param.kind == Parameter.KEYWORD_ONLY:
                        action = "append"
                        default = list()
                    else:
                        attrs.setdefault("nargs", "*")
                elif default is Parameter.empty:
                    default = None
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
            group.add_argument(*short, name,
                action=action, default=default, **attrs)
        assert paired == groups.keys()
        
        args = parser.parse_args()
        main(**vars(args))
    except KeyboardInterrupt:
        signal(SIGINT, SIG_DFL)
        kill(getpid(), SIGINT)
    except BrokenPipeError:
        signal(SIGPIPE, SIG_DFL)
        kill(getpid(), SIGPIPE)
