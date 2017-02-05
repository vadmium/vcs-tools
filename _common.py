from signal import signal, SIGINT, SIGPIPE, SIG_DFL
from os import kill, getpid
from argparse import ArgumentParser
from inspect import signature, Parameter

def run_cli(main):
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
