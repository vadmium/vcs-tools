#! /usr/bin/env python3

import svnlog
from sys import stdin, stdout
from contextlib import ExitStack
import email.parser, email.message, email.generator
from warnings import warn

def main(*inputs, log=None):
    """Merge dumps of different parts of a Subversion repository

    Multiple dump streams may be concatenated into a single input stream if
    they have contiguous revision numbers.

    Separate dump streams can also be given of separate paths in the
    repository. In this case a log input is also required to determine
    whether there are any copies between the paths to restore.
    """
    
    if log:
        with open(log, "rb") as log:
            copies = dict()
            for [rev, rev_copies] in svnlog.iter_svn_copies(log):
                if rev_copies is None:
                    continue
                copies[rev] = rev_copies
    else:
        copies = None
    
    with ExitStack() as cleanup:
        dumps = list()
        for input in inputs:
            stream = cleanup.enter_context(open(input, "rb"))
            dumps.append({"stream": stream})
        if not dumps:
            dumps = ({"stream": stdin.buffer},)
        
        out_version = None
        for dump in dumps:
            record = read_message_header(dump["stream"])
            [version] = record.get_all("SVN-fs-dump-format-version", ())
            dump["version"] = int(version)
            if out_version is None:
                out_version = dump["version"]
            else:
                out_version = max(out_version, dump["version"])
        version = ("SVN-fs-dump-format-version", format(out_version))
        write_message_fields(stdout.buffer, (version,))
        
        out_uuid = None
        out_record = None
        end = False
        for dump in dumps:
            record = read_message_header(dump["stream"])
            if record:
                uuid = record.get_all("UUID", ())
            if record and uuid:
                if dump["version"] < 2:
                    warn(f"{dump['stream'].name}: UUID record only "
                        "expected in version >= 2")
                [uuid] = uuid
                if out_uuid is None:
                    out_uuid = uuid
                elif out_uuid != uuid:
                    warn(f"{dump['stream'].name}: Conflicting UUID {uuid}; "
                        f"expected {out_uuid}")
                record = read_message_header(dump["stream"])
            
            if record is None:
                end = True
            else:
                content = dump["stream"].read(int(record["Content-length"]))
                if out_record is None:
                    out_record = record
                    out_content = content
                elif (record.items() != out_record.items() or
                        content != out_content):
                    new_record = email.message.Message()
                    for field in ("Revision-number", "Prop-content-length",
                            "Content-length"):
                        values = out_record.get_all(field, ())
                        if (record.get_all(field, ()) != values):
                            warn(f"{dump['stream'].name}: Conflicting "
                                f"{field} field")
                        for value in values:
                            new_record[field] = value
                    out_record = new_record
                    if content != out_content:
                        warn(f"{dump['stream'].name}: Conflicting content")
        
        if out_uuid is not None and out_version >= 2:
            write_message_fields(stdout.buffer, (("UUID", out_uuid),))
        if not end:
            generator = email.generator.BytesGenerator(stdout.buffer,
                mangle_from_=False)
            generator.flatten(out_record)
            stdout.buffer.write(out_content)

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

def write_message_fields(file, headers):
    msg = email.message.Message()
    for (name, value) in headers:
        msg[name] = value
    email.generator.BytesGenerator(file, mangle_from_=False).flatten(msg)

if __name__ == "__main__":
    from argparse import ArgumentParser, RawDescriptionHelpFormatter
    from clifunc import splitdoc
    from inspect import getdoc
    
    [summary, details] = splitdoc(getdoc(main))
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
        description=summary, epilog=details)
    parser.add_argument("input", nargs="*",
        help="input dump streams (default: stdin)")
    parser.add_argument("-l", "--log", help="input log stream")
    args = parser.parse_args()
    main(*args.input, log=args.log)
