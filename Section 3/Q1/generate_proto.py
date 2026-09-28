"""
Generate document_pb2.py and document_pb2_grpc.py from document.proto.

Same as running, in this folder:
    python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. document.proto

server.py and client.py import this module, so the code is (re)generated
automatically when it is missing or older than document.proto. It can also be
run by hand: python3 generate_proto.py
"""
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
PROTO = os.path.join(HERE, "document.proto")
OUTPUTS = [os.path.join(HERE, name) for name in ("document_pb2.py", "document_pb2_grpc.py")]


def up_to_date():
    return all(os.path.exists(path) and os.path.getmtime(path) >= os.path.getmtime(PROTO)
               for path in OUTPUTS)


def generate():
    from grpc_tools import protoc   # pip install grpcio-tools

    # Generate into a temporary folder and move the files into place, so
    # clients started at the same moment never import a half-written file.
    with tempfile.TemporaryDirectory(dir=HERE) as tmp:
        status = protoc.main(["grpc_tools.protoc", "-I" + HERE, "--python_out=" + tmp,
                              "--grpc_python_out=" + tmp, PROTO])
        if status != 0:
            sys.exit("protoc failed to compile " + PROTO)
        for path in OUTPUTS:
            os.replace(os.path.join(tmp, os.path.basename(path)), path)


if not up_to_date():
    generate()

if HERE not in sys.path:
    sys.path.insert(0, HERE)
