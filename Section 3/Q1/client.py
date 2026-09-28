"""
Q1 - Collaborative Document Editing using gRPC: Client CLI.

    usage: python3 client.py [server_address] [client_name]   (default localhost:50051)

Menu (type the number and answer the prompts, or type the command directly):
  1. Create Document       create <name> "<content>"
  2. Open Document         open <name>
  3. Edit Document         edit <name> <position> "<text>"
  4. Subscribe to Updates  subscribe <name>
  5. Exit                  exit

Every subscription reads its server-streaming call on its own thread, which
prints each update as it arrives, so the user can keep issuing commands on the
main thread while updates come in. Commands can also be piped in on stdin (the
prompt is only shown when stdin is a terminal).
"""
import os
import shlex
import socket
import sys
import threading

import grpc

import generate_proto  # noqa: F401  (creates document_pb2*.py from document.proto if needed)
import document_pb2
import document_pb2_grpc

MENU = """1. Create Document       create <name> "<content>"
2. Open Document         open <name>
3. Edit Document         edit <name> <position> "<text>"
4. Subscribe to Updates  subscribe <name>
5. Exit                  exit"""

INTERACTIVE = sys.stdin.isatty()   # show the "> " prompt only for a terminal
_print_lock = threading.Lock()     # one thread prints at a time


def say(text):
    with _print_lock:
        print(text, flush=True)


def prompt():
    if INTERACTIVE:
        with _print_lock:
            print("> ", end="", flush=True)


class DocumentClient:
    def __init__(self, address, client_id):
        self.channel = grpc.insecure_channel(address)
        self.stub = document_pb2_grpc.DocumentServiceStub(self.channel)
        self.client_id = client_id
        self.subscriptions = {}   # document name -> (streaming call, reader thread)

    def create(self, name, content):
        response = self._call(self.stub.CreateDocument,
                              document_pb2.CreateDocumentRequest(name=name, content=content))
        if response:
            say("[Client] Document %s created." % name)

    def open(self, name):
        response = self._call(self.stub.GetDocument, document_pb2.GetDocumentRequest(name=name))
        if response:
            say("[Client] " + response.content)

    def edit(self, name, position, text):
        response = self._call(self.stub.EditDocument, document_pb2.EditDocumentRequest(
            name=name, position=position, text=text, client_id=self.client_id))
        if response:
            say("[Client] Edit applied (version %d): %s" % (response.version, response.content))

    def subscribe(self, name):
        if name in self.subscriptions:
            say("[Client] Already subscribed to %s." % name)
            return
        call = self.stub.SubscribeToUpdates(
            document_pb2.UpdateRequest(name=name, client_id=self.client_id))
        registered = threading.Event()
        result = {}   # "error" is set by the reader thread if the subscribe failed
        reader = threading.Thread(target=self._read_updates,
                                  args=(name, call, registered, result), daemon=True)
        reader.start()

        # Wait for the server's snapshot, so that "Subscribed" is only printed
        # once the server has registered us and no later edit can be missed.
        registered.wait()
        if "error" in result:
            reader.join()
            say("[Client] Error: " + result["error"])
            return
        self.subscriptions[name] = (call, reader)
        say("[Client] Subscribed to updates.")

    def close(self):
        for call, _ in self.subscriptions.values():
            call.cancel()
        for _, reader in self.subscriptions.values():
            reader.join()
        self.channel.close()

    # Runs on the subscription's own thread for the lifetime of the stream.
    def _read_updates(self, name, call, registered, result):
        confirmed = False
        try:
            for update in call:
                if update.snapshot:   # first message: the subscription is registered
                    confirmed = True
                    registered.set()
                    continue
                with _print_lock:
                    print("\n[Update] Document %s modified.\n%s" % (update.name, update.content),
                          flush=True)
                    if INTERACTIVE:
                        print("> ", end="", flush=True)
            error = "subscription closed by server"
        except grpc.RpcError as rpc_error:
            if rpc_error.code() == grpc.StatusCode.CANCELLED:   # we unsubscribed
                return
            error = rpc_error.details() or str(rpc_error.code())
            if rpc_error.code() == grpc.StatusCode.UNAVAILABLE:
                error = "cannot reach server (%s)" % error
        if not confirmed:
            result["error"] = error
            registered.set()
        else:
            say("\n[Client] Subscription to %s ended: %s" % (name, error))

    @staticmethod
    def _call(method, request):
        """Unary call; returns the response, or None after printing the error."""
        try:
            response = method(request, timeout=10)
        except grpc.RpcError as rpc_error:
            say("[Client] Error: cannot reach server (%s)" % rpc_error.details())
            return None
        if not response.success:
            say("[Client] Error: " + response.message)
            return None
        return response


def ask(question):
    """Read one answer for a menu prompt; None at end of input."""
    if INTERACTIVE:
        with _print_lock:
            print(question + ": ", end="", flush=True)
    line = sys.stdin.readline()
    return line.rstrip("\n") if line else None


def parse_position(text):
    if text is None or not text.isdigit():
        say("[Client] Error: position must be a non-negative integer")
        return None
    return int(text)


def run_menu_option(client, option):
    if option == "1":
        name = ask("Document name")
        content = None if name is None else ask("Initial content")
        if content is not None:
            client.create(name, content)
    elif option == "2":
        name = ask("Document name")
        if name is not None:
            client.open(name)
    elif option == "3":
        name = ask("Document name")
        position = None if name is None else parse_position(ask("Position"))
        text = None if position is None else ask("Text to insert")
        if text is not None:
            client.edit(name, position, text)
    elif option == "4":
        name = ask("Document name")
        if name is not None:
            client.subscribe(name)


def run_command(client, args):
    """Typed command such as: edit report.txt 6 "Distributed " """
    command = args[0]
    if command == "create" and len(args) in (2, 3):
        client.create(args[1], args[2] if len(args) == 3 else "")
    elif command == "open" and len(args) == 2:
        client.open(args[1])
    elif command == "edit" and len(args) == 4:
        position = parse_position(args[2])
        if position is not None:
            client.edit(args[1], position, args[3])
    elif command == "subscribe" and len(args) == 2:
        client.subscribe(args[1])
    elif command == "help":
        say(MENU)
    else:
        say("[Client] Unknown command. Type 'help' for the list of commands.")


def main():
    address = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    client_id = sys.argv[2] if len(sys.argv) > 2 else "%s:%d" % (socket.gethostname(), os.getpid())

    client = DocumentClient(address, client_id)
    say("[Client] %s connected to %s\n%s" % (client_id, address, MENU))
    try:
        while True:
            prompt()
            line = sys.stdin.readline()
            if not line:   # end of input
                break
            try:
                args = shlex.split(line)
            except ValueError as error:   # e.g. a missing closing quote
                say("[Client] Error: " + str(error))
                continue
            if not args:
                continue
            if args[0] in ("5", "exit", "quit"):
                break
            if args[0] in ("1", "2", "3", "4") and len(args) == 1:
                run_menu_option(client, args[0])
            else:
                run_command(client, args)
    except KeyboardInterrupt:
        pass
    client.close()
    say("[Client] Bye.")


if __name__ == "__main__":
    main()
