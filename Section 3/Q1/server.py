"""
Q1 - Collaborative Document Editing using gRPC: Document Server.

    usage: python3 server.py [address]        (default 0.0.0.0:50051)

Documents live in memory. Synchronization:
  - DocumentService.lock protects the name -> document dictionary only.
  - Document.lock protects one document's content, version and subscriber
    list. Every edit runs entirely under this lock, so concurrent edits to the
    same document are applied one at a time (in the order they are processed)
    and can never interleave or corrupt the text. Edits to different documents
    do not block each other.
  - Every subscriber has its own queue.Queue (thread-safe) of pending updates.
    Lock order is always DocumentService.lock -> Document.lock, and the first
    is released before the second is taken.

Update propagation: while still holding Document.lock, an edit puts a
DocumentUpdate (the full new contents) into the queue of every subscriber of
that document. Each SubscribeToUpdates call runs on its own server thread and
yields from its queue, so a slow client never blocks editors, and every
subscriber sees every version, in order.
"""
import queue
import sys
import threading
from concurrent import futures

import grpc

import generate_proto  # noqa: F401  (creates document_pb2*.py from document.proto if needed)
import document_pb2
import document_pb2_grpc

# Each open subscription holds one worker thread for as long as it is open,
# so this is also the limit on subscriptions + concurrent requests.
MAX_WORKERS = 64

_print_lock = threading.Lock()


def log(message):
    with _print_lock:
        print("[Server] " + message, flush=True)


class Document:
    def __init__(self, content):
        self.lock = threading.Lock()
        self.content = content
        self.version = 0
        self.subscribers = []   # one queue.Queue per open subscription


class DocumentService(document_pb2_grpc.DocumentServiceServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.documents = {}

    def _find(self, name):
        with self.lock:
            return self.documents.get(name)

    def CreateDocument(self, request, context):
        if not request.name:
            return document_pb2.CreateDocumentResponse(
                success=False, message="document name must not be empty")
        with self.lock:
            if request.name in self.documents:
                return document_pb2.CreateDocumentResponse(
                    success=False, message="document %s already exists" % request.name)
            self.documents[request.name] = Document(request.content)
        log("created %s (%d chars)" % (request.name, len(request.content)))
        return document_pb2.CreateDocumentResponse(
            success=True, message="Document %s created." % request.name)

    def GetDocument(self, request, context):
        doc = self._find(request.name)
        if doc is None:
            return document_pb2.GetDocumentResponse(
                success=False, message="document %s not found" % request.name)
        with doc.lock:
            return document_pb2.GetDocumentResponse(
                success=True, content=doc.content, version=doc.version)

    def EditDocument(self, request, context):
        doc = self._find(request.name)
        if doc is None:
            return document_pb2.EditDocumentResponse(
                success=False, message="document %s not found" % request.name)

        with doc.lock:
            if request.position > len(doc.content):
                return document_pb2.EditDocumentResponse(
                    success=False,
                    message="position %d is outside the document (length %d)"
                            % (request.position, len(doc.content)))

            doc.content = (doc.content[:request.position] + request.text +
                           doc.content[request.position:])
            doc.version += 1

            update = document_pb2.DocumentUpdate(
                name=request.name, content=doc.content, version=doc.version,
                editor=request.client_id, position=request.position, text=request.text)
            for subscriber in doc.subscribers:
                subscriber.put(update)

            log('%s v%d: %s inserted "%s" at %d -> notified %d subscriber(s)'
                % (request.name, doc.version, request.client_id, request.text,
                   request.position, len(doc.subscribers)))
            return document_pb2.EditDocumentResponse(
                success=True, content=doc.content, version=doc.version)

    def SubscribeToUpdates(self, request, context):
        doc = self._find(request.name)
        if doc is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "document %s not found" % request.name)

        pending = queue.Queue()
        # When the client cancels or disconnects (or the server stops), wake
        # this thread up with None so it can finish.
        context.add_callback(lambda: pending.put(None))

        with doc.lock:
            # Register and queue the current contents atomically, so no edit
            # can fall between the snapshot and the first update.
            pending.put(document_pb2.DocumentUpdate(
                name=request.name, content=doc.content, version=doc.version, snapshot=True))
            doc.subscribers.append(pending)
        log("%s subscribed to %s" % (request.client_id, request.name))

        try:
            while context.is_active():
                update = pending.get()
                if update is None:
                    break
                yield update
        finally:
            with doc.lock:
                doc.subscribers.remove(pending)
            log("%s unsubscribed from %s" % (request.client_id, request.name))


def main():
    address = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0:50051"

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))
    document_pb2_grpc.add_DocumentServiceServicer_to_server(DocumentService(), server)
    try:
        port = server.add_insecure_port(address)   # 0 on failure in older grpcio
    except RuntimeError:
        port = 0
    if port == 0:
        sys.exit("[Server] could not listen on " + address)
    server.start()
    log("DocumentService listening on " + address)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log("shutting down")
        server.stop(grace=1).wait()


if __name__ == "__main__":
    main()
