# interceptors.py
import collections
import time
from datetime import datetime
from urllib import request

import grpc


# ---------- TODO(22) ----------
# LoggingInterceptor (serveur) : à chaque RPC, afficher
#   [HH:MM:SS] METHOD  duration=XXms  code=XX  user=YY
#
# ⚠️ grpc.ServerInterceptor n'a qu'UNE méthode : intercept_service().
#    Elle est appelée AVANT le RPC : continuation(handler_call_details)
#    renvoie un RpcMethodHandler, sans exécuter le RPC. Chronométrer
#    autour de continuation() donnerait donc toujours ~0 ms.
#
# Démarche :
#   1. handler = continuation(handler_call_details) (None -> return None)
#   2. selon le type (handler.unary_unary, .unary_stream, .stream_unary,
#      .stream_stream), envelopper la fonction dans un wrapper qui
#      chronomètre avec time.perf_counter() :
#        - réponse unique  : autour de l'appel à la fonction
#        - réponse en flux : wrapper GÉNÉRATEUR (yield from ...), log à la fin
#   3. reconstruire le handler avec grpc.unary_unary_rpc_method_handler(
#        wrapper, handler.request_deserializer, handler.response_serializer)
#      (idem unary_stream_…, stream_unary_…, stream_stream_…)
#   4. code : context.code() (None = OK ; exception non-abort = UNKNOWN)
#   5. user : dict(handler_call_details.invocation_metadata).get("x-user")
#   Pensez à print(..., flush=True).
class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        if handler.unary_unary is not None:
            def wrapper(request, context):
                start_time = time.perf_counter()
                result = handler.unary_unary(request, context)
                end_time = time.perf_counter()
                duration_ms = (end_time - start_time) * 1000
                username = dict(handler_call_details.invocation_metadata).get("x-user")
                hours = datetime.now().strftime("%H:%M:%S")
                method = handler_call_details.method
                print(f"[{hours}] {method} duration={duration_ms}ms code= {context.code()} user={username}", flush=True)
                return result

            return (
                grpc.unary_unary_rpc_method_handler(wrapper, handler.request_deserializer, handler.response_serializer))
        elif handler.unary_stream is not None:
            def wrapper(request, context):
                start_time = time.perf_counter()
                yield from handler.unary_stream(request, context)
                end_time = time.perf_counter()
                duration_ms = (end_time - start_time) * 1000
                username = dict(handler_call_details.invocation_metadata).get("x-user")
                hours = datetime.now().strftime("%H:%M:%S")
                method = handler_call_details.method
                print(f"[{hours}] {method} duration={duration_ms: .2f}ms code= {context.code()} user={username}",
                      flush=True)

            return grpc.unary_stream_rpc_method_handler(
                wrapper,
                handler.request_deserializer,
                handler.response_serializer
            )
        elif handler.stream_unary is not None:
            def wrapper(request, context):
                start_time = time.perf_counter()
                result = handler.stream_unary(request, context)
                end_time = time.perf_counter()
                duration_ms = (end_time - start_time) * 1000
                username = dict(handler_call_details.invocation_metadata).get("x-user", "Unknown")
                hours = datetime.now().strftime("%H:%M:%S")
                method = handler_call_details.method
                print(f"[{hours}] {method} duration={duration_ms:.2f}ms code={context.code() or 'OK'} user={username}",
                      flush=True)
                return result

            return grpc.stream_unary_rpc_method_handler(wrapper, handler.request_deserializer,
                                                        handler.response_serializer)
        elif handler.stream_stream is not None:
            def wrapper(request, context):
                start_time = time.perf_counter()
                yield from handler.stream_stream(request, context)
                end_time = time.perf_counter()
                duration_ms = (end_time - start_time) * 1000
                username = dict(handler_call_details.invocation_metadata).get("x-user")
                hours = datetime.now().strftime("%H:%M:%S")
                method = handler_call_details.method
                print(f"[{hours}] {method} duration={duration_ms: .2f}ms code= {context.code()} user={username}",
                      flush=True)

            return grpc.stream_stream_rpc_method_handler(
                wrapper,
                handler.request_deserializer,
                handler.response_serializer
            )
        return handler


# ---------- TODO(23) ----------
# HeaderInterceptor (client) : ajoute le metadata ("x-user", <pseudo>) à
# CHAQUE appel — y compris ListTasks, Subscribe et SearchKeywords, d'où
# l'héritage des 4 interfaces.
# grpc.ClientCallDetails est abstraite : utilisez la classe _ClientCallDetails
# ci-dessous pour construire des détails modifiés, puis
#   return continuation(nouveaux_details, request)
class _ClientCallDetails(
    collections.namedtuple(
        "_ClientCallDetails",
        ("method", "timeout", "metadata", "credentials",
         "wait_for_ready", "compression")),
    grpc.ClientCallDetails):
    pass


class HeaderInterceptor(grpc.UnaryUnaryClientInterceptor,
                        grpc.UnaryStreamClientInterceptor,
                        grpc.StreamUnaryClientInterceptor,
                        grpc.StreamStreamClientInterceptor):
    def __init__(self, user: str):
        self._user = user

    def _inject(self, details):
        metadata = list(details.metadata) if details.metadata else []
        metadata.append(("x-user", self._user))
        return _ClientCallDetails(
            method=details.method,
            timeout=details.timeout,
            metadata=metadata,
            credentials=details.credentials,
            wait_for_ready=getattr(details, "wait_for_ready", None),
            compression=getattr(details, "compression", None),
        )

    def intercept_unary_unary(self, continuation, details, request):
        return continuation(self._inject(details), request)

    def intercept_unary_stream(self, continuation, details, request):
        return continuation(self._inject(details), request)

    def intercept_stream_unary(self, continuation, details, request_iterator):
        return continuation(self._inject(details), request_iterator)

    def intercept_stream_stream(self, continuation, details, request_iterator):
        return continuation(self._inject(details), request_iterator)
