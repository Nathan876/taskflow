# test_flow.py — python test_flow.py
import queue
import subprocess
import sys
import tempfile
import threading
import time

import grpc

import taskflow_pb2
import taskflow_pb2 as pb
import taskflow_pb2_grpc

PORT = 50061  # port dédié aux tests : ne gêne pas un serveur de démo déjà lancé


def expect_error(fn, code):
    """Appelle fn() et vérifie qu'elle échoue avec le code gRPC attendu."""
    try:
        fn()
    except grpc.RpcError as e:
        assert e.code() == code, f"attendu {code}, reçu {e.code()}"
        return
    raise AssertionError(f"attendu une erreur {code}, aucun échec")


def run_tests(stub, log_path):
    T = 1
    # --- TODO(19) : scénario nominal ---
    # 1. Créer "Rapport" assignée à alice (created_by="alice") -> récupérer l'id
    response_create = stub.CreateTask(taskflow_pb2.CreateTaskRequest(title="Rapport", created_by="alice"), timeout=T)
    task_id = response_create.task.id

    # 2. GetTask(id) -> titre == "Rapport", statut == TODO
    response_get = stub.GetTask(taskflow_pb2.GetTaskRequest(id=task_id), timeout=T)

    if response_get.title != "Rapport":
        print("Erreur dans GetTask")

    # 3. UpdateStatus(id, DONE) -> statut == DONE
    response_update = stub.UpdateStatus(pb.UpdateStatusRequest(
        id=task_id,
        new_status=pb.DONE,
        requested_by="alice"
    ), timeout=3)

    if response_update.status != 2:
        print("Erreur dans UpdateStatus")
    # 4. ListTasks() -> contient au moins 1 tâche ;
    #    ListTasks(status_filter=DONE) -> exactement cette tâche

    # --- TODO(20) : erreurs attendues ---
    # 1. GetTask avec un id inconnu -> NOT_FOUND
    expect_error(stub.GetTask(pb.GetTaskRequest(id="inconnu"), timeout=3), grpc.StatusCode.NOT_FOUND)

    # 2. Création d'une tâche avec un titre vide -> INVALID_ARGUMENT
    expect_error(stub.CreateTask(pb.CreateTaskRequest(title="", created_by="alice"), timeout=3),
                 grpc.StatusCode.INVALID_ARGUMENT)

    # 3. UpdateStatus au même statut -> INVALID_ARGUMENT (notre tâche est déjà DONE)
    expect_error(
        stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.DONE, requested_by="alice"), timeout=3),
        grpc.StatusCode.INVALID_ARGUMENT)

    # 4. Tenter de réouvrir un DONE vers IN_PROGRESS -> INVALID_ARGUMENT
    expect_error(stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.IN_PROGRESS, requested_by="alice"),
                                   timeout=3), grpc.StatusCode.INVALID_ARGUMENT)

    # 5. DeleteTask par "bob" (alors qu'elle a été créée par alice) -> PERMISSION_DENIED
    expect_error(stub.DeleteTask(pb.DeleteTaskRequest(id=task_id, requested_by="bob"), timeout=3),
                 grpc.StatusCode.PERMISSION_DENIED)

    # 6. DeleteTask par "alice" puis GetTask -> NOT_FOUND
    stub.DeleteTask(pb.DeleteTaskRequest(id=task_id, requested_by="alice"), timeout=T)
    expect_error(stub.GetTask(pb.GetTaskRequest(id=task_id), timeout=3), grpc.StatusCode.NOT_FOUND)

    # --- TODO (21) client streaming ---
    # Créer 3 tâches de titres "alpha", "beta", "alpha beta"
    stub.CreateTask(pb.CreateTaskRequest(title="alpha", created_by="alice"), timeout=3)
    stub.CreateTask(pb.CreateTaskRequest(title="beta", created_by="alice"), timeout=3)
    stub.CreateTask(pb.CreateTaskRequest(title="alpha beta", created_by="alice"), timeout=3)

    def search_generator():
        yield pb.SearchEntry(keyword="alpha")
        yield pb.SearchEntry(keyword="BETA")

    summary = stub.SearchKeywords(search_generator(), timeout=3)
    assert summary.total_requests == 2
    for res_item in summary.results:
        if res_item.keyword.lower() == "alpha":
            assert res_item.match_count == 2
        elif res_item.keyword.lower() == "beta":
            assert res_item.match_count == 2

    # Un keyword vide dans le flux -> INVALID_ARGUMENT
    def invalid_search_generator():
        yield pb.SearchEntry(keyword="")

    expect_error(lambda: stub.SearchKeywords(invalid_search_generator(), timeout=3), grpc.StatusCode.INVALID_ARGUMENT)

    # --- TODO (24) Subscribe ---
    sub_channel = grpc.insecure_channel(f"localhost:{PORT}")
    sub_channel = grpc.intercept_channel(sub_channel, pb.unary_unary_client_interceptor(lambda d, r: d) if hasattr(pb,
                                                                                                                   'unary_unary_client_interceptor') else lambda
        channel: channel)  # ou channel intercepté avec HeaderInterceptor("testeur")
    # Pour faire simple avec le channel de test global intercepté ou un canal séparé :
    sub_stub = taskflow_pb2_grpc.TaskFlowStub(sub_channel)

    q = queue.Queue()

    def listen_thread():
        try:
            stream = sub_stub.Subscribe(pb.SubscribeRequest(username="testeur", event_types=["DELETED"]))
            for ev in stream:
                q.put(ev)
        except grpc.RpcError:
            pass

    t_listen = threading.Thread(target=listen_thread, daemon=True)
    t_listen.start()

    # Supprimer une tâche existante pour déclencher un événement DELETED
    stub.DeleteTask(pb.DeleteTaskRequest(id=task_id, requested_by="alice"), timeout=3)

    try:
        event_recu = q.get(timeout=2)
        assert event_recu.event_type == "DELETED"
        assert event_recu.task_id == task_id
    except queue.Empty:
        raise AssertionError("Aucun événement DELETED reçu via le subscribe")
    sub_channel.close()

    # --- TODO (25) : Étape 5 metadata x-user ---
    with open(log_path, "r", encoding="utf-8") as f:
        log_content = f.read()

    assert "user=testeur" in log_content, "Le metadata x-user=testeur est introuvable dans les logs du serveur"
    assert "code=NOT_FOUND" in log_content, "Le code d'erreur NOT_FOUND est introuvable dans les logs du serveur"

def main():
    log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
    proc = subprocess.Popen([sys.executable, "server.py", "--port", str(PORT)],
                            stdout=log, stderr=subprocess.STDOUT)
    channel = grpc.insecure_channel(f"localhost:{PORT}")
    try:
        grpc.channel_ready_future(channel).result(timeout=10)  # attend le serveur
        # Étape 5 : channel = grpc.intercept_channel(channel, HeaderInterceptor("testeur"))
        run_tests(taskflow_pb2_grpc.TaskFlowStub(channel), log.name)
        print("✅ Tous les tests passent.")
    finally:
        channel.close()
        proc.terminate()   # toujours exécuté, même si un assert échoue
        proc.wait()


if __name__ == "__main__":
    main()
