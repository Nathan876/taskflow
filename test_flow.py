# test_flow.py — python test_flow.py
import queue
import subprocess
import sys
import tempfile
import threading
import time

import grpc

import taskflow_pb2 as pb
import taskflow_pb2_grpc
from interceptors import HeaderInterceptor

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
    # --- TODO(19) : scénario nominal ---
    # 1. Créer "Rapport" assignée à alice (created_by="alice") -> récupérer l'id
    res = stub.CreateTask(pb.CreateTaskRequest(title="Rapport", created_by="alice"), timeout=3)
    task_id = res.task.id

    # 2. GetTask(id) -> titre == "Rapport", statut == TODO
    t = stub.GetTask(pb.GetTaskRequest(id=task_id), timeout=3)
    assert t.title == "Rapport"
    assert t.status == pb.TODO

    # 3. UpdateStatus(id, DONE) -> statut == DONE
    up = stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.DONE, requested_by="alice"), timeout=3)
    assert up.status == pb.DONE

    # 4. ListTasks() -> contient au moins 1 tâche ;
    #    ListTasks(status_filter=DONE) -> exactement cette tâche
    assert len(list(stub.ListTasks(pb.ListTasksRequest(), timeout=3))) >= 1
    done_list = list(stub.ListTasks(pb.ListTasksRequest(status_filter=pb.DONE), timeout=3))
    assert len(done_list) == 1
    assert done_list[0].id == task_id

    # --- TODO(20) : erreurs attendues ---
    expect_error(lambda: stub.GetTask(pb.GetTaskRequest(
         id="inconnu"), timeout=3), grpc.StatusCode.NOT_FOUND)
    # Ajouter : titre vide -> INVALID_ARGUMENT ;
    expect_error(lambda: stub.CreateTask(pb.CreateTaskRequest(title="", created_by="alice"), timeout=3),
                 grpc.StatusCode.INVALID_ARGUMENT)
    # UpdateStatus au même statut -> INVALID_ARGUMENT ;
    expect_error(lambda: stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.DONE, requested_by="alice"),
                                           timeout=3), grpc.StatusCode.INVALID_ARGUMENT)
    # DONE -> IN_PROGRESS -> INVALID_ARGUMENT ;
    expect_error(
        lambda: stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.IN_PROGRESS, requested_by="alice"),
                                  timeout=3), grpc.StatusCode.INVALID_ARGUMENT)

    t2 = stub.CreateTask(pb.CreateTaskRequest(title="Temp", created_by="alice"), timeout=3).task.id
    # DeleteTask par "bob" -> PERMISSION_DENIED ;
    expect_error(lambda: stub.DeleteTask(pb.DeleteTaskRequest(id=t2, requested_by="bob"), timeout=3),
                 grpc.StatusCode.PERMISSION_DENIED)
    # DeleteTask par "alice" puis GetTask -> NOT_FOUND.
    stub.DeleteTask(pb.DeleteTaskRequest(id=t2, requested_by="alice"), timeout=3)
    expect_error(lambda: stub.GetTask(pb.GetTaskRequest(id=t2), timeout=3), grpc.StatusCode.NOT_FOUND)    #     id="inconnu"), timeout=3), grpc.StatusCode.NOT_FOUND)
    # --- TODO(21) : client streaming ---
    # Créer 3 tâches de titres "alpha", "beta", "alpha beta".
    stub.CreateTask(pb.CreateTaskRequest(title="alpha", created_by="alice"), timeout=3)
    stub.CreateTask(pb.CreateTaskRequest(title="beta", created_by="alice"), timeout=3)
    stub.CreateTask(pb.CreateTaskRequest(title="alpha beta", created_by="alice"), timeout=3)

    # Envoyer ["alpha", "BETA"] en client streaming ->
    # total_requests == 2, matchs alpha == 2, BETA == 2 (casse ignorée).
    def gen():
        yield pb.SearchEntry(keyword="alpha")
        yield pb.SearchEntry(keyword="BETA")

    summary = stub.SearchKeywords(gen(), timeout=3)
    assert summary.total_requests == 2
    for r in summary.results:
        if r.keyword.lower() in ("alpha", "beta"):
            assert r.match_count == 2
    # Un keyword vide dans le flux -> INVALID_ARGUMENT.
    expect_error(lambda: stub.SearchKeywords(iter([pb.SearchEntry(keyword="")]), timeout=3),
                 grpc.StatusCode.INVALID_ARGUMENT)

    # --- TODO(24) : Subscribe ---
    # Sur un channel séparé, s'abonner avec event_types=["DELETED"] ;[cite: 1]
    # lire le flux dans un threading.Thread qui pousse dans une queue.Queue.[cite: 1]
    # time.sleep(0.3) pour laisser l'abonnement s'établir, puis créer et[cite: 1]
    # supprimer une tâche : q.get(timeout=2) doit renvoyer un DELETED[cite: 1]
    # (le CREATED a été filtré). Fermez ensuite ce channel.[cite: 1]

    sub_channel = grpc.insecure_channel(f"localhost:{PORT}")
    sub_channel = grpc.intercept_channel(sub_channel, HeaderInterceptor("testeur"))
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
    time.sleep(0.3)

    t3_res = stub.CreateTask(pb.CreateTaskRequest(title="TempDelete", created_by="alice"), timeout=3)
    t3_id = t3_res.task.id
    stub.DeleteTask(pb.DeleteTaskRequest(id=t3_id, requested_by="alice"), timeout=3)

    try:
        event_recu = q.get(timeout=2)
        assert event_recu.event_type == "DELETED"
        assert event_recu.task_id == t3_id
    except queue.Empty:
        raise AssertionError("Aucun événement DELETED reçu via le subscribe")
    sub_channel.close()

    # --- TODO(25) : Étape 5 — metadata x-user ---
    # Le stdout du serveur est écrit dans log_path. Vérifiez qu'il contient[cite: 1]
    # "user=testeur" (metadata ajouté par HeaderInterceptor) et une ligne[cite: 1]
    # avec code=NOT_FOUND (produite par LoggingInterceptor).[cite: 1]
    with open(log_path, "r", encoding="utf-8") as f:
        logs = f.read()
    assert "user=testeur" in logs
    assert "code=NOT_FOUND" in logs


def main():
    log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
    proc = subprocess.Popen([sys.executable, "server.py", "--port", str(PORT)],
                            stdout=log, stderr=subprocess.STDOUT)
    channel = grpc.insecure_channel(f"localhost:{PORT}")
    try:
        grpc.channel_ready_future(channel).result(timeout=10)  # attend le serveur
        # Étape 5 :
        channel = grpc.intercept_channel(channel, HeaderInterceptor("testeur"))
        run_tests(taskflow_pb2_grpc.TaskFlowStub(channel), log.name)
        print("✅ Tous les tests passent.")
    finally:
        channel.close()
        proc.terminate()   # toujours exécuté, même si un assert échoue
        proc.wait()


if __name__ == "__main__":
    main()
