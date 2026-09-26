# client.py
import argparse
import threading
from async_timeout import timeout

import grpc

import taskflow_pb2
import taskflow_pb2_grpc
from interceptors import HeaderInterceptor

STATUS_NAMES = {0: "TODO", 1: "IN_PROGRESS", 2: "DONE"}
received_events = []  # pour l'option 9 (débug)


def print_event(event):
    print(f"\n🔔 [{event.event_type}] {event.author}: {event.message}\n> ",
          end="", flush=True)


# ---------- TODO(10) ----------
# Thread d'écoute : s'abonner via
#   stub.Subscribe(SubscribeRequest(username=..., event_types=[...]))
# puis for event in stream: mémoriser dans received_events + print_event(event)
# Entourez le tout d'un try/except grpc.RpcError : si le serveur tombe,
# afficher UNE ligne propre (code + details), pas une traceback.
# (CANCELLED = c'est nous qui quittons : ne rien afficher.)
def listen_events(stub, username, event_types):
    try:
        stream = stub.Subscribe(taskflow_pb2.SubscribeRequest(username=username, event_types=event_types))

        for event in stream:
            received_events.append(event)
            print_event(event)
    except grpc.RpcError as e:
        pass


def print_task(task):
    print(f"  [{STATUS_NAMES[task.status]:12}] {task.id} "
          f"« {task.title} » → {task.assigned_to or 'non assignée'} "
          f"({len(task.comments)} commentaire(s))")


def print_comment(comment):
    date_iso = comment.created_at.ToDatetime().isoformat()
    print(f"  - [{date_iso}] {comment.author} : {comment.text}")


def print_statuses():
    for status in STATUS_NAMES:
        print(f"{status} : {STATUS_NAMES[status]}")


def main():
    parser = argparse.ArgumentParser(description="Client TaskFlow")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--user", required=True)
    parser.add_argument("--events", default="",
                        help="filtre, ex: CREATED,DELETED (vide = tout)")
    parser.add_argument("--no-listen", action="store_true")
    parser.add_argument("--timeout", type=float, default=3, help="bonus B1")
    args = parser.parse_args()

    channel = grpc.insecure_channel(f"{args.host}:{args.port}")
    channel = grpc.intercept_channel(channel, HeaderInterceptor(args.user))
    stub = taskflow_pb2_grpc.TaskFlowStub(channel)
    T = args.timeout  # à passer en timeout=T sur TOUS les appels (sauf Subscribe)

    event_types = [e.strip().upper() for e in args.events.split(",") if e.strip()]

    if not args.no_listen:
        threading.Thread(target=listen_events,
                         args=(stub, args.user, event_types),
                         daemon=True).start()

    while True:
        print("""
=== TaskFlow ===  (utilisateur: {u})
 1. Créer une tâche        6. Commenter une tâche
 2. Lister les tâches      7. Supprimer une tâche
 3. Voir une tâche         8. Recherche multi-mots-clés (streaming)
 4. Changer le statut      9. Événements reçus (aide au débug)
 5. Réassigner             0. Quitter""".format(u=args.user))

        try:
            choice = input("choix > ").strip()
            if choice == "1":
                print("=== Créer une tâche ===")
                # ---------- TODO(11) ----------
                # Demander title/description/assigné, appeler CreateTask
                # (created_by=args.user, timeout=T), afficher l'id retourné.
                title = input("Entre un titre:")
                description = input("Entre un description:")
                assigned_to = input("Entre un nom:")

                response = stub.CreateTask(
                    taskflow_pb2.CreateTaskRequest(title=title, description=description, assigned_to=assigned_to,
                                                   created_by=args.user), timeout=T)

                print(response.task.id)

            elif choice == "2":
                print("=== Lister les tâches ===")

                # ---------- TODO(12) ----------
                # Proposer un filtre statut (vide = tous) et un filtre
                # assigné (vide = tous), appeler ListTasks en streaming.
                # Filtre vide -> NE PAS affecter le champ optional.

                print_statuses()

                print(f"{len(STATUS_NAMES)} : ALL")

                status = input("choix du statut > ").strip()
                assigned_to = input("choix du nom > (vide si tout)").strip()

                req = taskflow_pb2.ListTasksRequest()

                if status.isdigit():
                    status_choice = int(status)
                    if status_choice in STATUS_NAMES:
                        req.status_filter = status_choice

                if assigned_to:
                    req.assigned_to_filter = assigned_to

                for task in stub.ListTasks(req, timeout=T):
                    print_task(task)

            elif choice == "3":
                print("=== Voir une tâche ===")

                # ---------- TODO(13) ----------
                # GetTask : afficher la tâche ET ses commentaires
                # (auteur, date ISO via c.created_at.ToDatetime(), texte).

                id = input("choix de la tâche entrez sont id > ").strip()

                if id:
                    task = stub.GetTask(taskflow_pb2.GetTaskRequest(id=id), timeout=T)
                    print_task(task)
                    if len(task.comments) > 0:
                        for comment in task.comments:
                            print_comment(comment)

            elif choice == "4":
                print("=== Changer le statut ===")

                # ---------- TODO(14) ----------
                # Menu TODO/IN_PROGRESS/DONE -> UpdateStatus
                # (requested_by=args.user).
                id = input("choix de la tâche entrez sont id > ").strip()
                print_statuses()
                status = input("choix du statut > ").strip()

                if status.isdigit():
                    status_choice = int(status)
                    stub.UpdateStatus(
                        taskflow_pb2.UpdateStatusRequest(id=id, new_status=status_choice, requested_by=args.user),
                        timeout=T)
                else:
                    print("Erreur lors du choix du statut")

            elif choice == "5":
                print("=== Réassigner ===")

                # ---------- TODO(15) ----------
                # AssignTask (requested_by=args.user).
                id = input("choix de la tâche entrez sont id > ").strip()
                assigned_to = input("choix du nom > ").strip()

                stub.AssignTask(taskflow_pb2.AssignTaskRequest(id=id, new_assignee=assigned_to, requested_by=args.user),
                                timeout=T)

            elif choice == "6":
                print("=== Commenter une tâche ===")

                # ---------- TODO(16) ----------
                # AddComment (texte multi-mots, author=args.user).
                id = input("choix de la tâche entrez sont id > ").strip()
                comment = input("entrez votre commentaire > ").strip()
                stub.AddComment(taskflow_pb2.AddCommentRequest(id=id, author=args.user, text=comment), timeout=T)

            elif choice == "7":
                print("=== Supprimer une tâche ===")

                # ---------- TODO(17) ----------
                # DeleteTask (requested_by=args.user).
                id = input("choix de la tâche entrez sont id > ").strip()
                stub.DeleteTask(taskflow_pb2.DeleteTaskRequest(id=id, requested_by=args.user), timeout=T)

            elif choice == "8":
                print("=== Recherche multi-mots-clés (streaming) ===")
                # ---------- TODO(18) ----------
                # Client streaming : demander des mots-clés un par un
                # (ligne vide = fin), construire un GÉNÉRATEUR Python qui
                # yield les SearchEntry, appeler SearchKeywords(generator)
                # et afficher le SearchSummary (total + résultats).

                words = []
                while True:
                    word = input("Entrez un mot > ").strip()

                    if word == "":
                        break

                    words.append(word)

                def getKeyword():
                    for word in words:
                        yield taskflow_pb2.SearchEntry(keyword=word)

                result = stub.SearchKeywords(getKeyword(), timeout=T)
                print(result)

            elif choice == "9":
                print(f"{len(received_events)} événement(s) reçu(s)")
            elif choice == "0":
                print("Au revoir !")
                break
        except grpc.RpcError as e:
            print(f"❌ Erreur gRPC [{e.code().name}] : {e.details()}")
        except (KeyboardInterrupt, EOFError):
            print()
            break
    channel.close()


if __name__ == "__main__":
    main()
