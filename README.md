# TaskFlow

## Distribution des rôles

- Membre A : Nathan CHABALIER  
- Membre B : Cecile FISCHER

On a inversé les rôles sans le vouloir pendant la séance sur les étapes 22 et 23 par rapport au rôle que nous avions.

## Git log

fba4320 (HEAD -> main, origin/main, origin/HEAD) Merge pull request #7 from Nathan876/interceptors

eb48f78 (origin/interceptors, interceptors) Fix: Interceptors bugs

a67bcb6 Merge pull request #6 from Nathan876/feat/interceptor

74a191f (origin/feat/interceptor) feat(test/interceptor): add header interceptor and  add end tests

ba1698f Feat: Add interceptors server (#5)

6c75c44 Merge pull request #4 from Nathan876/feat/test

8932bd6 (origin/feat/test) feat(test): add tests

6f7cfa9 Merge branch 'main' into feat/test

b41cc61 Service contract (#3)

993eee6 feat(test): add tests

a6e28a2 Merge pull request #2 from Nathan876/feat/client

fc39b5f (origin/feat/client) feat(client): add client

365a209 Merge pull request #1

bf5b027 squelette initial

a1d8356 Initial commit

## Questions

### Question 1 — Le contrat de service (`taskflow.proto`)

**(a)** C'est un enum, car cela permet de s'assurer qu'il possède bien l'un des trois statuts possibles. C'est aussi plus simple pour comparer le nouveau statut (new_status) avec le statut actuel. De plus, pour les filtres, cela simplifie également le processus (status_filter) pour choisir parmi les valeurs possibles. En proto3, si le client n'envoie pas un champ explicitement, Protobuf lui donne automatiquement sa valeur par défaut, qui est toujours l'élément 0 de l'énumération.

**(b)** Un champ scalaire stocke une seule et unique valeur d'un type basique, tandis que repeated Comment comments stocke une liste de zéro, un ou plusieurs éléments complexes (des messages).

**(c)** Cela permet d'encapsuler les données pour indiquer que ce type n'a de sens que dans ce contexte. Cela permet de créer une architecture plus propre et plus lisible.

### ⚠️ Question piège (12)

Si la queue de Bob n'est pas retirée lors de la déconnexion, le serveur continue de stocker les messages qui lui sont destinés, ce qui provoque une fuite de mémoire. De plus, le serveur continuera de considérer ce client comme abonné : le système va tenter de lui distribuer des données. Ces tentatives vont s'accumuler et occuper inutilement les threads du pool d'exécution. Cela va finir par saturer le serveur et provoquer un crash.

### 1. Unaire vs streaming

1. **Unary** → Le client envoie une requête et le serveur renvoie une réponse unique. On l'utilise par exemple pour CreateTask ou GetTask : ce mode convient bien aux opérations CRUD classiques.
2. **Server-streaming** → Le client envoie une requête et le serveur renvoie un flux de données. On l'utilise pour Subscribe ou ListTasks, car ces réponses peuvent être volumineuses : cela évite d'envoyer une très grande liste de tâches d'un seul coup.
3. **Client-streaming** → Le client envoie un flux continu de données et le serveur répond par un bilan global. C'est utile par exemple pour l'import de tâches, lorsqu'on envoie un gros volume de données.

### 2. Concurrence

Le serveur gRPC Python utilise un ThreadPoolExecutor. Chaque requête est traitée par un thread différent, donc si plusieurs requêtes accèdent à _tasks et le modifient en même temps, cela pose problème. Deux requêtes simultanées corrompraient l'état sans verrou si, par exemple, deux utilisateurs créaient une tâche en même temps : les deux compteraient le nombre de tâches présentes et attribueraient le même id. Deux id identiques seraient alors problématiques.

### 3. REST vs gRPC

Avec un WebSocket.

### 4. abort dans un stream

Oui, le client verrait les deux tâches. En gRPC, les messages d'un flux sont envoyés sous forme de trames indépendantes : si le serveur fait un yield de deux tâches, celles-ci partent immédiatement sur le réseau et le client les consomme au fur et à mesure. Si le serveur appelle ensuite context.abort(), gRPC envoie des trailers marquant la fin du flux avec un code d'erreur. Côté client, la boucle for task in stub.ListTasks(...) traitera les deux premières tâches, puis lèvera une exception au moment de lire la troisième.

### 5. Modèle interne vs message

Les classes générées par Protobuf sont des DTO prévus pour la sérialisation. En isolant le stockage via des dict, on sépare la logique applicative du protocole de communication. Le risque, si l'on stocke un objet Task par exemple et qu'on le renvoie directement à un client, est que gRPC commence à le lire en arrière-plan ; si au même moment un autre client appelle AddComment et modifie ce même objet, la sérialisation risque d'échouer ou de renvoyer un état incohérent.

C'est pourquoi AddComment récupère le dict de la tâche, ajoute le commentaire à la liste Python, puis instancie un nouveau message Protobuf propre pour le renvoyer.

### 6. Binôme

Nous n'avons pas eu de désaccord majeur. Si cela avait été le cas, nous aurions fait des recherches pour déterminer laquelle des solutions aurait été la plus sécurisée et la plus optimale.
