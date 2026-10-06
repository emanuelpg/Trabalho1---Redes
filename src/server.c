#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "../include/protocol.h"

#define MAX_WORKERS 32

/* 
 * Estrutura para manter o registo de cada Worker ligado.
 * socket_fd: descritor de ficheiro do socket do cliente (-1 indica livre).
 * is_busy: flag que assinala se o worker está processando uma tarefa.
 */
typedef struct {
    int socket_fd;
    int is_busy;
} WorkerNode;

/* Tabela de workers compartilhada entre várias threads */
WorkerNode workers[MAX_WORKERS];

/* 
 * Mutex para proteger o acesso concorrente ao array 'workers'.
 * Impede condições de corrida (race conditions) quando múltiplos
 * clientes conectam ou desconectam em simultâneo.
 */
pthread_mutex_t workers_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Garante o envio integral de 'len' bytes pelo socket.
 * O socket TCP pode fragmentar dados em pacotes menores, logo a função
 * 'send()' pode transmitir menos bytes do que o requisitado numa só chamada.
 */
ssize_t send_all(int fd, const void *buf, size_t len) {
    size_t total_sent = 0;
    const char *ptr = (const char *)buf;
    while (total_sent < len) {
        ssize_t n = send(fd, ptr + total_sent, len - total_sent, 0);
        if (n <= 0) return n; /* Erro de transmissão ou cliente fechou conexão */
        total_sent += n;
    }
    return total_sent;
}

/*
 * Garante a leitura de exatamente 'len' bytes do socket.
 * A função 'recv()' pode retornar com pacotes parciais; este ciclo
 * só conclui quando o buffer for totalmente preenchido.
 */
ssize_t recv_all(int fd, void *buf, size_t len) {
    size_t total_recv = 0;
    char *ptr = (char *)buf;
    while (total_recv < len) {
        ssize_t n = recv(fd, ptr + total_recv, len - total_recv, 0);
        if (n <= 0) return n; /* Erro ou conexão encerrada */
        total_recv += n;
    }
    return total_recv;
}

/*
 * Regista o socket_fd de um novo Worker num slot disponível do array.
 * Requer bloqueio de mutex para segurança multithread.
 */
void register_worker(int client_fd) {
    pthread_mutex_lock(&workers_mutex);
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].socket_fd == -1) {
            workers[i].socket_fd = client_fd;
            workers[i].is_busy = 0;
            printf("[SERVER] Worker registado no slot %d (fd: %d).\n", i, client_fd);
            break;
        }
    }
    pthread_mutex_unlock(&workers_mutex);
}

/*
 * Remove o registo de um Worker do array quando a conexão se perde.
 */
void unregister_worker(int client_fd) {
    pthread_mutex_lock(&workers_mutex);
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].socket_fd == client_fd) {
            workers[i].socket_fd = -1;
            workers[i].is_busy = 0;
            printf("[SERVER] Worker desconectado do slot %d.\n", i);
            break;
        }
    }
    pthread_mutex_unlock(&workers_mutex);
}

/*
 * Orquestra o processamento quando o cliente conectado é um SUBMITTER.
 * 1. Recebe o intervalo total.
 * 2. Verifica workers ativos.
 * 3. Divide o intervalo em fatias e distribui via socket para cada worker.
 * 4. Aguarda as respostas de cada worker, soma os totais e responde ao Submitter.
 */
void handle_submitter(int client_fd) {
    TaskPayload batch;
    if (recv_all(client_fd, &batch, sizeof(TaskPayload)) <= 0) {
        printf("[SERVER] Falha ao receber intervalo da tarefa do Submitter.\n");
        return;
    }

    printf("[SERVER] Tarefa recebida: intervalo [%lu a %lu]\n", batch.range_start, batch.range_end);

    /* Cria um snapshot dos workers disponíveis de forma atómica */
    pthread_mutex_lock(&workers_mutex);
    int active_workers[MAX_WORKERS];
    int count = 0;
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].socket_fd != -1 && !workers[i].is_busy) {
            active_workers[count++] = workers[i].socket_fd;
        }
    }
    pthread_mutex_unlock(&workers_mutex);

    /* Se não houver workers online, devolve 0 e avisa o submitter */
    if (count == 0) {
        printf("[SERVER] Aviso: Nenhum worker disponível para processamento.\n");
        Header err_hdr = { .type = MSG_FINAL_RESULT, .payload_size = sizeof(ResultPayload) };
        ResultPayload err_res = { .task_id = batch.task_id, .primes_count = 0 };
        send_all(client_fd, &err_hdr, sizeof(Header));
        send_all(client_fd, &err_res, sizeof(ResultPayload));
        return;
    }

    /* Divisão de trabalho (Map): reparte o intervalo equitativamente */
    uint64_t total_elements = batch.range_end - batch.range_start + 1;
    uint64_t chunk_size = total_elements / count;
    uint64_t total_primes = 0;

    for (int i = 0; i < count; i++) {
        uint64_t sub_start = batch.range_start + (i * chunk_size);
        /* O último worker absorve o resto da divisão */
        uint64_t sub_end = (i == count - 1) ? batch.range_end : (sub_start + chunk_size - 1);

        TaskPayload sub_task = {
            .task_id = (uint64_t)i + 1,
            .range_start = sub_start,
            .range_end = sub_end
        };

        /* Envia o cabeçalho e os dados do bloco ao worker */
        Header req_hdr = { .type = MSG_TASK_ASSIGN, .payload_size = sizeof(TaskPayload) };
        send_all(active_workers[i], &req_hdr, sizeof(Header));
        send_all(active_workers[i], &sub_task, sizeof(TaskPayload));
    }

    /* Redução (Reduce): recolhe a contagem de cada worker e totaliza */
    for (int i = 0; i < count; i++) {
        Header res_hdr;
        ResultPayload res_data;
        if (recv_all(active_workers[i], &res_hdr, sizeof(Header)) > 0 &&
            recv_all(active_workers[i], &res_data, sizeof(ResultPayload)) > 0) {
            total_primes += res_data.primes_count;
        }
    }

    /* Devolve o somatório final ao Submitter */
    Header out_hdr = { .type = MSG_FINAL_RESULT, .payload_size = sizeof(ResultPayload) };
    ResultPayload final_payload = { .task_id = batch.task_id, .primes_count = total_primes };
    send_all(client_fd, &out_hdr, sizeof(Header));
    send_all(client_fd, &final_payload, sizeof(ResultPayload));

    printf("[SERVER] Tarefa concluída com sucesso. Total de primos: %lu\n", total_primes);
}

/*
 * Rotina executada em paralelo por cada thread criada para atender um cliente.
 */
void *client_thread(void *arg) {
    int client_fd = *(int *)arg;
    free(arg); /* Liberta a memória alocada no 'main' para o ponteiro */

    /* 1. Lê o cabeçalho de identificação inicial */
    Header hdr;
    if (recv_all(client_fd, &hdr, sizeof(Header)) <= 0) {
        close(client_fd);
        return NULL;
    }

    /* 2. Lê a identificação do papel (ROLE_WORKER ou ROLE_SUBMITTER) */
    uint32_t role;
    if (recv_all(client_fd, &role, sizeof(uint32_t)) <= 0) {
        close(client_fd);
        return NULL;
    }

    /* 3. Ramifica o fluxo consoante o papel informado */
    if (role == ROLE_WORKER) {
        register_worker(client_fd);
        
        /* 
         * Mantém a thread aberta enquanto o worker estiver vivo.
         * 'MSG_PEEK' espia o buffer sem consumir dados: se o retorno for <= 0,
         * significa que o socket foi fechado ou interrompido.
         */
        char ping;
        while (recv(client_fd, &ping, 1, MSG_PEEK) > 0) {
            sleep(1);
        }
        
        unregister_worker(client_fd);
    } else if (role == ROLE_SUBMITTER) {
        handle_submitter(client_fd);
    }

    /* Fecha a conexão após a conclusão do atendimento */
    close(client_fd);
    return NULL;
}

int main(int argc, char *argv[]) {
    int port = (argc > 1) ? atoi(argv[1]) : DEFAULT_PORT;
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    /* Inicializa a tabela de workers com descritores vazios */
    for (int i = 0; i < MAX_WORKERS; i++) workers[i].socket_fd = -1;

    /* 
     * 1. Criação do socket TCP:
     * AF_INET = Protocolo IPv4
     * SOCK_STREAM = Protocolo TCP orientado a conexão
     */
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Erro ao criar socket");
        exit(EXIT_FAILURE);
    }

    /* 
     * 2. Configuração de reutilização imediata da porta (SO_REUSEADDR):
     * Impede que o socket fique preso em estado TIME_WAIT ao reiniciar o servidor.
     */
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("Erro em setsockopt");
        exit(EXIT_FAILURE);
    }

    /* 3. Configuração dos endereços do servidor */
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; /* Escuta em qualquer interface de rede */
    address.sin_port = htons(port);       /* Converte porta para Network Byte Order */

    /* 4. Ligação do socket ao endereço e porta especificados */
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Erro em bind");
        exit(EXIT_FAILURE);
    }

    /* 5. Coloca o socket em modo de escuta para aguardar conexões */
    if (listen(server_fd, 10) < 0) {
        perror("Erro em listen");
        exit(EXIT_FAILURE);
    }

    printf("[SERVER] Coordenador a escutar na porta %d...\n", port);

    /* 6. Ciclo infinito para receção contínua de novas conexões */
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        
        /* Aloca o descritor na heap para evitar condições de corrida com a thread */
        int *client_fd = malloc(sizeof(int));
        *client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (*client_fd < 0) {
            perror("Erro em accept");
            free(client_fd);
            continue;
        }

        /* 7. Cria uma thread separada para tratar a nova conexão */
        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, client_fd) != 0) {
            perror("Erro ao instanciar thread");
            close(*client_fd);
            free(client_fd);
            continue;
        }

        /* 
         * 'pthread_detach': liberta automaticamente os recursos da thread
         * no sistema operativo quando esta termina, sem necessidade de 'pthread_join'.
         */
        pthread_detach(tid);
    }

    close(server_fd);
    return 0;
}