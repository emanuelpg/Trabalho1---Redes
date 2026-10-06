#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <arpa/inet.h>
#include "../include/protocol.h"

/*
 * Garante a transmissão integral de 'len' bytes pelo socket TCP.
 * Como o TCP opera sobre streams contínuos, uma chamada a 'send()' pode 
 * não enviar todos os bytes solicitados de uma só vez devido a limites de buffer.
 */
ssize_t send_all(int fd, const void *buf, size_t len) {
    size_t total_sent = 0;
    const char *ptr = (const char *)buf;
    while (total_sent < len) {
        ssize_t n = send(fd, ptr + total_sent, len - total_sent, 0);
        if (n <= 0) return n; /* Erro de transmissão ou conexão encerrada */
        total_sent += n;
    }
    return total_sent;
}

/*
 * Garante a leitura de exatamente 'len' bytes do socket TCP.
 * O laço persiste até acumular a quantidade exata de bytes da estrutura esperada.
 */
ssize_t recv_all(int fd, void *buf, size_t len) {
    size_t total_recv = 0;
    char *ptr = (char *)buf;
    while (total_recv < len) {
        ssize_t n = recv(fd, ptr + total_recv, len - total_recv, 0);
        if (n <= 0) return n; /* Erro ou socket fechado pelo servidor */
        total_recv += n;
    }
    return total_recv;
}

/*
 * Algoritmo determinístico para checagem de primalidade.
 * Otimizado com a regra 6k +/- 1 para reduzir drasticamente o número de divisões.
 */
bool is_prime(uint64_t n) {
    if (n <= 1) return false;
    if (n <= 3) return true;
    if (n % 2 == 0 || n % 3 == 0) return false;
    for (uint64_t i = 5; i * i <= n; i += 6) {
        if (n % i == 0 || n % (i + 2) == 0) return false;
    }
    return true;
}

/*
 * Modo WORKER:
 * Permanece em ciclo ativo escutando ordens de trabalho vindas do coordenador.
 * Ao receber uma fatia de intervalo, calcula a quantidade de primos e devolve a resposta.
 */
void run_worker(int sock) {
    printf("[WORKER] Registado com sucesso no coordenador. A aguardar tarefas...\n");

    while (1) {
        Header hdr;
        /* Aguarda o cabeçalho da próxima mensagem enviada pelo servidor */
        if (recv_all(sock, &hdr, sizeof(Header)) <= 0) {
            printf("[WORKER] Ligação com o servidor perdida ou encerrada.\n");
            break;
        }

        /* Se a mensagem for uma atribuição de tarefa */
        if (hdr.type == MSG_TASK_ASSIGN) {
            TaskPayload task;
            if (recv_all(sock, &task, sizeof(TaskPayload)) <= 0) {
                printf("[WORKER] Erro ao obter dados do lote de tarefa.\n");
                break;
            }

            printf("[WORKER] Processando bloco %lu: intervalo [%lu até %lu]...\n", 
                   task.task_id, task.range_start, task.range_end);

            /* Realiza a computação da fatia recebida */
            uint64_t count = 0;
            for (uint64_t i = task.range_start; i <= task.range_end; i++) {
                if (is_prime(i)) {
                    count++;
                }
            }

            /* Prepara o cabeçalho e o payload com o resultado do bloco */
            Header res_hdr = { 
                .type = MSG_TASK_RESULT, 
                .payload_size = sizeof(ResultPayload) 
            };
            ResultPayload res_data = { 
                .task_id = task.task_id, 
                .primes_count = count 
            };

            /* Devolve o resultado ao servidor coordenador */
            send_all(sock, &res_hdr, sizeof(Header));
            send_all(sock, &res_data, sizeof(ResultPayload));
            
            printf("[WORKER] Bloco %lu finalizado com sucesso. Primos encontrados: %lu\n", 
                   task.task_id, count);
        }
    }
}

/*
 * Modo SUBMITTER:
 * Submete um intervalo global para cálculo, aguarda a consolidação distribuída
 * e exibe o somatório total recebido do servidor.
 */
void run_submitter(int sock, uint64_t start, uint64_t end) {
    /* Monta a estrutura da tarefa principal */
    TaskPayload task = { 
        .task_id = 100, 
        .range_start = start, 
        .range_end = end 
    };

    /* Envia os parâmetros da tarefa para o coordenador */
    send_all(sock, &task, sizeof(TaskPayload));
    printf("[SUBMITTER] Intervalo [%lu a %lu] enviado. Aguardando processamento distribuído...\n", start, end);

    /* Aguarda a resposta consolidada do servidor */
    Header hdr;
    ResultPayload res;
    if (recv_all(sock, &hdr, sizeof(Header)) > 0 && 
        recv_all(sock, &res, sizeof(ResultPayload)) > 0) {
        printf("[SUBMITTER] Resposta final recebida: %lu números primos encontrados no intervalo!\n", 
               res.primes_count);
    } else {
        printf("[SUBMITTER] Falha na receção da resposta final do coordenador.\n");
    }
}

int main(int argc, char *argv[]) {
    /* Validação dos argumentos passados via terminal */
    if (argc < 4) {
        printf("Uso incorreto. Sintaxe esperada:\n");
        printf("  Modo Worker:    %s <IP> <PORTA> worker\n", argv[0]);
        printf("  Modo Submitter: %s <IP> <PORTA> submit <INICIO> <FIM>\n", argv[0]);
        return 1;
    }

    const char *ip = argv[1];
    int port = atoi(argv[2]);
    const char *mode = argv[3];

    /* 
     * 1. Criação do socket TCP:
     * AF_INET: Endereçamento IPv4
     * SOCK_STREAM: Protocolo orientado a fluxo contínuo e confiável (TCP)
     */
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Erro ao criar socket");
        return 1;
    }

    /* 2. Configuração do endereço de destino do servidor */
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port); /* Converte a porta para Network Byte Order (Big-Endian) */

    /* Converte o IP textual (ex: "127.0.0.1") para binário */
    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        perror("Formato de endereço IP inválido");
        close(sock);
        return 1;
    }

    /* 3. Estabelece a ligação TCP (3-way handshake) com o servidor */
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Falha ao conectar com o servidor");
        close(sock);
        return 1;
    }

    /* 4. Handshake de Aplicação: identifica o papel do cliente perante o servidor */
    if (strcmp(mode, "worker") == 0) {
        uint32_t role = ROLE_WORKER;
        Header hdr = { .type = MSG_REGISTER_REQ, .payload_size = sizeof(uint32_t) };
        send_all(sock, &hdr, sizeof(Header));
        send_all(sock, &role, sizeof(uint32_t));
        
        run_worker(sock);
    } else if (strcmp(mode, "submit") == 0) {
        if (argc < 6) {
            printf("Erro: O modo submitter requer os limites: <INICIO> <FIM>\n");
            close(sock);
            return 1;
        }
        
        uint32_t role = ROLE_SUBMITTER;
        Header hdr = { .type = MSG_TASK_SUBMIT, .payload_size = sizeof(uint32_t) };
        send_all(sock, &hdr, sizeof(Header));
        send_all(sock, &role, sizeof(uint32_t));
        
        uint64_t start = strtoull(argv[4], NULL, 10);
        uint64_t end = strtoull(argv[5], NULL, 10);
        run_submitter(sock, start, end);
    } else {
        printf("Modo desconhecido: '%s'. Utilize 'worker' ou 'submit'.\n", mode);
    }

    /* 5. Encerramento seguro do descritor de socket */
    close(sock);
    return 0;
}