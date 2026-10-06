#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>

#define DEFAULT_PORT 8080
#define BUFFER_SIZE  1024

typedef enum {
    ROLE_SUBMITTER = 1,
    ROLE_WORKER    = 2
} ClientRole;

typedef enum {
    MSG_REGISTER_REQ = 1,
    MSG_TASK_SUBMIT  = 2,
    MSG_TASK_ASSIGN  = 3,
    MSG_TASK_RESULT  = 4,
    MSG_FINAL_RESULT = 5
} MessageType;

/* Cabeçalho padrão de mensagem */
typedef struct {
    uint32_t type;    /* MessageType */
    uint32_t payload_size;
} Header;

/* Tarefa de contagem de primos no intervalo [range_start, range_end] */
typedef struct {
    uint64_t task_id;
    uint64_t range_start;
    uint64_t range_end;
} TaskPayload;

/* Resultado computado pelo worker ou agregado pelo servidor */
typedef struct {
    uint64_t task_id;
    uint64_t primes_count;
} ResultPayload;

#endif