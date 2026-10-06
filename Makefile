CC = gcc
CFLAGS = -Wall -Wextra -pedantic -std=c99 -pthread -I./include

all: server client

server: src/server.c
	$(CC) $(CFLAGS) src/server.c -o server

client: src/client.c
	$(CC) $(CFLAGS) src/client.c -o client

make run: all
	./server & ./client
clean:
	rm -f server client