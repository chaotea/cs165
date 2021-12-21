/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 *
 * Getting started hints:
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/
char* execute_DbOperator(DbOperator* query) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak.
    char* result = NULL;
    Status status;

    if (!query) {
        log_err("No query\n");
    } else if (query->type == CREATE) {
        if(query->operator_fields.create_operator.create_type == _DB) {
            if (create_db(query->operator_fields.create_operator.name).code != OK) {
                log_err("Create db failed\n");
            }
            log_test("Create db succeeded\n");
        } else if (query->operator_fields.create_operator.create_type == _TABLE) {
            create_table(query->operator_fields.create_operator.db,
                query->operator_fields.create_operator.name,
                query->operator_fields.create_operator.col_count,
                &status);
            if (status.code != OK) {
                log_err("Create table failed\n");
            }
            log_test("Create table succeeded\n");
        } else if (query->operator_fields.create_operator.create_type == _COLUMN){
            create_column(query->operator_fields.create_operator.table,
                query->operator_fields.create_operator.name,
                false,
                &status);
            if (status.code != OK) {
                log_err("Create column failed\n");
            }
            log_test("Create column succeeded\n");
        }
    } else if (query->type == LOAD) {
        if (load_table(query->operator_fields.load_operator.file_name).code != OK) {
            log_err("Load failed\n");
        }
        log_test("Load succeeded\n");
    } else if (query->type == INSERT) {
        if (relational_insert(query->operator_fields.insert_operator.table, query->operator_fields.insert_operator.values).code != OK) {
            log_err("Insert failed\n");
        }
        log_test("Insert succeeded\n");
    } else if (query->type == SHUTDOWN) {
        if (shutdown_database().code != OK) {
            log_err("Shutdown failed\n");
        }
        log_test("Shutdown succeeded\n");
    } else {
        log_err("Unknown query\n");
    }
    free(query);
    return result;
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = NULL;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0)  {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length, 0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            //    Corresponding database operator is executed over the query
            char* result = NULL;
            if (query) {
                result = execute_DbOperator(query);
                if (result) {
                    send_message.length = strlen(result);
                    char send_buffer[send_message.length + 1];
                    strcpy(send_buffer, result);
                    send_message.payload = send_buffer;
                    send_message.status = OK_WAIT_FOR_RESPONSE;
                } else {
                    send_message.length = 0;
                    send_message.payload = NULL;
                    send_message.status = OK_DONE;
                }
            } else if (send_message.status == OK_DONE) {
                send_message.length = 0;
                send_message.payload = NULL;
            } else if (send_message.status == UNKNOWN_COMMAND) {
                send_message.length = 0;
                send_message.payload = NULL;
                log_err("Unknown command\n");
            } else if (send_message.status == QUERY_UNSUPPORTED) {
                send_message.length = 0;
                send_message.payload = NULL;
                log_err("Query unsupported\n");
            } else {
                send_message.length = 0;
                send_message.payload = NULL;
                log_err("Error parsing message\n");
            }

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
                log_err("Server failed to send message status\n");
                exit(1);
            }

            // 4. Send response to the request
            if (result && send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Triggered by %s\n", recv_message.payload);
                log_err("Result: %s\n", result);
                log_err("Server failed to send message body\n");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
//
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients?
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)

int main(void) {
    struct stat st;
	if (stat(MAINDIR, &st) != -1) {
		load_database();
	}

    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}
