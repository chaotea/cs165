/*
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <limits.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}


/**
 * Parse create column
 **/

DbOperator* parse_create_col(char* create_arguments, message* send_message) {
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &send_message->status);
    char* table_name = next_token(create_arguments_index, &send_message->status);

    // Incorrect number of arguments
    if (send_message->status == INCORRECT_FORMAT) {
        log_err("Incorrect number of arguments\n");
        return NULL;
    }

    // Get the column name free of quotation marks
    column_name = trim_quotes(column_name);

    // Read and chop off last char, which should be a ')'
    int last_char = strlen(table_name) - 1;
    if (table_name[last_char] != ')') {
        log_err("Missing ')' in query\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }

    // Replace the ')' with a null terminating character
    table_name[last_char] = '\0';

    Table* table = lookup_table(table_name);
    if (table == NULL) {
        log_err("Table not found\n");
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }

    // Make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, column_name);
    dbo->operator_fields.create_operator.table = table;
    return dbo;
}


/**
 * Parse create table
 **/

DbOperator* parse_create_tbl(char* create_arguments, message* send_message) {
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &send_message->status);
    char* db_name = next_token(create_arguments_index, &send_message->status);
    char* col_cnt = next_token(create_arguments_index, &send_message->status);

    // Incorrect number of arguments
    if (send_message->status == INCORRECT_FORMAT) {
        log_err("Incorrect number of arguments\n");
        return NULL;
    }

    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);

    // Read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        log_err("Missing ')' in query\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }

    // Replace the ')' with a null terminating character
    col_cnt[last_char] = '\0';

    // Check that the database is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        log_err("Database not active\n");
        send_message->status = INVALID_ARGUMENT;
        return NULL;
    }

    // Turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        log_err("Invalid column count\n");
        send_message->status = INVALID_ARGUMENT;
        return NULL;
    }

    // Make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}


/**
 * Parse create db
 **/

DbOperator* parse_create_db(char* create_arguments, message* send_message) {
    char* token = strsep(&create_arguments, ",");

    // Too few arguments
    if (token == NULL) {
        log_err("Incorrect number of arguments\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }

    // Get the db name free of quotation marks
    char* db_name = token;
    db_name = trim_quotes(db_name);

    // Read and chop off last char, which should be a ')'
    int last_char = strlen(db_name) - 1;
    if (db_name[last_char] != ')') {
        log_err("Missing ')' in query\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }

    // Replace the ')' with a null terminating character
    db_name[last_char] = '\0';

    // Too many arguments
    token = strsep(&create_arguments, ",");
    if (token != NULL) {
        log_err("Incorrect number of arguments\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }

    // Make create dbo for db
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _DB;
    strcpy(dbo->operator_fields.create_operator.name, db_name);
    return dbo;
}


/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/

DbOperator* parse_create(char* create_arguments, message* send_message) {
    DbOperator* dbo = NULL;
    char* tokenizer_copy;
    char* to_free;

    // Since strsep destroys input, we create a copy of our input.
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char* token;
    strcpy(tokenizer_copy, create_arguments);

    // Check for leading parenthesis after create.
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;

        // Token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            log_err("Incorrect number of arguments\n");
            return NULL;
        } else {
            // Pass off to next parse function.
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy, send_message);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy, send_message);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy, send_message);
            } else {
                send_message->status = UNKNOWN_COMMAND;
            }
        }
    } else {
        log_err("Missing '(' in query\n");
        send_message->status = INCORRECT_FORMAT;
    }
    free(to_free);
    return dbo;
}


/**
 * parse_insert reads in the arguments for a create statement and
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;

    // Check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;

        // Parse table input
        char* table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            log_err("Invalid number of arguments\n");
            return NULL;
        }

        // Lookup the table and make sure it exists
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // Make insert operator
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);

        // parse inputs until we reach the end. Turn each given string into an integer.
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free(dbo);
            return NULL;
        }
        return dbo;
    } else {
        log_err("Missing '(' in query\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
}


/**
 * parse_load
 **/

DbOperator* parse_load(char* load_arguments, message* send_message) {
    if (strncmp(load_arguments, "(", 1) == 0) {
        load_arguments++;
        char* token = strsep(&load_arguments, ",");

        // Too few arguments
        if (token == NULL) {
            log_err("Incorrect number of arguments\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // Get the file name free of quotation marks
        char* file_name = token;
        file_name = trim_quotes(file_name);

        // Read and chop off last char, which should be a ')'
        int last_char = strlen(file_name) - 1;
        if (file_name[last_char] != ')') {
            log_err("Missing ')' in query\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // Replace the ')' with a null terminating character
        file_name[last_char] = '\0';

        // Too many arguments
        token = strsep(&load_arguments, ",");
        if (token != NULL) {
            log_err("Incorrect number of arguments\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // Make load operator
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = LOAD;
        dbo->operator_fields.load_operator.file_name = file_name;
        return dbo;
    } else {
        log_err("Missing '(' in query\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
}


/**
 * parse_select
 **/

DbOperator* parse_select(char* select_arguments, message* send_message, GeneralizedColumnHandle* handle) {
    if (strncmp(select_arguments, "(", 1) == 0) {
        select_arguments++;
        char** select_arguments_index = &select_arguments;
        char* column_name = next_token(select_arguments_index, &send_message->status);
        char* lower_bound = next_token(select_arguments_index, &send_message->status);
        char* upper_bound = next_token(select_arguments_index, &send_message->status);

        // Incorrect number of arguments
        if (send_message->status == INCORRECT_FORMAT) {
            log_err("Incorrect number of arguments\n");
            return NULL;
        }

        // Lookup the column
        Column* column = lookup_column(column_name);
        if (column == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // Read and chop off last char, which should be a ')'
        int last_char = strlen(upper_bound) - 1;
        if (upper_bound[last_char] != ')') {
            log_err("Missing ')' in query\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // Replace the ')' with a null terminating character
        upper_bound[last_char] = '\0';

        // Parse the bounds
        int lower = (strcmp(lower_bound, "null") == 0) ? INT_MIN : atoi(lower_bound);
        int upper = (strcmp(upper_bound, "null") == 0) ? INT_MAX : atoi(upper_bound);

        // Make create dbo for table
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        dbo->operator_fields.select_operator.column = column;
        dbo->operator_fields.select_operator.lower = lower;
        dbo->operator_fields.select_operator.upper = upper;
        dbo->operator_fields.select_operator.handle = handle;
        return dbo;
    } else {
        log_err("Missing '(' in query\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
}


/**
 * parse_fetch
 **/

DbOperator* parse_fetch(char* fetch_arguments, message* send_message, ClientContext* context, GeneralizedColumnHandle* handle) {
    if (strncmp(fetch_arguments, "(", 1) == 0) {
        fetch_arguments++;
        char** fetch_arguments_index = &fetch_arguments;
        char* column_name = next_token(fetch_arguments_index, &send_message->status);
        char* positions_handle_name = next_token(fetch_arguments_index, &send_message->status);

        // Incorrect number of arguments
        if (send_message->status == INCORRECT_FORMAT) {
            log_err("Incorrect number of arguments\n");
            return NULL;
        }

        // Lookup the column
        Column* column = lookup_column(column_name);
        if (column == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // Read and chop off last char, which should be a ')'
        int last_char = strlen(positions_handle_name) - 1;
        if (positions_handle_name[last_char] != ')') {
            log_err("Missing ')' in query\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // Replace the ')' with a null terminating character
        positions_handle_name[last_char] = '\0';

        // Lookup the handle
        GeneralizedColumnHandle* positions_handle = lookup_handle(context, positions_handle_name);

        // Make create dbo for table
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        dbo->operator_fields.fetch_operator.column = column;
        dbo->operator_fields.fetch_operator.positions = positions_handle->generalized_column.column_pointer.result;
        dbo->operator_fields.fetch_operator.handle = handle;
        return dbo;
    } else {
        log_err("Missing '(' in query\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
}


DbOperator* parse_print(char* print_arguments, message* send_message, ClientContext* context) {
    if (strncmp(print_arguments, "(", 1) == 0) {
        print_arguments++;
        char* token = strsep(&print_arguments, ",");

        // Too few arguments
        if (token == NULL) {
            log_err("Incorrect number of arguments\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // Get the handle name free of quotation marks
        char* handle_name = token;
        handle_name = trim_quotes(handle_name);

        // Read and chop off last char, which should be a ')'
        int last_char = strlen(handle_name) - 1;
        if (handle_name[last_char] != ')') {
            log_err("Missing ')' in query\n");
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // Replace the ')' with a null terminating character
        handle_name[last_char] = '\0';

        // Lookup the handle name
        GeneralizedColumnHandle* handle = lookup_handle(context, handle_name);

        // Make print operator
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = PRINT;
        dbo->operator_fields.print_operator.result = handle->generalized_column.column_pointer.result;
        return dbo;
    } else {
        log_err("Missing '(' in query\n");
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
}


/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 *
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse?
 *      What if such command requires multiple arguments?
 **/

DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed.
    DbOperator* dbo = NULL; // = malloc(sizeof(DbOperator));

    // The -- signifies a comment line, no operator needed.
    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        return dbo;
    }

    char* equals_pointer = strchr(query_command, '=');
    char* handle_name = query_command;
    GeneralizedColumnHandle* handle;
    if (equals_pointer != NULL) {
        // handle exists, store here.
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle_name);

        handle = create_handle(context, handle_name);

        query_command = ++equals_pointer;
    } else {
        handle_name = NULL;
    }

    cs165_log(stdout, "QUERY: %s", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    // check what command is given.
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command, send_message);
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command, send_message);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(query_command, send_message, handle);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, send_message, context, handle);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, send_message, context);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return dbo;
    }

    // TODO: delete this later
    if (dbo == NULL) {
        send_message->status = QUERY_UNSUPPORTED;
        return dbo;
    }

    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
