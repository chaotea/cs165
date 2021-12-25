#define _DEFAULT_SOURCE
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "cs165_api.h"
#include "utils.h"
#include <errno.h>


// In this class, there will always be only one active database at a time
Db* current_db;


/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status* ret_status) {
	if (db != current_db) {
		log_err("Error creating table. Requested db is not the current db.\n");
		ret_status->code = ERROR;
		return NULL;
	}

	// If there is no more capacity in the db
	if (db->tables_size == db->tables_capacity) {
		size_t new_capacity;
		if (db->tables_capacity == 0) {
			// If the original capacity is 0, calloc 1 table
			new_capacity = 1;
			db->tables = calloc(new_capacity, sizeof(Table));
		} else {
			// Otherwise, double the capacity and realloc
			new_capacity = db->tables_capacity * 2;
			db->tables = realloc(db->tables, new_capacity * sizeof(Table));
		}
		db->tables_capacity = new_capacity;
	}

	// Add the table and adjust the size of the db
	// tables_size acts as the index
	Table* table = malloc(sizeof(Table));
	db->tables[db->tables_size] = table;
	db->tables_size++;

	// Initialize the table fields
	strcpy(table->name, name);
	// table->name[strlen(name)] = '\0';
	table->columns = calloc(num_columns, sizeof(Column));
	table->col_count = num_columns;
	table->col_idx = 0;
	table->table_length = 0;
	table->table_capacity = DEFAULT_COL_SIZE;

	ret_status->code = OK;
	return table;
}


/*
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char* db_name) {
	Status ret_status;

	if (current_db) {
		log_err("Error creating database. There is already a database currently active.\n");
		ret_status.code = ERROR;
		return ret_status;
	}

	current_db = malloc(sizeof(Db));
	strcpy(current_db->name, db_name);
	current_db->tables = NULL;
	current_db->tables_size = 0;
	current_db->tables_capacity = 0;

	ret_status.code = OK;
	return ret_status;
}


/*
 * Create column
 */
Column* create_column(Table* table, char* name, int sorted, Status* ret_status) {
	(void) sorted;

	if (table->col_idx == table->col_count) {
		log_err("Error creating column. Table is full\n");
		ret_status->code = ERROR;
		return NULL;
	}

	// Add the column
	Column* column = malloc(sizeof(Column));
	table->columns[table->col_idx] = column;
	table->col_idx++;

	// Initialize column fields
	strcpy(column->name, name);
	// column->name[strlen(name)] = '\0';
	column->data = calloc(DEFAULT_COL_SIZE, sizeof(int));
	column->index = NULL;
	column->length = 0;

	ret_status->code = OK;
	return column;
}


// Insert into table
Status relational_insert(Table* table, int* values) {
	Status ret_status;

	// Increase table capacity if required
	if (table->table_length == table->table_capacity) {
		size_t new_capacity = table->table_capacity * 2;
		for (size_t i = 0; i < table->col_count; i++) {
			table->columns[i] = realloc(table->columns[i]->data, new_capacity * sizeof(int));
		}
		table->table_capacity = new_capacity;
	}

	for (size_t i = 0; i < table->col_count; i++) {
		table->columns[i]->data[table->table_length] = values[i];
		table->columns[i]->length++;
	}
	table->table_length++;

	free(values);

	ret_status.code = OK;
	return ret_status;
}


Result* select_column(Column* column, int lower, int upper, Status* ret_status) {
	Result* result = malloc(sizeof(Result));
	result->num_tuples = 0;
	result->capacity = DEFAULT_COL_SIZE;
	result->data_type = INDEX;
	size_t* indexes = calloc(DEFAULT_COL_SIZE, sizeof(size_t));

	bool no_lower = (lower == 0) ? true : false;
	bool no_upper = (upper == 0) ? true : false;

	for (size_t i = 0; i < column->length; i++) {
		if (
			(no_lower || column->data[i] >= lower) &&
			(no_upper || column->data[i] < upper)
		) {
			if (result->num_tuples == result->capacity) {
				result->capacity = result->capacity * 2;
				indexes = realloc(indexes, result->capacity * sizeof(size_t));
			}
			indexes[result->num_tuples] = i;
			result->num_tuples++;
		}
	}

	ret_status->code = OK;
	result->payload = indexes;
	return result;
}

Result* fetch(Column* column, Result* positions, Status* ret_status) {
	Result* result = malloc(sizeof(Result));
	result->num_tuples = positions->num_tuples;
	result->capacity = positions->num_tuples;
	result->data_type = INT;

	int* values = calloc(positions->num_tuples, sizeof(int));
	for (size_t i = 0; i < positions->num_tuples; i++) {
		size_t index = *((size_t*) positions->payload + i);
		values[i] = column->data[index];
	}

	ret_status->code = OK;
	result->payload = values;
	return result;
}


char* print_result(Result* result, Status* ret_status) {
	size_t len = 0;
	char tuple[128];
	char* response;

	// TODO: send an array of ints instead of chars

	if (result->data_type == INT) {
		for (size_t i = 0; i < result->num_tuples; i++) {
			len += sprintf(tuple, "%d", *((int*) result->payload + i)) + 1;
		}

		response = malloc((len + 1) * sizeof(char));
		memset(response, 0, len + 1);

		for (size_t j = 0; j < result->num_tuples; j++) {
			sprintf(response, "%s%d\n", response, *((int*) result->payload + j));
		}

		response[len] = '\0';

	} else if (result->data_type == LONG) {
		for (size_t i = 0; i < result->num_tuples; i++) {
			len += sprintf(tuple, "%ld", *((long int*) result->payload + i)) + 1;
		}

		response = malloc((len + 1) * sizeof(char));
		memset(response, 0, len + 1);

		for (size_t j = 0; j < result->num_tuples; j++) {
			sprintf(response, "%s%ld\n", response, *((long int*) result->payload + j));
		}

		response[len] = '\0';
		
	} else if (result->data_type == FLOAT) {
		for (size_t i = 0; i < result->num_tuples; i++) {
			len += sprintf(tuple, "%.2f", *((float*) result->payload + i)) + 1;
		}

		response = malloc((len + 1) * sizeof(char));
		memset(response, 0, len + 1);

		for (size_t j = 0; j < result->num_tuples; j++) {
			sprintf(response, "%s%.2f\n", response, *((float*) result->payload + j));
		}

		response[len] = '\0';
	}

	ret_status->code = OK;
	return response;
}


Result* add_values(Result* first, Result* second, Status* ret_status) {
	Result* result = malloc(sizeof(Result));
	result->num_tuples = first->num_tuples;
	result->capacity = first->num_tuples;
	result->data_type = INT;

	int* values = calloc(result->num_tuples, sizeof(int));
	for (size_t i = 0; i < result->num_tuples; i++) {
		values[i] = *((int*) first->payload + i) + *((int*) second->payload + i);
	}

	ret_status->code = OK;
	result->payload = values;
	return result;
}


Result* subtract_values(Result* first, Result* second, Status* ret_status) {
	Result* result = malloc(sizeof(Result));
	result->num_tuples = first->num_tuples;
	result->capacity = first->num_tuples;
	result->data_type = INT;

	int* values = calloc(result->num_tuples, sizeof(int));
	for (size_t i = 0; i < result->num_tuples; i++) {
		values[i] = *((int*) first->payload + i) - *((int*) second->payload + i);
	}

	ret_status->code = OK;
	result->payload = values;
	return result;
}


Result* calculate_sum(Result* values, Status* ret_status) {
	Result* result = malloc(sizeof(Result));
	result->num_tuples = 1;
	result->capacity = 1;
	result->data_type = LONG;
	result->payload = malloc(sizeof(long int));

	long int sum = 0;
	for (size_t i = 0; i < values->num_tuples; i++) {
		sum += (long int) *((int*) values->payload + i);
	}

	ret_status->code = OK;
	*((long int*) result->payload) = sum;
	return result;
}

Result* calculate_average(Result* values, Status* ret_status) {
	Result* result = malloc(sizeof(Result));
	result->num_tuples = 1;
	result->capacity = 1;
	result->data_type = FLOAT;
	result->payload = malloc(sizeof(float));

	long int sum = 0;
	for (size_t i = 0; i < values->num_tuples; i++) {
		sum += (long int) *((int*) values->payload + i);
	}
	float avg = sum / (float) values->num_tuples;

	ret_status->code = OK;
	*((float*) result->payload) = avg;
	return result;
}

Result* calculate_max(Result* values, Status* ret_status) {
	Result* result = malloc(sizeof(Result));
	result->num_tuples = 1;
	result->capacity = 1;
	result->data_type = INT;
	result->payload = malloc(sizeof(INT));

	int max = *((int*) values->payload);
	for (size_t i = 1; i < values->num_tuples; i++) {
		if (*((int*) values->payload + i) > max) {
			max = *((int*) values->payload + i);
		}
	}

	ret_status->code = OK;
	*((int*) result->payload) = max;
	return result;
}

Result* calculate_min(Result* values, Status* ret_status) {
	Result* result = malloc(sizeof(Result));
	result->num_tuples = 1;
	result->capacity = 1;
	result->data_type = INT;
	result->payload = malloc(sizeof(INT));

	int min = *((int*) values->payload);
	for (size_t i = 1; i < values->num_tuples; i++) {
		if (*((int*) values->payload + i) < min) {
			min = *((int*) values->payload + i);
		}
	}

	ret_status->code = OK;
	*((int*) result->payload) = min;
	return result;
}


// Load database from file
// TODO: send the file from client to server
Status load_table(const char* file_name) {
	Status ret_status;

	FILE* fp;
	fp = fopen(file_name, "r");
	if (!fp) {
		log_err("Unable to open file\n");
		ret_status.code = ERROR;
		return ret_status;
	}


	char buf[BUF_SIZE];
	if (fgets(buf, BUF_SIZE, fp) == NULL) {
		log_err("Empty file\n");
	}

	// Get the table name
    char* header;
    char* to_free;
    header = to_free = malloc((strlen(buf)+1) * sizeof(char));
    strcpy(header, buf);
    // Separate the db name from table name
    char* db_name = strsep(&header, ".");
	// If there is no dot
    if (db_name == NULL) {
        ret_status.code = ERROR;
		return ret_status;
    }
    // Check that the database is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        ret_status.code = ERROR;
		return ret_status;
    }
    // Separate the table name from the rest of the header
    char* table_name = strsep(&header, ".");
    if (table_name == NULL) {
        ret_status.code = ERROR;
		return ret_status;
    }
    // Check that the table is in the database
	Table* table = NULL;
    for (size_t i = 0; i < current_db->tables_size; i++) {
        if (strcmp(current_db->tables[i]->name, table_name) == 0) {
            table = current_db->tables[i];
        }
    }
	if (table == NULL) {
		ret_status.code = ERROR;
		return ret_status;
	}
    free(to_free);

	// parse the data
	while (fgets(buf, BUF_SIZE, fp) != NULL) {
		char* tokenizer_copy;
		tokenizer_copy = to_free = malloc((strlen(buf)+1) * sizeof(char));
		char* token;
    	strcpy(tokenizer_copy, buf);

		int* values = malloc(table->col_count * sizeof(int));
		size_t idx = 0;

		char** insert_index = &tokenizer_copy;
		while ((token = strsep(insert_index, ",")) != NULL) {
            int insert_val = atoi(token);
            values[idx] = insert_val;
            idx++;
        }
        // check that we received the correct number of input values
        if (idx != table->col_count) {
            free(to_free);
			ret_status.code = ERROR;
			return ret_status;
        }

		free(to_free);
		relational_insert(table, values);
	}

	fclose(fp);

	ret_status.code = OK;
	return ret_status;
}


Status db_startup() {
	Status ret_status;

	FILE* fp;
	char meta_path[strlen(MAINDIR) + strlen(METADATA_FILE_NAME) + 2];
	sprintf(meta_path, "%s/%s", MAINDIR, METADATA_FILE_NAME);

	fp = fopen(meta_path, "r");
	if (!fp) {
		log_err("Unable to open file\n");
		ret_status.code = ERROR;
		return ret_status;
	}

	char buf[BUF_SIZE];
	if (fgets(buf, BUF_SIZE, fp) == NULL) {
		log_err("Empty file\n");
		fclose(fp);
		ret_status.code = ERROR;
		return ret_status;
	}

	char* line = buf;
	char* db_name = strsep(&line, ",");
	char* table_count = strsep(&line, ",");
	int num_tables = atoi(table_count);

	if (create_db(db_name).code != OK) {
		log_err("Couldn't create db");
		fclose(fp);
		ret_status.code = ERROR;
		return ret_status;
	}

	for (int i = 0; i < num_tables; i++) {
		fgets(buf, BUF_SIZE, fp);
		line = buf;
		char* table_name = strsep(&line, ",");
		char* column_count = strsep(&line, ",");
		char* table_length = strsep(&line, ",");
		int num_columns = atoi(column_count);
		int length = atoi(table_length);

		Status rstatus;
		Table* table = create_table(current_db, table_name, num_columns, &rstatus);
		if (rstatus.code != OK) {
			log_err("Couldn't create table");
			fclose(fp);
			ret_status.code = ERROR;
			return ret_status;
		}
		table->table_length = length;

		for (int j = 0; j < num_columns; j++) {
			fgets(buf, BUF_SIZE, fp);
			line = buf;
			int last_char = strlen(line) - 1;
			line[last_char] = '\0';
			Column* column = create_column(table, line, 0, &rstatus);
			if (rstatus.code != OK) {
				log_err("Couldn't create column");
				fclose(fp);
				ret_status.code = ERROR;
				return ret_status;
			}
			column->data = realloc(column->data, table->table_length * sizeof(int));
			column->length = table->table_length;

			// Set the path name
			char path[MAX_SIZE_NAME * 3 + strlen(MAINDIR) + 8];
			sprintf(path, "%s/%s.%s.data", MAINDIR, table->name, column->name);

			// Open the persistence file
			int fd;
			fd = open(path, O_RDWR, 0600);
			if (fd < 0) {
				log_err("Opening persistence file\n");
				fclose(fp);
				ret_status.code = ERROR;
    			return ret_status;
			}

			// Map the contents of the file to memory
			int* data = (int*) mmap(0, table->table_length * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
			if (data == MAP_FAILED) {
				close(fd);
				fclose(fp);
				log_err("Mmapping file failed\n");
				ret_status.code = ERROR;
    			return ret_status;
			}

			// TODO: set column->data = data
			// and directly modify memory mapped data

			// Write data from file to column
			for (size_t i = 0; i < table->table_length; i++) {
				column->data[i] = data[i];
			}

			// TODO: sync and unmap in shutdown, not startup
			
			// Unmap the file and close the file descriptor
			if (munmap(data, table->table_length * sizeof(int)) == -1) {
				close(fd);
				fclose(fp);
				log_err("Munmap file failed\n");
				ret_status.code = ERROR;
    			return ret_status;
			}

			close(fd);
		}
	}

	fclose(fp);

	ret_status.code = OK;
	return ret_status;
}


Status db_shutdown() {
    Status ret_status;

	// If no db currently active, return OK
	if (!current_db) {
		ret_status.code = OK;
    	return ret_status;
	}

	// Create new directory for database
	struct stat st;
	if (stat(MAINDIR, &st) == -1) {
		log_test("FOUND\n");
		mkdir(MAINDIR, 0777);
	}

	// Construct metadata file
	FILE* fp;
	char meta_path[strlen(MAINDIR) + strlen(METADATA_FILE_NAME) + 2];
	sprintf(meta_path, "%s/%s", MAINDIR, METADATA_FILE_NAME);
	fp = fopen(meta_path, "w");
	if (!fp) {
		log_err("Unable to create metadata file\n");
		log_err("Error: %s\n", strerror(errno));
		ret_status.code = ERROR;
		return ret_status;
	}
	fprintf(fp, "%s,%zu\n", current_db->name, current_db->tables_size);
	for (size_t tbl = 0; tbl < current_db->tables_size; tbl++) {
		Table* table = current_db->tables[tbl];
		fprintf(fp, "%s,%zu,%zu\n", table->name, table->col_count, table->table_length);
		for (size_t col = 0; col < table->col_count; col++) {
			Column* column = table->columns[col];
			fprintf(fp, "%s\n", column->name);
		}
	}
	fclose(fp);


	// Loop through the tables in the db
	for (size_t tbl = 0; tbl < current_db->tables_size; tbl++) {
		Table* table = current_db->tables[tbl];

		// Store columns of table as files
		for (size_t col = 0; col < table->col_count; col++) {
			Column* column = table->columns[col];

			// Set the path name
			char path[MAX_SIZE_NAME * 2 + strlen(MAINDIR) + 8];
			sprintf(path, "%s/%s.%s.data", MAINDIR, table->name, column->name);
			// char path[MAX_SIZE_NAME * 3 + 16];
			// sprintf(path, "%s.%s.%s.data", current_db->name, table->name, column->name);

			// Open/create the file
			int fd;
			int rflag = -1;
			fd = open(path, O_RDWR | O_CREAT, 0600);
			if (fd < 0) {
				log_err("Opening persistence file failed\n");
				ret_status.code = ERROR;
    			return ret_status;
			}

			// Move the file pointer to the end of the file
			rflag = lseek(fd, table->table_length * sizeof(int) - 1, SEEK_SET);
			if (rflag == -1) {
				close(fd);
				log_err("Lseek in file failed\n");
				ret_status.code = ERROR;
    			return ret_status;
			}

			// Write an empty string to the end of the file
			rflag = write(fd, "", 1);
			if (rflag == -1) {
				close(fd);
				log_err("Writing to file failed\n");
				ret_status.code = ERROR;
    			return ret_status;
			}

			// Map the contents of the file to memory
			int* data = (int*) mmap(0, table->table_length * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
			if (data == MAP_FAILED) {
				close(fd);
				log_err("Mmapping file failed\n");
				ret_status.code = ERROR;
    			return ret_status;
			}

			// Write data from column to file
			for (size_t i = 0; i < table->table_length; i++) {
				data[i] = column->data[i];
			}

			// Sync the contents of the memory with the file and flush the pages to disk
			if (data != NULL && fd != -1) {
				rflag = msync(data, table->table_length * sizeof(int), MS_SYNC);
				if (rflag == -1) {
					close(fd);
					log_err("Msync file failed\n");
					ret_status.code = ERROR;
    				return ret_status;
				}
				rflag = munmap(data, table->table_length * sizeof(int));
				if (rflag == -1) {
					close(fd);
					log_err("Munmap file failed\n");
					ret_status.code = ERROR;
    				return ret_status;
				}
				close(fd);
			}
			free(column->data);
			free(column->index);
			free(column);
		}
		free(table->columns);
		free(table);
	}
	free(current_db->tables);
	free(current_db);

	current_db = NULL;

	ret_status.code = OK;
    return ret_status;
}

char* execute_db_operator(DbOperator* query) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak.
    char* response = NULL;
    Status status;

    if (!query) {
        log_err("No query\n");
    } else if (query->type == CREATE) {
        if(query->operator_fields.create_operator.create_type == _DB) {
            if (create_db(query->operator_fields.create_operator.name).code != OK) {
                log_err("Create db failed\n");
            } else {
            	log_test("Create db succeeded\n");
			}
        } else if (query->operator_fields.create_operator.create_type == _TABLE) {
            create_table(query->operator_fields.create_operator.db,
                query->operator_fields.create_operator.name,
                query->operator_fields.create_operator.col_count,
                &status);
            if (status.code != OK) {
                log_err("Create table failed\n");
            } else {
            	log_test("Create table succeeded\n");
			}
        } else if (query->operator_fields.create_operator.create_type == _COLUMN){
            create_column(query->operator_fields.create_operator.table,
                query->operator_fields.create_operator.name,
                false,
                &status);
            if (status.code != OK) {
                log_err("Create column failed\n");
            } else {
            	log_test("Create column succeeded\n");
			}
        }
    } else if (query->type == LOAD) {
        if (load_table(query->operator_fields.load_operator.file_name).code != OK) {
            log_err("Load failed\n");
        } else {
        	log_test("Load succeeded\n");
		}
    } else if (query->type == INSERT) {
        if (relational_insert(query->operator_fields.insert_operator.table, query->operator_fields.insert_operator.values).code != OK) {
            log_err("Insert failed\n");
        } else {
        	log_test("Insert succeeded\n");
		}
    } else if (query->type == SELECT) {
        Result* indexes = select_column(query->operator_fields.select_operator.column,
            query->operator_fields.select_operator.lower,
            query->operator_fields.select_operator.upper,
            &status);
        if (status.code != OK) {
            log_err("Select failed\n");
        } else {
			log_test("Select succeeded\n");
		}
		query->operator_fields.select_operator.handle->generalized_column.column_type = RESULT;
		query->operator_fields.select_operator.handle->generalized_column.column_pointer.result = indexes;
    } else if (query->type == FETCH) {
        Result* result = fetch(query->operator_fields.fetch_operator.column,
            query->operator_fields.fetch_operator.positions,
            &status);
        if (status.code != OK) {
            log_err("Fetch failed\n");
        } else {
			log_test("Fetch succeeded\n");
		}
        query->operator_fields.fetch_operator.handle->generalized_column.column_type = RESULT;
        query->operator_fields.fetch_operator.handle->generalized_column.column_pointer.result = result;
    } else if (query->type == PRINT) {
        response = print_result(query->operator_fields.print_operator.result, &status);
        if (status.code != OK) {
            log_err("Print failed\n");
        } else {
        	log_test("Print succeeded\n");
		}
    } else if (query->type == ADD) {
        Result* result = add_values(query->operator_fields.math_operator.first,
            query->operator_fields.math_operator.second,
            &status);
        if (status.code != OK) {
            log_err("Add failed\n");
        } else {
			log_test("Add succeeded\n");
		}
        query->operator_fields.math_operator.handle->generalized_column.column_type = RESULT;
        query->operator_fields.math_operator.handle->generalized_column.column_pointer.result = result;
    } else if (query->type == SUBTRACT) {
        Result* result = subtract_values(query->operator_fields.math_operator.first,
            query->operator_fields.math_operator.second,
            &status);
        if (status.code != OK) {
            log_err("Subtract failed\n");
        } else {
			log_test("Subtract succeeded\n");
		}
        query->operator_fields.math_operator.handle->generalized_column.column_type = RESULT;
        query->operator_fields.math_operator.handle->generalized_column.column_pointer.result = result;
    } else if (query->type == AGGREGATE) {
        Result* result = NULL;
        if (query->operator_fields.aggregate_operator.aggregate_type == _SUM) {
            result = calculate_sum(query->operator_fields.aggregate_operator.values, &status);
            if (status.code != OK) {
                log_err("Sum failed\n");
            } else {
            	log_test("Sum succeeded\n");
			}
        } else if (query->operator_fields.aggregate_operator.aggregate_type == _AVG) {
            result = calculate_average(query->operator_fields.aggregate_operator.values, &status);
            if (status.code != OK) {
                log_err("Average failed\n");
            } else {
            	log_test("Average succeeded\n");
			}
        } else if (query->operator_fields.aggregate_operator.aggregate_type == _MAX) {
            result = calculate_max(query->operator_fields.aggregate_operator.values, &status);
            if (status.code != OK) {
                log_err("Max failed\n");
            } else {
				log_test("Max succeeded\n");
			}
        } else if (query->operator_fields.aggregate_operator.aggregate_type == _MIN) {
            result = calculate_min(query->operator_fields.aggregate_operator.values, &status);
            if (status.code != OK) {
                log_err("Min failed\n");
            } else {
				log_test("Min succeeded\n");
			}
        }
        query->operator_fields.aggregate_operator.handle->generalized_column.column_type = RESULT;
        query->operator_fields.aggregate_operator.handle->generalized_column.column_pointer.result = result;
    } else if (query->type == SHUTDOWN) {
        if (db_shutdown().code != OK) {
            log_err("Shutdown failed\n");
        } else {
			log_test("Shutdown succeeded\n");
		}
    } else {
        log_err("Unknown query while executing\n");
    }
    free(query);
    return response;
}