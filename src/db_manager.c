#define _DEFAULT_SOURCE
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "cs165_api.h"
#include "utils.h"


// In this class, there will always be only one active database at a time
Db* current_db;


/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status* ret_status) {
	if (db != current_db) {
		log_err("Error creating table. Requested db is not the current db.");
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
	Table* table = calloc(1, sizeof(Table));
	db->tables[db->tables_size] = table;
	db->tables_size++;

	// Initialize the table fields
	strcpy(table->name, name);
	table->name[strlen(name)] = '\0';
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
		log_err("Error creating database. There is already a database currently active.");
		ret_status.code = ERROR;
		return ret_status;
	}

	current_db = calloc(1, sizeof(Db));
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
		log_err("Error creating column. Table is full. %d", table->col_count);
		ret_status->code = ERROR;
		return NULL;
	}

	// Add the column
	Column* column = calloc(1, sizeof(Column));
	table->columns[table->col_idx] = column;
	table->col_idx++;

	// Initialize column fields
	strcpy(column->name, name);
	column->name[strlen(name)] = '\0';
	column->data = calloc(DEFAULT_COL_SIZE, sizeof(int));
	column->index = NULL;

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
	}
	table->table_length++;

	free(values);

	ret_status.code = OK;
	return ret_status;
}


// Load database from file
Status load_table(const char* file_name) {
	Status ret_status;

	log_test("Loading database from file %s\n", file_name);

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

Status shutdown_server() {
    Status ret_status;

	// If no db currently active, return OK
	if (!current_db) {
		ret_status.code = OK;
    	return ret_status;
	}

	// Create new directory for database
	// if (mkdir("data", 0600) == -1) {
	// 	log_err("Creating main directory failed\n");
	// 	ret_status.code = ERROR;
	// 	return ret_status;
	// }

	// Loop through the tables in the db
	for (size_t tbl = 0; tbl < current_db->tables_size; tbl++) {
		Table* table = current_db->tables[tbl];

		// Store columns of table as files
		for (size_t col = 0; col < table->col_count; col++) {
			Column* column = table->columns[col];

			// Set the path name
			char path[MAX_SIZE_NAME * 3 + 16];
			// sprintf(path, "../data/%s.%s.%s.data", current_db->name, table->name, column->name);
			sprintf(path, "%s.%s.%s.data", current_db->name, table->name, column->name);

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

	ret_status.code = OK;
    return ret_status;
}