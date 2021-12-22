#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(char* name);

Column* lookup_column(char* name);

GeneralizedColumnHandle* lookup_handle(ClientContext* context, char* name);

GeneralizedColumnHandle* create_handle(ClientContext* context, char* name);

#endif
