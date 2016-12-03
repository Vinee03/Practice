#ifndef _GLOBALS_
#define _GLOBALS_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#define READONLY_TRANSACTION    		0
#define READWRITE_TRANSACTION   		1
#define MISCELLANEOUS_TRANSACTION		2	 

#define MAX_SITES		11	
#define MAX_VARIABLES		21	

#define ALL_VARIABLES		100	
#define ALL_SITES		100	

#define READ_OPERATION		0
#define WRITE_OPERATION		1
#define END_OPERATION		2
#define ABORT_OPERATION		-2
#define DUMP_OPERATION		3
#define FAIL_OPERATION		-4
#define RECOVER_OPERATION	4
#define QUERY_STATE_OPERATION	5

#define OPERATION_PENDING	0
#define OPERATION_BLOCKED	-1
#define OPERATION_REJECTED	-2
#define OPERATION_IGNORE	-3	
#define OPERATION_COMPLETE	1
/********************************************************************************************************************
                                              Struct Operation Starts
****************************************************************************************************************************/
struct operation {
 int trnid ;				
 int operationType ;			
 int operationTimestamp ;
 int transactionType ;			
 int transactionTimestamp ;
 int varNo ;
 int valueToWrite ;
 int valueRead ;
 int siteNo ;					
 int operationWaitListed_tickNo ;	        
 int operationStatusAtSites[MAX_SITES] ; 	
 struct operation *nextOperationSite ;
 struct operation *nextOperationTM ;
} ;

void logString(char *) ;
#endif
