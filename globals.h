#ifndef _GLOBALS_
#define _GLOBALS_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#define RONLY_TRANSACTIONS    		0
#define READ_WRITE_TRANSACTIONS   		1
#define OTHER_TRANSACTIONS		2	 

#define MAXIMUM_SITES		11	
#define MAXIMUM_VARIABLES		21	

#define ALL_VARIABLES		100	
#define ALL_SITES		100	

#define READ_OPN		0
#define WRITE_OPN		1
#define END_OPN		2
#define ABORT_OPN		-2
#define DUMP_OPN		3
#define FAIL_OPN		-4
#define RECOVER_OPN	4
#define QUERY_STATE_OPN	5

#define OPN_PENDING	0
#define OPN_BLOCKED	-1
#define OPN_REJECTED	-2
#define OPN_IGNORE	-3	
#define OPN_COMPLETE	1
/********************************************************************************************************************
                                              Struct Operation Starts
****************************************************************************************************************************/
struct operation {
 int trnid ;				
 int opnType ;			
 int opnTimestamp ;
 int trnType ;			
 int trnTimestamp ;
 int variablenumber ;
 int writtenValue ;
 int readValue ;
 int sitenumber ;					
 int waitlistedOpnTicknumber ;	        
 int operationStatusAtSites[MAXIMUM_SITES] ; 	
 struct operation *nextOperationSite ;
 struct operation *nextOperationTM ;
} ;

void logString(char *) ;
#endif
