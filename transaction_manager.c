#include "transaction_manager_struct.h"
#include "globalvar.h"
#include "site.h"
#include <ctype.h>
#include <sys/select.h>



#define WAIT_SITE_FAIL

#define SLEEP_DURATION_MS 	200

int availableSites[MAX_SITES] ;
int storeOperation(char *operationString, int operationtimestamp) ;
int prepareOperationNode(int trnid, int operationType, int varNo, int valueToWrite, int siteNo, int operationtimestamp, struct operation *opn) ;
void addOperationToTransactionQueue(int trnid, struct operation *opn) ;
void getSites(int varNo, int trySites[]) ;
int createNewTransaction(int trnid, int transactionType, int timestamp) ;
void abortTransaction(struct operation *opn) ;
void Sleep_ms(int time_ms) ;

#define WRITE_PENDING  1
#define WRITE_CHECK_FOR_COMPLETE 0
#define WRITE_FAILED   -1

void startTransactionManager() {
  int tickNo = 0 ;
  int trnid ;
  int flagPending = 1 ;
  char log_desc[1000] ;
  while(flagPending != 0) {
    sprintf(log_desc, "\n\n\n*******startTransactionManager: tickNo %d************\n", tickNo) ;
    logString(log_desc) ;
    flagPending = 0 ;


    while( T[MISCELLANEOUS_TID].current_opn != NULL ) {
      if(flagPending == 0)
	flagPending = 1 ;
      if(T[MISCELLANEOUS_TID].current_opn->operationTimestamp > tickNo) {
        break ;
      }
      if(T[MISCELLANEOUS_TID].timestamp <= tickNo && T[MISCELLANEOUS_TID].current_opn->operationTimestamp <= tickNo) {
        struct operation *opn = T[MISCELLANEOUS_TID].current_opn ;
        if(opn->operationType == DUMP_OPERATION) {	//Dump could be on a single site or on all sites containing a specific variable or on all sites for all variables
          if(opn->varNo == ALL_VARIABLES) {
            if(opn->siteNo == ALL_SITES) {	//dump(): The dump is to be performed for all variables on all sites
              int siteNo ;
              for(siteNo = 1 ; siteNo < MAX_SITES ; siteNo++) {
                if(siteInfo[siteNo].flag_siteAvailable == 0)	//We do not send operations to failed sites
                  continue ;
                performOperation(opn, siteNo) ;		//Send dump operation for the given site
                int operationStatus = opn->operationStatusAtSites[siteNo] ;
#ifdef _DEBUG_
                if(operationStatus == OPERATION_REJECTED)
                 printf("Dump operation rejected @ site %d\n", siteNo) ;
                if(operationStatus == OPERATION_BLOCKED)
                 printf("Dump operation @ site %d\n", siteNo) ;
#endif
                if(operationStatus != OPERATION_COMPLETE) {
#ifdef _DEBUG_
                  printf("startTransactionManager: site %d did not set dump operation state to complete\n", siteNo ) ;
#endif
                  opn->operationStatusAtSites[siteNo] = OPERATION_COMPLETE ;
                }
              }
            }
            else if(opn->siteNo != ALL_SITES) {		//dump(1): 1 is siteNo
              if(siteInfo[opn->siteNo].flag_siteAvailable == 0) {	//We do not send operations to failed sites
               printf("startTransactionManager: received dump operation for failed site %d\n", opn->siteNo ) ;
              }
              else {
                performOperation(opn, opn->siteNo) ;
                int operationStatus = opn->operationStatusAtSites[opn->siteNo] ;
#ifdef _DEBUG_
                if(operationStatus == OPERATION_REJECTED)
                 printf("Dump operation rejected @ site %d\n", opn->siteNo) ;
                if(operationStatus == OPERATION_BLOCKED)
                 printf("Dump operation @ site %d\n", opn->siteNo) ;
#endif
                if(operationStatus != OPERATION_COMPLETE) {
#ifdef _DEBUG_
                  printf("startTransactionManager: site %d did not set dump operation state to complete\n", opn->siteNo ) ;
#endif
                  opn->operationStatusAtSites[opn->siteNo] = OPERATION_COMPLETE ;
                  int operationStatus = opn->operationStatusAtSites[opn->siteNo] ;
#ifdef _DEBUG_
                  if(operationStatus == OPERATION_REJECTED)
                   printf("Dump operation rejected @ site %d\n", opn->siteNo) ;
                  if(operationStatus == OPERATION_BLOCKED)
                   printf("Dump operation @ site %d\n", opn->siteNo) ;
#endif
                  if(operationStatus != OPERATION_COMPLETE) {
#ifdef _DEBUG_
                    printf("startTransactionManager: site %d did not set dump operation state to complete\n", opn->siteNo ) ;
#endif
                    opn->operationStatusAtSites[opn->siteNo] = OPERATION_COMPLETE ;
                  }
                }
              }
            }
          }
          else if(opn->varNo != ALL_VARIABLES) {	//dump(x1) Operation is to be performed for a specific variables on the sites storing it
            int siteNo ;
            if(opn->varNo % 2 == 1) {	//Variable is odd and its not replicated
              siteNo = (opn->varNo % 10) + 1 ;
              if(siteInfo[siteNo].flag_siteAvailable == 0 ) {
                printf("startTransactionManager: dump operation on varNo %d cannot be performed @ site %d since the site has failed\n", opn->varNo, siteNo ) ;
              }
              else {
               performOperation(opn, siteNo) ;
               int operationStatus = opn->operationStatusAtSites[siteNo] ;
#ifdef _DEBUG_
               if(operationStatus == OPERATION_REJECTED)
                printf("Dump operation rejected @ site %d\n", siteNo) ;
               if(operationStatus == OPERATION_BLOCKED)
                printf("Dump operation @ site %d\n", siteNo) ;
#endif
               if(operationStatus != OPERATION_COMPLETE) {
#ifdef _DEBUG_
                 printf("startTransactionManager: site %d did not set dump operation state to complete\n", siteNo ) ;
#endif
                 opn->operationStatusAtSites[siteNo] = OPERATION_COMPLETE ;
               }
              }
            }
            else {	//Variable number is even and it's replicated
              int siteNo ;
              for(siteNo = 1 ; siteNo < MAX_SITES ; siteNo++) {
                if(siteInfo[siteNo].flag_siteAvailable == 0)	//We do not send operations to failed sites
                  continue ;
                performOperation(opn, siteNo) ;		//Send dump operation for the given site
                int operationStatus = opn->operationStatusAtSites[siteNo] ;
#ifdef _DEBUG_
                if(operationStatus == OPERATION_REJECTED)
                 printf("Dump operation rejected @ site %d\n", siteNo) ;
                if(operationStatus == OPERATION_BLOCKED)
                 printf("Dump operation blocked @ site %d\n", siteNo) ;
#endif
                if(operationStatus != OPERATION_COMPLETE) {
#ifdef _DEBUG_
                  printf("startTransactionManager: site %d did not set dump operation state to complete\n", siteNo ) ;
#endif
                  opn->operationStatusAtSites[siteNo] = OPERATION_COMPLETE ;
                }
              }
            }
          }
        }
        else if(opn->operationType == QUERY_STATE_OPERATION ) {
	  int siteNo ;
	  for(siteNo = 1 ; siteNo < MAX_SITES ; siteNo++) {
	    if(siteInfo[siteNo].flag_siteAvailable == 0)    //We do not send operations to failed sites
	      continue ;
	    performOperation(opn, siteNo) ;         //Send dump operation for the given site
	    int operationStatus = opn->operationStatusAtSites[siteNo] ;
#ifdef _DEBUG_
	    if(operationStatus == OPERATION_REJECTED)
	    {
	        sprintf(log_desc,"Query state operation rejected @ site %d\n", siteNo) ;
		logString(log_desc) ;
	    }
	    if(operationStatus == OPERATION_BLOCKED)
	    {   sprintf(log_desc,"Query state operation blocked @ site %d\n", siteNo) ;
		logString(log_desc) ;
	    }
#endif
	    if(operationStatus != OPERATION_COMPLETE) {
#ifdef _DEBUG_
	      sprintf(log_desc,"startTransactionManager: site %d did not set querystate operation state to complete\n", siteNo ) ;
	      logString(log_desc) ;
#endif
	      opn->operationStatusAtSites[siteNo] = OPERATION_COMPLETE ;
	    }
          }
	  int transactionID ;
	  for(transactionID = 0; transactionID < MAX_TRANSACTIONS ; transactionID++ ) {
	    if(transactionID == MISCELLANEOUS_TID || T[transactionID].timestamp > tickNo || T[transactionID].timestamp == -1) {
	      continue ;
	    }
	    if(T[transactionID].current_opn == NULL) {	//The transaction has completed
	      if(T[transactionID].transactionCompletionStatus == TRANSACTION_COMMITED) {
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d COMMITED\n", transactionID) ;
                logString(log_desc) ;
	      }
	      else {
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d ABORTED\n", transactionID) ;
                logString(log_desc) ;
	      }
	    }
	    else if(T[transactionID].current_opn->operationTimestamp < tickNo){
	      if(T[transactionID].current_opn->operationType == READ_OPERATION) {
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d is waiting for operation read on varNo %d arrived at tick No. %d to be completed\n", transactionID, T[transactionID].current_opn->varNo, T[transactionID].current_opn->operationTimestamp) ;
                logString(log_desc) ;
	      }
	      else if(T[transactionID].current_opn->operationType == WRITE_OPERATION) { 
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d is waiting for operation write on varNo %d with value %d arrived at tick No. %d to be completed\n", transactionID, T[transactionID].current_opn->varNo, T[transactionID].current_opn->valueToWrite, T[transactionID].current_opn->operationTimestamp) ;
                logString(log_desc) ;
	      }
	    }
	    else if(T[transactionID].current_opn->operationTimestamp >= tickNo) {
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d is waiting for new operation to arrive from input Sequence\n", transactionID, T[transactionID].current_opn->varNo, T[transactionID].current_opn->valueToWrite, T[transactionID].current_opn->operationTimestamp) ;
                logString(log_desc) ;

	    }
	  }
        }
        else if(opn->operationType == FAIL_OPERATION || opn->operationType == RECOVER_OPERATION) {
          performOperation(opn, opn->siteNo) ;
          if(opn->operationType == FAIL_OPERATION ) {
	   sprintf(log_desc,"startTransactionManager: site %d has been failed\n", opn->siteNo ) ;
	   //logString(log_desc) ;
           siteInfo[opn->siteNo].flag_siteAvailable = 0 ;
	  } 
          else {
	   if(siteInfo[opn->siteNo].flag_siteAvailable == 0) { 
             siteInfo[opn->siteNo].flag_siteAvailable = 1 ;
	     sprintf(log_desc,"startTransactionManager: site %d has been recovered\n", opn->siteNo ) ;
	     //logString(log_desc) ;
             siteInfo[opn->siteNo].tick_upTime = tickNo ;		//Note the time @ which the site has recovered
	   }
           else {
             sprintf(log_desc,"startTransactionManager: site %d has been recovered. However we had not received a failure for this site\n", opn->siteNo ) ;
	     //logString(log_desc) ;
           }
          }
        }
        T[MISCELLANEOUS_TID].current_opn = T[MISCELLANEOUS_TID].current_opn->nextOperationTM ;
      } 
    }
    for(trnid = 0 ; trnid < MAX_TRANSACTIONS ; trnid++) {
      if(trnid  == MISCELLANEOUS_TID)
        continue ;
      if(T[trnid].current_opn != NULL ) {
	if(flagPending == 0) {
	  flagPending = 1 ;
	} 
	if(T[trnid].timestamp > tickNo) {		//The transaction hasn't arrived yet
	  continue ; 
	}
        if(T[trnid].current_opn->operationTimestamp <= tickNo) {		//Only consider the operation if it has already arrived or if its a previously committed operation
          struct operation *opn = T[trnid].current_opn ;
          if(opn->operationType == READ_OPERATION ) {
            if(opn->varNo % 2 == 1) {	//If operation is to be performed on an unreplicated variable
              int siteNo = (opn->varNo % 10) + 1 ;
              opn->siteNo = siteNo ;
              if(opn->operationStatusAtSites[siteNo] == OPERATION_PENDING ) {	//Which means the operation has not been sent to the site yet
                if(siteInfo[siteNo].flag_siteAvailable == 0 ) {		//If the site @ which operation was sent is now unavailable, and if there is no other site @ which operation can be performed, transaction is aborted
#ifdef ABORT_SITE_FAIL
                  sprintf(log_desc,"startTransactionManager: Transaction ID: %d ABORTED since read on var %d failed due to site %d failure\n", trnid, opn->varNo, opn->siteNo) ;
		  logString(log_desc);
                  T[trnid].current_opn = NULL ;			//Transaction cannot proceed further, hence it's aborted
#endif
#ifdef WAIT_SITE_FAIL
                  if(opn->operationWaitListed_tickNo == -1 ) {
                    opn->operationWaitListed_tickNo = tickNo ;
                  }
                  opn->operationStatusAtSites[siteNo] = OPERATION_PENDING ;
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked at read on var %d since site %d has temporarily failed. Retrying on next tick..\n", trnid, opn->varNo, siteNo) ;
                  logString(log_desc) ;
                  continue ;
#endif
                }
                else {
                  opn->siteNo = siteNo ;
                  opn->operationWaitListed_tickNo = -1 ;
                  performOperation(opn, siteNo) ;					//Send operation to be performed at the site
                  //Check if operation has been rejected/completed or blocked
                  if(opn->operationStatusAtSites[siteNo] == OPERATION_REJECTED ) {
                    sprintf(log_desc,"startTransactionManager: Transaction ID: %d ABORTED since read on var %d rejected by site %d\n", trnid, opn->varNo, opn->siteNo) ;
                    logString(log_desc) ;
                    abortTransaction(opn) ;
                    T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                    T[trnid].current_opn = NULL ;
                  }
                  else if(opn->operationStatusAtSites[siteNo] == OPERATION_BLOCKED) {
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked for read on var %d @ site %d since the site could not provide the lock\n", trnid, opn->varNo, opn->siteNo) ;
                    logString(log_desc) ;
                    //Do nothing
                  }
                  else if(opn->operationStatusAtSites[siteNo] == OPERATION_COMPLETE) {
                    //Operation has been completed
                    if(T[trnid].sites_accessed[siteNo].tick_firstAccessed == -1) {        //If this is the first time transaction transaction has accessed the site
                      T[trnid].sites_accessed[siteNo].tick_firstAccessed = tickNo ;
                    }
                    T[trnid].current_opn = T[trnid].current_opn->nextOperationTM ;
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d Read on var no. %d returns %d from site %d\n", trnid, opn->varNo, opn->valueRead, opn->siteNo) ;
                    logString(log_desc) ;
                  }
                }
              }
              else if(opn->operationStatusAtSites[siteNo] == OPERATION_BLOCKED) {	//If operation was previously blocked by the site
                if(siteInfo[siteNo].flag_siteAvailable == 0 ) {
#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction ID: %d ABORTED since read on var %d at site %d timed out\n", trnid, opn->varNo, opn->siteNo ) ;
                  abortTransaction(opn) ;
                  T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                  T[trnid].current_opn = NULL ;
#endif
#ifdef WAIT_SITE_FAIL
		  if(opn->operationWaitListed_tickNo == -1 ) {
		    opn->operationWaitListed_tickNo = tickNo ;    //Record the tickNo at which the operation was blocked due to site failure
		  }
		  sprintf(log_desc,"startTransactionManager: Transaction ID: %d has blocked at read on var %d at since site %d has temporarily failed\n", trnid, opn->varNo, opn->siteNo ) ;
                  logString(log_desc) ;
		  opn->operationStatusAtSites[siteNo] = OPERATION_PENDING ;
		} 
#endif
              }
              else if(opn->operationStatusAtSites[siteNo] == OPERATION_COMPLETE) {
                if(T[trnid].sites_accessed[siteNo].tick_firstAccessed == -1) {        //If this is the first time transaction transaction has accessed the site
                  T[trnid].sites_accessed[siteNo].tick_firstAccessed = tickNo ;
                }
                T[trnid].current_opn = T[trnid].current_opn->nextOperationTM ;
                sprintf(log_desc, "startTransactionManager: Transaction ID: %d Read on var no. %d returns %d from site %d\n", trnid, opn->varNo, opn->valueRead, opn->siteNo) ;
                logString(log_desc) ;
              }
            }
            else {	//Variable number is even and it's replicated on all sites
              RETRY_READ:
              if(opn->siteNo == -1) {
                opn->siteNo = 1 ;	//Begin to try reading starting from the 1st site
              }
              if(siteInfo[opn->siteNo].flag_siteAvailable == 0) {	//If the site has failed
                while(siteInfo[opn->siteNo].flag_siteAvailable == 0 && opn->siteNo < MAX_SITES) {
                  opn->siteNo++ ;
                }
                if(opn->siteNo == MAX_SITES) {	//We have tried all the sites and we could not get the read done
#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction ID: %d ABORTED since read on var %d could not be completed at any of the sites\n", trnid, opn->varNo) ;
                  abortTransaction(opn) ;
                  T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                  T[trnid].current_opn = NULL ;
                  continue ;
#endif
#ifdef WAIT_SITE_FAIL
                  if(opn->operationWaitListed_tickNo == -1) {
                    opn->operationWaitListed_tickNo = tickNo ;
                  }
                  int i = 1 ;
                  for(i = 1; i < MAX_SITES; i++) {		//We will retry this operation again at all sites
                    opn->operationStatusAtSites[i] = OPERATION_PENDING ;
                  }
                  opn->siteNo = 1 ;
                  sprintf(log_desc,"startTransactionManager: Transaction ID: %d blocked at read on var %d since all sites have failed. Retrying on the sites at the next tick..\n", trnid, opn->varNo) ;
                  logString(log_desc) ;
                  continue ;
#endif
                }
              }
              if(opn->operationStatusAtSites[opn->siteNo] == OPERATION_PENDING) {
                opn->operationWaitListed_tickNo = -1 ;
                performOperation(opn, opn->siteNo) ;                                       //Send operation to be performed at the site
                //Check if operation has been rejected/completed or blocked
                if(opn->operationStatusAtSites[opn->siteNo] == OPERATION_REJECTED ) {
                  //sprintf(log_desc, "startTransactionManager: Transaction ID: %d read on varNo %d rejected @ site %d. Retrying next available site\n", trnid, opn->varNo, opn->siteNo) ;
                  //logString(log_desc) ;
                  opn->siteNo++ ;
                  if(opn->siteNo == MAX_SITES) {        //We have tried all the sites and we could not get the read done
                    abortTransaction(opn) ;
                    T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d ABORTED since read on varNo %d since the operation could not be completed at any site\n", trnid, opn->varNo) ;
		    logString(log_desc);	
                    T[trnid].current_opn = NULL ;
                    continue ;
                  }
                  else {
                    goto RETRY_READ ;
                  }
                }
                else if(opn->operationStatusAtSites[opn->siteNo] == OPERATION_BLOCKED) {
                  //Do nothing
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked for read on var %d @ site %d since site could not provide the lock\n", trnid, opn->varNo, opn->siteNo) ;
                  logString(log_desc) ;
                }
                else if(opn->operationStatusAtSites[opn->siteNo] == OPERATION_COMPLETE) {
                  //Operation has been completed
                  T[trnid].current_opn = T[trnid].current_opn->nextOperationTM ;
                  if(T[trnid].sites_accessed[opn->siteNo].tick_firstAccessed == -1) {        //If this is the first time transaction transaction has accessed the site
                    T[trnid].sites_accessed[opn->siteNo].tick_firstAccessed = tickNo ;
                  }
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d Read on var no. %d returns %d from site %d\n", trnid, opn->varNo, opn->valueRead, opn->siteNo) ;
                  logString(log_desc) ;
                }
              }
              else if(opn->operationStatusAtSites[opn->siteNo] == OPERATION_BLOCKED) {       //If operation was previously blocked by the site
                if(siteInfo[opn->siteNo].flag_siteAvailable == 0 ) {  //If operation has timed out
                  opn->siteNo++ ;
                  if(opn->siteNo == MAX_SITES) {	//We have tried all the sites and we could not get the read done
#ifdef ABORT_SITE_FAIL
                    printf("startTransactionManager: Transaction ID: %d ABORTED since read on var %d could not be completed at any of the sites\n", trnid, opn->varNo) ;
                    abortTransaction(opn) ;
                    T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                    T[trnid].current_opn = NULL ;
                    continue ;
#endif
#ifdef WAIT_SITE_FAIL
                    if(opn->operationWaitListed_tickNo == -1) {
                      opn->operationWaitListed_tickNo = tickNo ;
                    }
                    int i = 1 ;
                    for(i = 1; i < MAX_SITES; i++) {		//We will retry this operation again at all sites
                      opn->operationStatusAtSites[i] = OPERATION_PENDING ;
                    }
                    opn->siteNo = 1 ;
                    sprintf(log_desc,"startTransactionManager: Transaction ID: %d blocked at read on var %d since all sites have failed. Retrying on the sites at the next tick..\n", trnid, opn->varNo) ;
                    logString(log_desc) ;
                    continue ;
#endif
                  }
		  else { 
                    sprintf(log_desc, "startTransactionManager: Transaction %d read @ site %d for varNo %d failed due to site failure, Retrying on next available site\n", trnid, opn->siteNo-1, opn->varNo) ;
                    logString(log_desc) ;
                  }
                }
                else {
                  sprintf(log_desc, "startTransactionManager: Transaction %d waiting for read @ site %d for varNo %d to be complete\n", trnid, opn->siteNo, opn->varNo) ;
                  //logString(log_desc) ;
                  printf("%s", log_desc) ;
                }

              }
              else if(opn->operationStatusAtSites[opn->siteNo] == OPERATION_COMPLETE) {
                if(T[trnid].sites_accessed[opn->siteNo].tick_firstAccessed == -1) {        //If this is the first time transaction transaction has accessed the site
                  T[trnid].sites_accessed[opn->siteNo].tick_firstAccessed = tickNo ;
                }
                sprintf(log_desc, "startTransactionManager: Transaction ID: %d Read on var no. %d returns %d from site %d\n", trnid, opn->varNo, opn->valueRead, opn->siteNo) ;
                logString(log_desc) ;
                T[trnid].current_opn = T[trnid].current_opn->nextOperationTM ;
              }
            }
          }

          else if(opn->operationType == WRITE_OPERATION ) {
            if(opn->varNo % 2 == 1) {	//If operation is to be performed on an unreplicated variable
              int siteNo = (opn->varNo % 10) + 1 ;
              opn->siteNo = siteNo ;
              if(opn->operationStatusAtSites[siteNo] == OPERATION_PENDING ) {	//Which means the operation has not been sent to the site yet
                if(siteInfo[siteNo].flag_siteAvailable == 0 ) {		//If the site @ which operation was sent is now unavailable, and if there is no other site at which operation can be performed, transaction is aborted

#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction ID: %d ABORTED since write on var %d with value %d failed due to site %d failure\n", trnid, opn->varNo, opn->valueToWrite, opn->siteNo) ;
                  T[trnid].current_opn = NULL ;			//Transaction cannot proceed further, hence it's aborted
#endif
#ifdef WAIT_SITE_FAIL
                  if(opn->operationWaitListed_tickNo == -1 ) {
                    opn->operationWaitListed_tickNo = tickNo ;
                  }
                  opn->operationStatusAtSites[siteNo] = OPERATION_PENDING ;
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked at write on var %d with value %d since site %d has temporarily failed\n", trnid, opn->varNo, opn->valueToWrite, opn->siteNo) ;
                  logString(log_desc) ;
#endif
                }
                else {
                  opn->siteNo = siteNo ;
                  opn->operationWaitListed_tickNo = -1 ;
                  performOperation(opn, siteNo) ;					//Send operation to be performed at the site
                  //Check if operation has been rejected/completed or blocked
                  if(opn->operationStatusAtSites[siteNo] == OPERATION_REJECTED ) {
                    sprintf(log_desc, "startTransactionManager: Transaction %d ABORTED since write on varNo %d with value %d rejected by site %d\n", trnid, opn->varNo, opn->valueToWrite, siteNo) ;
                    logString(log_desc) ;
                    abortTransaction(opn) ;
                    T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                    T[trnid].current_opn = NULL ;
                  }
                  else if(opn->operationStatusAtSites[siteNo] == OPERATION_BLOCKED) {
                    sprintf(log_desc,"startTransactionManager: Transaction ID: %d blocked for write on varNo %d with value %d @ site %d since site could not provide it with the lock\n", trnid, opn->varNo, opn->valueToWrite, opn->siteNo) ;
                    logString(log_desc) ;
                    //Do nothing
                  }
                  else if(opn->operationStatusAtSites[siteNo] == OPERATION_COMPLETE) {
                    //Operation has been completed
                   sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on var %d with value %d completed\n", trnid, opn->varNo, opn->valueToWrite, opn->siteNo) ;
                   logString(log_desc) ;
                    T[trnid].current_opn = T[trnid].current_opn->nextOperationTM ;
                    if(T[trnid].sites_accessed[siteNo].tick_firstAccessed == -1) {        //If this is the first time transaction transaction has accessed the site
                      T[trnid].sites_accessed[siteNo].tick_firstAccessed = tickNo ;
                    }
                    if(T[trnid].sites_accessed[siteNo].flagWriteAccessed == 0) {
                      T[trnid].sites_accessed[siteNo].flagWriteAccessed = 1 ;       //Set a flag indicating transaction has accessed the site for write operation
                    }
                  }
                }
              }
              else if(opn->operationStatusAtSites[siteNo] == OPERATION_BLOCKED) {	//If operation was previously blocked by the site
                if(siteInfo[siteNo].flag_siteAvailable == 0 ) {
#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction %d ABORTED since write on varNo %d with value to be written %d at site %d timedout\n", trnid, opn->varNo, opn->valueToWrite, opn->siteNo ) ;
                  abortTransaction(opn) ;
                  T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                  T[trnid].current_opn = NULL ;
#endif
#ifdef WAIT_SITE_FAIL
		  if(opn->operationWaitListed_tickNo == -1 ) {
		    opn->operationWaitListed_tickNo = tickNo ;    //Record the tickNo at which the operation was blocked due to site failure
		  }
		  sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked at write on var %d with value %d since site %d has temporarily failed\n", trnid, opn->varNo, opn->valueToWrite, opn->siteNo ) ;
                  logString(log_desc) ;
		  opn->operationStatusAtSites[siteNo] = OPERATION_PENDING ;
#endif
                }
                else {
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d still waiting for write on var %d with value %d blocked @ site %d\n", trnid, opn->varNo, opn->valueToWrite, opn->siteNo) ;
                    //logString(log_desc) ;
                    printf("%s", log_desc) ;
                }
              }
              else if(opn->operationStatusAtSites[siteNo] == OPERATION_COMPLETE) {
                sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on var %d with value %d completed\n", trnid, opn->varNo, opn->valueToWrite, opn->siteNo) ;
                logString(log_desc) ;
                T[trnid].current_opn = T[trnid].current_opn->nextOperationTM ;
              }
            }
            else {
              int siteNo, flag_writeStatus = WRITE_CHECK_FOR_COMPLETE ;
              int flag_writePerformed = 0 ;
              for(siteNo = 1; siteNo < MAX_SITES ; siteNo++ ) {
                if(opn->operationStatusAtSites[siteNo] == OPERATION_IGNORE || opn->operationStatusAtSites[siteNo] == OPERATION_COMPLETE) {	//We choose to ignore this site since it has/had failed during write
                  continue ;
                }
                else if(opn->operationStatusAtSites[siteNo] == OPERATION_PENDING) {
                  if(siteInfo[siteNo].flag_siteAvailable == 0) {
                    opn->operationStatusAtSites[siteNo] = OPERATION_IGNORE ;	//We will be igoring this site since it was not available at the time of writing
                    continue ;
                  }
                  else {	//site is available, perform the operation on that site
                    opn->operationWaitListed_tickNo = -1 ;
                    performOperation(opn, siteNo) ;                                       //Send operation to be performed at the site
                    //Check if operation has been rejected/completed or blocked
                    if(opn->operationStatusAtSites[siteNo] == OPERATION_REJECTED ) {
                      sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on varNo %d with value %d rejected by site %d\n", trnid, opn->varNo, opn->valueToWrite, siteNo) ;
                      logString(log_desc) ;
                      flag_writeStatus = WRITE_FAILED ;
                      break ;
                    }
                    else if(opn->operationStatusAtSites[siteNo] == OPERATION_BLOCKED) {
                      //Do nothing
                      sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked for write on var %d with value %d @ site %d since site could not provide it with the lock\n", trnid, opn->varNo, opn->valueToWrite, siteNo) ;
                      logString(log_desc) ;
                      printf("%s", log_desc) ;
                      if(flag_writeStatus != WRITE_PENDING) {
                        flag_writeStatus = WRITE_PENDING ;		//Set a flag indicating write is pending at one of the sites
                      }
                    }
                    else if(opn->operationStatusAtSites[siteNo] == OPERATION_COMPLETE) {
                      //Operation has been completed at one of the sites
                      if(flag_writePerformed == 0){
                        flag_writePerformed = 1 ;	//Indicate we have successfully written at atleast one of the sites
                      }
                      if(T[trnid].sites_accessed[siteNo].tick_firstAccessed == -1) {        //If this is the first time transaction transaction has accessed the site
                        T[trnid].sites_accessed[siteNo].tick_firstAccessed = tickNo ;
                      }
                      if(T[trnid].sites_accessed[siteNo].flagWriteAccessed == 0) {
                        T[trnid].sites_accessed[siteNo].flagWriteAccessed = 1 ;       //Set a flag indicating transaction has accessed the site for write operation
                      }
                      sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on var %d with value %d @ site %d completed\n", trnid, opn->varNo, opn->valueToWrite, siteNo) ;
                      logString(log_desc) ;
                    }
                  }
                }
                else if(opn->operationStatusAtSites[siteNo] == OPERATION_BLOCKED) {
                  if(siteInfo[siteNo].flag_siteAvailable == 0 ) {  //If site on which write was sent has now failed
                    opn->operationStatusAtSites[siteNo] = OPERATION_IGNORE ;
                  }
                  else {
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d still waiting for write on var %d with value %d @ site %d\n", trnid, opn->varNo, opn->valueToWrite, siteNo) ;
                    //logString(log_desc) ;
                    printf("%s", log_desc) ;
                    if(flag_writeStatus != WRITE_PENDING) {
                      flag_writeStatus = WRITE_PENDING ;         //Set a flag indicating write is pending at one of the sites
                    }
                  }
                }
                else if(opn->operationStatusAtSites[siteNo] == OPERATION_COMPLETE) {
		  if(T[trnid].sites_accessed[siteNo].tick_firstAccessed == -1) {        //If this is the first time transaction transaction has accessed the site
		    T[trnid].sites_accessed[siteNo].tick_firstAccessed = tickNo ;
		  }
		  if(T[trnid].sites_accessed[siteNo].flagWriteAccessed == 0) {
		    T[trnid].sites_accessed[siteNo].flagWriteAccessed = 1 ;       //Set a flag indicating transaction has accessed the site for write operation
		  } 
                  if(flag_writePerformed == 0){
		    flag_writePerformed = 1 ;	//Indicate we have successfully written at atleast one of the sites
		  }
                  sprintf(log_desc,"startTransactionManager: Transaction ID: %d write on var %d with value %d @ site %d completed\n", trnid, opn->varNo, opn->valueToWrite, siteNo) ;
                  logString(log_desc) ;
                }
              }
              if(flag_writeStatus == WRITE_CHECK_FOR_COMPLETE) {	//Either write happened to all sites or all sites have failed
		//Check if we have performed atleast one write successfully
                if(flag_writePerformed == 1) { 
		  T[trnid].current_opn = T[trnid].current_opn->nextOperationTM ;
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on varNo %d with value %d completed at all available sites\n", trnid, opn->varNo, opn->valueToWrite) ;
                  logString(log_desc) ;
                }
                else {
#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction ID: %d ABORTED since write on var %d with value %d could not be completed at any of the sites\n", trnid, opn->varNoopn->valueToWrite) ;
                  abortTransaction(opn) ;
                  T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                  T[trnid].current_opn = NULL ;
#endif
#ifdef WAIT_SITE_FAIL
                  if(opn->operationWaitListed_tickNo == -1) {
                    opn->operationWaitListed_tickNo = tickNo ;
                  }
                  int i = 1 ;
                  for(i = 1; i < MAX_SITES; i++) {		//We will retry this operation again at all sites
                    opn->operationStatusAtSites[i] = OPERATION_PENDING ;
                  }
                  sprintf(log_desc ,"startTransactionManager: Transaction ID: %d blocked at write on var %d with value %d since all sites have failed. Retrying on the sites at the next tick..\n", trnid, opn->varNo, opn->valueToWrite) ;
                  logString(log_desc) ;
#endif
                }
	      } 
              else if(flag_writeStatus == WRITE_FAILED) {
                sprintf(log_desc, "startTransactionManager: Transaction ID: %d ABORTED since write on varNo %d with value to be written %d rejected by site %d\n", trnid, opn->varNo, opn->valueToWrite, siteNo) ;
                logString(log_desc) ;
                abortTransaction(opn) ;
                T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                T[trnid].current_opn = NULL ;
              }
            }
          }

          else if(opn->operationType == END_OPERATION ) {
            int siteNo, flagCommit = 1 ;
	    for(siteNo = 1; siteNo < MAX_SITES && flagCommit == 1 && T[trnid].transactionType != READONLY_TRANSACTION; siteNo++) {
	      if(T[trnid].sites_accessed[siteNo].tick_firstAccessed != -1) {	//If the transaction has accessed this site
		if(siteInfo[siteNo].flag_siteAvailable == 0 ) {
		  flagCommit = 0 ;
		  sprintf(log_desc, "startTransactionManager: Transaction %d could not commit and has been ABORTED since site %d has failed\n", trnid, siteNo) ;
                  logString(log_desc) ;
		  abortTransaction(opn) ;
		  T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
		  T[trnid].current_opn = NULL ;
		}
		else if(siteInfo[siteNo].tick_upTime > T[trnid].sites_accessed[siteNo].tick_firstAccessed) {	//The site had failed after transaction accessed it for first time
                  flagCommit = 0 ;
                  sprintf(log_desc, "startTransactionManager: Transaction %d ABORTED since site %d had failed at some point after the transaction first accessed it\n", trnid, siteNo) ;
                  logString(log_desc) ;
                  abortTransaction(opn) ; 
		  T[trnid].transactionCompletionStatus = TRANSACTION_ABORTED ;
                  T[trnid].current_opn = NULL ;
		} 
	      }
	    }
	    if(flagCommit == 1) {
	      sprintf(log_desc, "startTransactionManager: Transaction %d committed @ tick %d\n", trnid, tickNo) ; 
              logString(log_desc) ;
	      for(siteNo = 1; siteNo < MAX_SITES; siteNo++) {
		if(T[trnid].sites_accessed[siteNo].tick_firstAccessed != -1 && siteInfo[siteNo].flag_siteAvailable == 1) {
		    performOperation(T[trnid].current_opn,siteNo); ///// CHECK HERE , when i try to print anything at site nothing gets printed for op
		} 
	      }
	      T[trnid].current_opn = NULL ; 
              T[trnid].transactionCompletionStatus = TRANSACTION_COMMITED ;
	    }
          }
        }
      }
      else if(T[trnid].current_opn == NULL && (T[trnid].transactionCompletionStatus == -1 && T[trnid].flag_transactionValid == 1)) {
	if(flagPending == 0) {
	  flagPending = 1 ;
        }
        if(T[trnid].inactiveTickNo == -1) {
          T[trnid].inactiveTickNo = tickNo ;
	  sprintf(log_desc, "startTransactionManager: Transaction %d is waiting for some operation to be received from the input sequence\n", trnid) ;
          logString(log_desc) ;
        }
        else {
	  sprintf(log_desc, "startTransactionManager: Transaction %d still WAITING for a new operation from the input for over %d ticks\n", trnid, tickNo - T[trnid].inactiveTickNo) ;
          logString(log_desc) ;
        }
      }
    }
    Sleep_ms(SLEEP_DURATION_MS) ; 
    tickNo++ ;
  }
}

void abortTransaction(struct operation *opn) {
  struct operation abort_opn ;
  int siteNo ;
  int trnid = opn->trnid ;
  memcpy(&abort_opn, opn, sizeof(struct operation)) ;		// CHECK HERE
  abort_opn.operationType = ABORT_OPERATION ;

  for(siteNo = 1; siteNo < MAX_SITES; siteNo++) {
    abort_opn.siteNo = siteNo ; 
    abort_opn.nextOperationSite = NULL ; 
    if(T[trnid].sites_accessed[siteNo].tick_firstAccessed != -1 && siteInfo[siteNo].flag_siteAvailable == 1) {
      performOperation(&abort_opn, siteNo) ;
    }
  }
  return ; 
}

/****************************************************************************************************************
						Function parseInput Starts								
*************************************************************************************************************************/
int parseInput(char *inputFile) {
  int ret ;


  
  int i ;


  T[MISCELLANEOUS_TID].transactionType = MISCELLANEOUS_TRANSACTION ;
  FILE *fp = fopen(inputFile, "r") ;
  if(fp == NULL) {
    printf("parseInput: fopen failed %s. Error %s\n", inputFile, (char *)strerror(errno)) ;
    return -1 ;
  }
  int timeStamp = 0 ;
  while(!feof(fp)) {
    char buff[100], buff1[100], *temp ;
    memset(buff, 0, 100) ;
    fscanf(fp, "%s", buff) ;
    if(buff[0] == '/' || buff[0] == '#') {		
      fgets(buff1, 100, fp) ;
      continue ;
    }
    if(strncmp(buff,"dump", strlen("dump")) == 0 || strncmp(buff,"begin", strlen("begin")) == 0 || strncmp(buff,"R", strlen("R")) == 0 || strncmp(buff,"W", strlen("W")) == 0 || strncmp(buff,"fail", strlen("fail")) == 0 || strncmp(buff,"recover", strlen("recover")) == 0 || strncmp(buff,"end", strlen("end")) == 0 || strncmp(buff, "querystate", strlen("querystate")) == 0) {
    ret = storeOperation(buff, timeStamp) ;
      if(ret == -1) {
        printf("parseInput: storeOperation returned error for operation %s\n", buff) ;
        return -1 ;
      }

      temp = strstr(buff, ";") ;	
      if(temp == NULL) {
        timeStamp ++ ;
      }
    }
  }











  return  0 ;
}

int storeOperation(char *operationString, int operationtimestamp) {
  int trnid = -1, ret ;
  char *temp ;
  struct operation *opn = (struct operation *)malloc(sizeof(struct operation )) ;
  if(opn == NULL) {
    printf("storeOperation: Fatal Error. malloc for struct operation failed. Error: %s\n", strerror(errno)) ;
    return -1 ;
  }
  if(strncmp(operationString,"beginRO", strlen("beginRO")) == 0) {	
    temp = operationString ;
    while(!isdigit(*temp))
     temp++ ;
    trnid = atoi(temp) ;

    int transactionType = READONLY_TRANSACTION ;
    ret = createNewTransaction(trnid, transactionType, operationtimestamp) ;
    if(ret == -1) {
      printf("storeOperation: createNewTransaction returns error for new transaction trnid %d\n", trnid) ;
      return -1 ;
    }
  }
  else if(strncmp(operationString,"begin", strlen("begin")) == 0) {	
    temp = operationString ;
    while(!isdigit(*temp))
     temp++ ;
    trnid = atoi(temp) ;
    //printf("New transaction %d\n", trnid) ;
    int transactionType = READWRITE_TRANSACTION ;
    ret = createNewTransaction(trnid, transactionType, operationtimestamp) ;
    if(ret == -1) {
      printf("storeOperation: createNewTransaction returns error for new transaction trnid %d\n", trnid) ;
      return -1 ;
    }
  }
  else if(strncmp(operationString,"end", strlen("end")) == 0) {	
    temp = operationString ;
    while(!isdigit(*temp))
     temp++ ;
    trnid = atoi(temp) ;
    int varNo = -1, valueToWrite = -1, siteNo = -1 ;
    ret = prepareOperationNode(trnid, END_OPERATION, varNo, valueToWrite, siteNo, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for end operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;


    
  }
  else if(strncmp(operationString,"dump", strlen("dump")) == 0) {
    int varNo = -1, siteNo ;
    trnid = MISCELLANEOUS_TID ;
    temp = strstr(operationString, "(") ;
    if(temp == NULL) {
     printf("storeOperation: Could not parse dump operation %s\n", operationString) ;
     return -1 ;
    }
    while(*temp != ')' && !isalpha(*temp) && !isdigit(*temp))
     temp++ ;


    if(*temp == ')') {		
     varNo = ALL_VARIABLES ;
     siteNo = ALL_SITES ;
    }

    else if(isdigit(*temp)) {		
     siteNo = atoi(temp) ;
     varNo = ALL_VARIABLES ;

     
    }
    else if(isalpha(*temp)) {
     while(!isdigit(*temp))
      temp++ ;
     varNo = atoi(temp) ;
     siteNo = ALL_SITES ;

     
    }
    int valueToWrite = -1 ;
    ret = prepareOperationNode(trnid, DUMP_OPERATION, varNo, valueToWrite, siteNo, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for dump operation\n") ;
      return -1 ;
    }

    addOperationToTransactionQueue(trnid, opn) ;
  }
  else if(strncmp(operationString,"fail", strlen("fail")) == 0) {
    trnid = MISCELLANEOUS_TID ;
    temp = strstr(operationString, "(") ;
    if(temp == NULL) {
     printf("storeOperation: Could not parse fail operation %s\n", operationString) ;
     return -1 ;
    }
    while(!isdigit(*temp))
     temp++ ;
    int siteNo = atoi(temp) ;
    int varNo = -1, valueToWrite = -1 ;
    ret = prepareOperationNode(trnid, FAIL_OPERATION, varNo, valueToWrite, siteNo, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for fail operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;
  }
  else if(strncmp(operationString,"recover", strlen("recover")) == 0) {
    trnid = MISCELLANEOUS_TID ;
    temp = strstr(operationString, "(") ;
    if(temp == NULL) {
     printf("storeOperation: Could not parse recover operation %s\n", operationString) ;
     return -1 ;
    }
    while(!isdigit(*temp))
     temp++ ;
    int siteNo = atoi(temp) ;

    
    int varNo = -1, valueToWrite = -1 ;
    ret = prepareOperationNode(trnid, RECOVER_OPERATION, varNo, valueToWrite, siteNo, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for recover operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;
  }
  else if(strncmp(operationString,"querystate", strlen("querystate")) == 0) {
    trnid = MISCELLANEOUS_TID ;
    int siteNo = ALL_SITES, varNo = ALL_VARIABLES, valueToWrite = -1 ;
    ret = prepareOperationNode(trnid, QUERY_STATE_OPERATION, varNo, valueToWrite, siteNo, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for new operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;
  }
  else if(strncmp(operationString,"R", strlen("R")) == 0) {	//We have got a read operation
    temp = operationString ;
    while(!isdigit(*temp))
     temp++ ;
    trnid = atoi(temp) ;
    while(isdigit(*temp))
     temp++ ;
    while(!isdigit(*temp))
     temp++ ;
    int varNo = atoi(temp) ;
    int valueToWrite = -1, siteNo = -1 ;
    ret = prepareOperationNode(trnid, READ_OPERATION, varNo, valueToWrite, siteNo, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for new operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;


   }
  else if(strncmp(operationString,"W", strlen("W")) == 0) {	
    temp = operationString ;
    while(!isdigit(*temp))
     temp++ ;
    trnid = atoi(temp) ;

    while(isdigit(*temp))
     temp++ ;
    while(!isdigit(*temp))
     temp++ ;
    int varNo = atoi(temp) ;

    while(isdigit(*temp))
     temp++ ;
    while(!isdigit(*temp) && *temp != '-')
     temp++ ;
    int valueToWrite = atoi(temp), siteNo = -1 ;
    ret = prepareOperationNode(trnid, WRITE_OPERATION, varNo, valueToWrite, siteNo, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for new operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;

    
  }
  return  0 ;
}

//A new transaction entry is created
int createNewTransaction(int trnid, int transactionType, int timestamp) {
  if(trnid >= MAX_TRANSACTIONS ) {	//Transaction Manager can't support transaction ids greater than MAX_TRANSACTIONS
    printf("createNewTransaction: Transaction %d exceeds Max limit %d\n", trnid, MAX_TRANSACTIONS) ;
    return -1 ;
  }
  if(T[trnid].timestamp != -1 ) {
    printf("createNewTransaction: Error duplication of trnid %d in input transaction sequence\n", trnid) ;
    return -1 ;
  }
  T[trnid].timestamp = timestamp ;
  T[trnid].transactionType = transactionType ;
  T[trnid].flag_transactionValid = 1 ;
  return 0 ;
}

void addOperationToTransactionQueue(int trnid, struct operation *opn) {
  if(T[trnid].first_opn == NULL) {	//This is the first operation assigned to that transaction
    T[trnid].first_opn = T[trnid].last_opn = T[trnid].current_opn = opn ;
  }
  else {
    T[trnid].last_opn->nextOperationTM = opn ;
    T[trnid].last_opn = opn ;
  }
  return  ;
}

int prepareOperationNode(int trnid, int operationType, int varNo, int valueToWrite, int siteNo, int operationtimestamp, struct operation *opn) {
  opn->trnid = trnid ;
  opn->operationType = operationType ;
  opn->operationTimestamp = operationtimestamp ;
  opn->nextOperationTM = NULL ;
  opn->varNo = varNo ;
  opn->valueToWrite = valueToWrite ;
  opn->siteNo = siteNo ;
  opn->operationWaitListed_tickNo = -1 ;
  int site_No ;
  for(site_No = 1 ; site_No < MAX_SITES; site_No++) {
    opn->operationStatusAtSites[site_No] = OPERATION_PENDING ;
  }
  
  if(T[trnid].timestamp == -1 && trnid == MISCELLANEOUS_TID) {
    T[trnid].timestamp = operationtimestamp ;
  }
  opn->transactionType = T[trnid].transactionType ;
  opn->transactionTimestamp = T[trnid].timestamp ;

  return 0 ;
}






void initializeTransactionManager() {
  //Initialize all transaction structures
  int trnid, siteNo ;
  for(trnid = 0; trnid < MAX_TRANSACTIONS; trnid++) {
    T[trnid].timestamp = -1 ;
    T[trnid].transactionType = -1 ;
    T[trnid].transactionCompletionStatus = -1 ;
    T[trnid].inactiveTickNo = -1 ;
    T[trnid].flag_transactionValid = 0 ;
    T[trnid].first_opn = T[trnid].last_opn = T[trnid].current_opn = NULL ;
    for(siteNo = 0; siteNo < MAX_SITES; siteNo++)  {
      T[trnid].sites_accessed[siteNo].tick_firstAccessed = -1 ;
      T[trnid].sites_accessed[siteNo].flagWriteAccessed = 0 ; 
    }
  }

  //Initialize variable-site mapping
  int varNo ;
  for(varNo = 1; varNo < MAX_VARIABLES ; varNo++)  {
    if(varNo % 2 == 1) {	//if the variable is odd, use the given data in problem statement to find the siteNo @ which it will be stored
      siteNo = (varNo % 10) + 1 ;
      siteInfo[siteNo].flag_varNo[varNo] = 1 ;
    }
    else {	//Even variables are present at all sites
      for(siteNo = 1; siteNo < MAX_SITES ; siteNo++ ) {
        siteInfo[siteNo].flag_varNo[varNo] = 1 ;
      }
    }
  }

  for(siteNo = 1; siteNo < MAX_SITES; siteNo++) {	//All sites are assumed to be up initially
    siteInfo[siteNo].flag_siteAvailable = 1 ;
    siteInfo[siteNo].tick_upTime = 0 ;		//All sites are assumed to be up from the 0th tick
  }
}


void Sleep_ms(int time_ms) {
  struct timeval tv ;
  tv.tv_sec = 0 ;
  tv.tv_usec = time_ms * 1000 ;
  select(0, NULL , NULL, NULL, &tv) ;
  return ;
}
