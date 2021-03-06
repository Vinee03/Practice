#include "transaction_manager_struct.h"
#include "globalvar.h"
#include "site.h"
#include <ctype.h>
#include <sys/select.h>



#define WAIT_SITE_FAIL

#define SLEEP_DURATION_MS 	200

int availableSites[MAXIMUM_SITES] ;
int storeOperation(char *operationString, int operationtimestamp) ;
int prepareOperationNode(int trnid, int opnType, int variablenumber, int writtenValue, int sitenumber, int operationtimestamp, struct operation *opn) ;
void addOperationToTransactionQueue(int trnid, struct operation *opn) ;
void getSites(int variablenumber, int trySites[]) ;
int createNewTransaction(int trnid, int trnType, int timestamp) ;
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


    while( T[OTHER_TRNID].current_operation != NULL ) {
      if(flagPending == 0)
	flagPending = 1 ;
      if(T[OTHER_TRNID].current_operation->opnTimestamp > tickNo) {
        break ;
      }
      if(T[OTHER_TRNID].timestamp <= tickNo && T[OTHER_TRNID].current_operation->opnTimestamp <= tickNo) {
        struct operation *opn = T[OTHER_TRNID].current_operation ;
        if(opn->opnType == DUMP_OPN)
	{	//Dump could be on a single site or on all sites containing a specific variable or on all sites for all variables
          if(opn->variablenumber == ALL_VARIABLES) {
            if(opn->sitenumber == ALL_SITES) {	//dump(): The dump is to be performed for all variables on all sites
              int sitenumber ;
              for(sitenumber = 1 ; sitenumber < MAXIMUM_SITES ; sitenumber++) {
                if(siteInfo[sitenumber].flag_site_available == 0)	//We do not send operations to failed sites
                  continue ;
                performOperation(opn, sitenumber) ;		//Send dump operation for the given site
                int operationStatus = opn->operationStatusAtSites[sitenumber] ;
#ifdef _DEBUG_
                if(operationStatus == OPN_REJECTED)
                 printf("Dump operation rejected @ site %d\n", sitenumber) ;
                if(operationStatus == OPN_BLOCKED)
                 printf("Dump operation @ site %d\n", sitenumber) ;
#endif
                if(operationStatus != OPN_COMPLETE) {
#ifdef _DEBUG_
                  printf("startTransactionManager: site %d did not set dump operation state to complete\n", sitenumber ) ;
#endif
                  opn->operationStatusAtSites[sitenumber] = OPN_COMPLETE ;
                }
              }
            }
            else if(opn->sitenumber != ALL_SITES) {		//dump(1): 1 is sitenumber
              if(siteInfo[opn->sitenumber].flag_site_available == 0) {	//We do not send operations to failed sites
               printf("startTransactionManager: received dump operation for failed site %d\n", opn->sitenumber ) ;
              }
              else {
                performOperation(opn, opn->sitenumber) ;
                int operationStatus = opn->operationStatusAtSites[opn->sitenumber] ;
#ifdef _DEBUG_
                if(operationStatus == OPN_REJECTED)
                 printf("Dump operation rejected @ site %d\n", opn->sitenumber) ;
                if(operationStatus == OPN_BLOCKED)
                 printf("Dump operation @ site %d\n", opn->sitenumber) ;
#endif
                if(operationStatus != OPN_COMPLETE) {
#ifdef _DEBUG_
                  printf("startTransactionManager: site %d did not set dump operation state to complete\n", opn->sitenumber ) ;
#endif
                  opn->operationStatusAtSites[opn->sitenumber] = OPN_COMPLETE ;
                  int operationStatus = opn->operationStatusAtSites[opn->sitenumber] ;
#ifdef _DEBUG_
                  if(operationStatus == OPN_REJECTED)
                   printf("Dump operation rejected @ site %d\n", opn->sitenumber) ;
                  if(operationStatus == OPN_BLOCKED)
                   printf("Dump operation @ site %d\n", opn->sitenumber) ;
#endif
                  if(operationStatus != OPN_COMPLETE) {
#ifdef _DEBUG_
                    printf("startTransactionManager: site %d did not set dump operation state to complete\n", opn->sitenumber ) ;
#endif
                    opn->operationStatusAtSites[opn->sitenumber] = OPN_COMPLETE ;
                  }
                }
              }
            }
          }
          else if(opn->variablenumber != ALL_VARIABLES) {	//dump(x1) Operation is to be performed for a specific variables on the sites storing it
            int sitenumber ;
            if(opn->variablenumber % 2 == 1) {	//Variable is odd and its not replicated
              sitenumber = (opn->variablenumber % 10) + 1 ;
              if(siteInfo[sitenumber].flag_site_available == 0 ) {
                printf("startTransactionManager: dump operation on variablenumber %d cannot be performed @ site %d since the site has failed\n", opn->variablenumber, sitenumber ) ;
              }
              else {
               performOperation(opn, sitenumber) ;
               int operationStatus = opn->operationStatusAtSites[sitenumber] ;
#ifdef _DEBUG_
               if(operationStatus == OPN_REJECTED)
                printf("Dump operation rejected @ site %d\n", sitenumber) ;
               if(operationStatus == OPN_BLOCKED)
                printf("Dump operation @ site %d\n", sitenumber) ;
#endif
               if(operationStatus != OPN_COMPLETE) {
#ifdef _DEBUG_
                 printf("startTransactionManager: site %d did not set dump operation state to complete\n", sitenumber ) ;
#endif
                 opn->operationStatusAtSites[sitenumber] = OPN_COMPLETE ;
               }
              }
            }
            else {	//Variable number is even and it's replicated
              int sitenumber ;
              for(sitenumber = 1 ; sitenumber < MAXIMUM_SITES ; sitenumber++) {
                if(siteInfo[sitenumber].flag_site_available == 0)	//We do not send operations to failed sites
                  continue ;
                performOperation(opn, sitenumber) ;		//Send dump operation for the given site
                int operationStatus = opn->operationStatusAtSites[sitenumber] ;
#ifdef _DEBUG_
                if(operationStatus == OPN_REJECTED)
                 printf("Dump operation rejected @ site %d\n", sitenumber) ;
                if(operationStatus == OPN_BLOCKED)
                 printf("Dump operation blocked @ site %d\n", sitenumber) ;
#endif
                if(operationStatus != OPN_COMPLETE) {
#ifdef _DEBUG_
                  printf("startTransactionManager: site %d did not set dump operation state to complete\n", sitenumber ) ;
#endif
                  opn->operationStatusAtSites[sitenumber] = OPN_COMPLETE ;
                }
              }
            }
          }
        }
        else if(opn->opnType == QUERY_STATE_OPN ) {
	  int sitenumber ;
	  for(sitenumber = 1 ; sitenumber < MAXIMUM_SITES ; sitenumber++) {
	    if(siteInfo[sitenumber].flag_site_available == 0)    //We do not send operations to failed sites
	      continue ;
	    performOperation(opn, sitenumber) ;         //Send dump operation for the given site
	    int operationStatus = opn->operationStatusAtSites[sitenumber] ;
#ifdef _DEBUG_
	    if(operationStatus == ON_REJECTED)
	    {
	        sprintf(log_desc,"Query state operation rejected @ site %d\n", sitenumber) ;
		logString(log_desc) ;
	    }
	    if(operationStatus == ON_BLOCKED)
	    {   sprintf(log_desc,"Query state operation blocked @ site %d\n", sitenumber) ;
		logString(log_desc) ;
	    }
#endif
	    if(operationStatus != OPN_COMPLETE) {
#ifdef _DEBUG_
	      sprintf(log_desc,"startTransactionManager: site %d did not set querystate operation state to complete\n", sitenumber ) ;
	      logString(log_desc) ;
#endif
	      opn->operationStatusAtSites[sitenumber] = OPN_COMPLETE ;
	    }
          }
	  int transactionID ;
	  for(transactionID = 0; transactionID < MAXIMUM_TRANSACTIONS ; transactionID++ ) {
	    if(transactionID == OTHER_TRNID || T[transactionID].timestamp > tickNo || T[transactionID].timestamp == -1) {
	      continue ;
	    }
	    if(T[transactionID].current_operation == NULL) {	//The transaction has completed
	      if(T[transactionID].trn_Completion_Status == TRN_COMMITED) {
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d COMMITED\n", transactionID) ;
                logString(log_desc) ;
	      }
	      else {
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d ABORTED\n", transactionID) ;
                logString(log_desc) ;
	      }
	    }
	    else if(T[transactionID].current_operation->opnTimestamp < tickNo){
	      if(T[transactionID].current_operation->opnType == READ_OPN) {
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d is waiting for operation read on variablenumber %d arrived at tick No. %d to be completed\n", transactionID, T[transactionID].current_operation->variablenumber, T[transactionID].current_operation->opnTimestamp) ;
                logString(log_desc) ;
	      }
	      else if(T[transactionID].current_operation->opnType == WRITE_OPN) { 
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d is waiting for operation write on variablenumber %d with value %d arrived at tick No. %d to be completed\n", transactionID, T[transactionID].current_operation->variablenumber, T[transactionID].current_operation->writtenValue, T[transactionID].current_operation->opnTimestamp) ;
                logString(log_desc) ;
	      }
	    }
	    else if(T[transactionID].current_operation->opnTimestamp >= tickNo) {
		sprintf(log_desc, "startTransactionManager: querystate- Transaction ID: %d is waiting for new operation to arrive from input Sequence\n", transactionID, T[transactionID].current_operation->variablenumber, T[transactionID].current_operation->writtenValue, T[transactionID].current_operation->opnTimestamp) ;
                logString(log_desc) ;

	    }
	  }
        }
        else if(opn->opnType == FAIL_OPN || opn->opnType == RECOVER_OPN) {
          performOperation(opn, opn->sitenumber) ;
          if(opn->opnType == FAIL_OPN ) {
	   sprintf(log_desc,"startTransactionManager: site %d has been failed\n", opn->sitenumber ) ;
	   //logString(log_desc) ;
           siteInfo[opn->sitenumber].flag_site_available = 0 ;
	  } 
          else {
	   if(siteInfo[opn->sitenumber].flag_site_available == 0) { 
             siteInfo[opn->sitenumber].flag_site_available = 1 ;
	     sprintf(log_desc,"startTransactionManager: site %d has been recovered\n", opn->sitenumber ) ;
	     //logString(log_desc) ;
             siteInfo[opn->sitenumber].tick_time = tickNo ;		//Note the time @ which the site has recovered
	   }
           else {
             sprintf(log_desc,"startTransactionManager: site %d has been recovered. However we had not received a failure for this site\n", opn->sitenumber ) ;
	     //logString(log_desc) ;
           }
          }
        }
        T[OTHER_TRNID].current_operation = T[OTHER_TRNID].current_operation->opnTM ;
      } 
    }
    for(trnid = 0 ; trnid < MAXIMUM_TRANSACTIONS ; trnid++) {
      if(trnid  == OTHER_TRNID)
        continue ;
      if(T[trnid].current_operation != NULL ) {
	if(flagPending == 0) {
	  flagPending = 1 ;
	} 
	if(T[trnid].timestamp > tickNo) {		//The transaction hasn't arrived yet
	  continue ; 
	}
        if(T[trnid].current_operation->opnTimestamp <= tickNo) {		//Only consider the operation if it has already arrived or if its a previously committed operation
          struct operation *opn = T[trnid].current_operation ;
          if(opn->opnType == READ_OPN ) {
            if(opn->variablenumber % 2 == 1) {	//If operation is to be performed on an unreplicated variable
              int sitenumber = (opn->variablenumber % 10) + 1 ;
              opn->sitenumber = sitenumber ;
              if(opn->operationStatusAtSites[sitenumber] == OPN_PENDING )
	      {	//Which means the operation has not been sent to the site yet
                if(siteInfo[sitenumber].flag_site_available == 0 ) {		//If the site @ which operation was sent is now unavailable, and if there is no other site @ which operation can be performed, transaction is aborted
#ifdef ABORT_SITE_FAIL
                  sprintf(log_desc,"startTransactionManager: Transaction ID: %d ABORTED since read on var %d failed due to site %d failure\n", trnid, opn->variablenumber, opn->sitenumber) ;
		  logString(log_desc);
                  T[trnid].current_operation = NULL ;			//Transaction cannot proceed further, hence it's aborted
#endif
#ifdef WAIT_SITE_FAIL
                  if(opn->waitlistedOpnTicknumber == -1 ) {
                    opn->waitlistedOpnTicknumber = tickNo ;
                  }
                  opn->operationStatusAtSites[sitenumber] = OPN_PENDING ;
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked at read on var %d since site %d has temporarily failed. Retrying on next tick..\n", trnid, opn->variablenumber, sitenumber) ;
                  logString(log_desc) ;
                  continue ;
#endif
                }
                else {
                  opn->sitenumber = sitenumber ;
                  opn->waitlistedOpnTicknumber = -1 ;
                  performOperation(opn, sitenumber) ;					//Send operation to be performed at the site
                  //Check if operation has been rejected/completed or blocked
                  if(opn->operationStatusAtSites[sitenumber] == OPN_REJECTED ) {
                    sprintf(log_desc,"startTransactionManager: Transaction ID: %d ABORTED since read on var %d rejected by site %d\n", trnid, opn->variablenumber, opn->sitenumber) ;
                    logString(log_desc) ;
                    abortTransaction(opn) ;
                    T[trnid].trn_Completion_Status = TRN_ABORTED ;
                    T[trnid].current_operation = NULL ;
                  }
                  else if(opn->operationStatusAtSites[sitenumber] == OPN_BLOCKED) {
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked for read on var %d @ site %d since the site could not provide the lock\n", trnid, opn->variablenumber, opn->sitenumber) ;
                    logString(log_desc) ;
                    //Do nothing
                  }
                  else if(opn->operationStatusAtSites[sitenumber] == OPN_COMPLETE) {
                    //Operation has been completed
                    if(T[trnid].accessedSites[sitenumber].firstAccessedTick == -1) {        //If this is the first time transaction transaction has accessed the site
                      T[trnid].accessedSites[sitenumber].firstAccessedTick = tickNo ;
                    }
                    T[trnid].current_operation = T[trnid].current_operation->opnTM ;
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d Read on var no. %d returns %d from site %d\n", trnid, opn->variablenumber, opn->readValue, opn->sitenumber) ;
                    logString(log_desc) ;
                  }
                }
              }
              else if(opn->operationStatusAtSites[sitenumber] == OPN_BLOCKED) {	//If operation was previously blocked by the site
                if(siteInfo[sitenumber].flag_site_available == 0 ) {
#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction ID: %d ABORTED since read on var %d at site %d timed out\n", trnid, opn->variablenumber, opn->sitenumber ) ;
                  abortTransaction(opn) ;
                  T[trnid].trn_Completion_Status = TRN_ABORTED ;
                  T[trnid].current_operation = NULL ;
#endif
#ifdef WAIT_SITE_FAIL
		  if(opn->waitlistedOpnTicknumber == -1 ) {
		    opn->waitlistedOpnTicknumber = tickNo ;    //Record the tickNo at which the operation was blocked due to site failure
		  }
		  sprintf(log_desc,"startTransactionManager: Transaction ID: %d has blocked at read on var %d at since site %d has temporarily failed\n", trnid, opn->variablenumber, opn->sitenumber ) ;
                  logString(log_desc) ;
		  opn->operationStatusAtSites[sitenumber] = OPN_PENDING ;
		} 
#endif
              }
              else if(opn->operationStatusAtSites[sitenumber] == OPN_COMPLETE) {
                if(T[trnid].accessedSites[sitenumber].firstAccessedTick == -1) {        //If this is the first time transaction transaction has accessed the site
                  T[trnid].accessedSites[sitenumber].firstAccessedTick = tickNo ;
                }
                T[trnid].current_operation = T[trnid].current_operation->opnTM ;
                sprintf(log_desc, "startTransactionManager: Transaction ID: %d Read on var no. %d returns %d from site %d\n", trnid, opn->variablenumber, opn->readValue, opn->sitenumber) ;
                logString(log_desc) ;
              }
            }
            else {	//Variable number is even and it's replicated on all sites
              RETRY_READ:
              if(opn->sitenumber == -1) {
                opn->sitenumber = 1 ;	//Begin to try reading starting from the 1st site
              }
              if(siteInfo[opn->sitenumber].flag_site_available == 0) {	//If the site has failed
                while(siteInfo[opn->sitenumber].flag_site_available == 0 && opn->sitenumber < MAXIMUM_SITES) {
                  opn->sitenumber++ ;
                }
                if(opn->sitenumber == MAXIMUM_SITES) {	//We have tried all the sites and we could not get the read done
#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction ID: %d ABORTED since read on var %d could not be completed at any of the sites\n", trnid, opn->variablenumber) ;
                  abortTransaction(opn) ;
                  T[trnid].trn_Completion_Status = TRN_ABORTED ;
                  T[trnid].current_operation = NULL ;
                  continue ;
#endif
#ifdef WAIT_SITE_FAIL
                  if(opn->waitlistedOpnTicknumber == -1) {
                    opn->waitlistedOpnTicknumber = tickNo ;
                  }
                  int i = 1 ;
                  for(i = 1; i < MAXIMUM_SITES; i++) {		//We will retry this operation again at all sites
                    opn->operationStatusAtSites[i] = OPN_PENDING ;
                  }
                  opn->sitenumber = 1 ;
                  sprintf(log_desc,"startTransactionManager: Transaction ID: %d blocked at read on var %d since all sites have failed. Retrying on the sites at the next tick..\n", trnid, opn->variablenumber) ;
                  logString(log_desc) ;
                  continue ;
#endif
                }
              }
              if(opn->operationStatusAtSites[opn->sitenumber] == OPN_PENDING) {
                opn->waitlistedOpnTicknumber = -1 ;
                performOperation(opn, opn->sitenumber) ;                                       //Send operation to be performed at the site
                //Check if operation has been rejected/completed or blocked
                if(opn->operationStatusAtSites[opn->sitenumber] == OPN_REJECTED ) {
                  //sprintf(log_desc, "startTransactionManager: Transaction ID: %d read on variablenumber %d rejected @ site %d. Retrying next available site\n", trnid, opn->variablenumber, opn->sitenumber) ;
                  //logString(log_desc) ;
                  opn->sitenumber++ ;
                  if(opn->sitenumber == MAXIMUM_SITES) {        //We have tried all the sites and we could not get the read done
                    abortTransaction(opn) ;
                    T[trnid].trn_Completion_Status = TRN_ABORTED ;
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d ABORTED since read on variablenumber %d since the operation could not be completed at any site\n", trnid, opn->variablenumber) ;
		    logString(log_desc);	
                    T[trnid].current_operation = NULL ;
                    continue ;
                  }
                  else {
                    goto RETRY_READ ;
                  }
                }
                else if(opn->operationStatusAtSites[opn->sitenumber] == OPN_BLOCKED) {
                  //Do nothing
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked for read on var %d @ site %d since site could not provide the lock\n", trnid, opn->variablenumber, opn->sitenumber) ;
                  logString(log_desc) ;
                }
                else if(opn->operationStatusAtSites[opn->sitenumber] == OPN_COMPLETE) {
                  //Operation has been completed
                  T[trnid].current_operation = T[trnid].current_operation->opnTM ;
                  if(T[trnid].accessedSites[opn->sitenumber].firstAccessedTick == -1) {        //If this is the first time transaction transaction has accessed the site
                    T[trnid].accessedSites[opn->sitenumber].firstAccessedTick = tickNo ;
                  }
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d Read on var no. %d returns %d from site %d\n", trnid, opn->variablenumber, opn->readValue, opn->sitenumber) ;
                  logString(log_desc) ;
                }
              }
              else if(opn->operationStatusAtSites[opn->sitenumber] == OPN_BLOCKED) {       //If operation was previously blocked by the site
                if(siteInfo[opn->sitenumber].flag_site_available == 0 ) {  //If operation has timed out
                  opn->sitenumber++ ;
                  if(opn->sitenumber == MAXIMUM_SITES) {	//We have tried all the sites and we could not get the read done
#ifdef ABORT_SITE_FAIL
                    printf("startTransactionManager: Transaction ID: %d ABORTED since read on var %d could not be completed at any of the sites\n", trnid, opn->variablenumber) ;
                    abortTransaction(opn) ;
                    T[trnid].trn_Completion_Status = TRN_ABORTED ;
                    T[trnid].current_operation = NULL ;
                    continue ;
#endif
#ifdef WAIT_SITE_FAIL
                    if(opn->waitlistedOpnTicknumber == -1) {
                      opn->waitlistedOpnTicknumber = tickNo ;
                    }
                    int i = 1 ;
                    for(i = 1; i < MAXIMUM_SITES; i++) {		//We will retry this operation again at all sites
                      opn->operationStatusAtSites[i] = OPN_PENDING ;
                    }
                    opn->sitenumber = 1 ;
                    sprintf(log_desc,"startTransactionManager: Transaction ID: %d blocked at read on var %d since all sites have failed. Retrying on the sites at the next tick..\n", trnid, opn->variablenumber) ;
                    logString(log_desc) ;
                    continue ;
#endif
                  }
		  else { 
                    sprintf(log_desc, "startTransactionManager: Transaction %d read @ site %d for variablenumber %d failed due to site failure, Retrying on next available site\n", trnid, opn->sitenumber-1, opn->variablenumber) ;
                    logString(log_desc) ;
                  }
                }
                else {
                  sprintf(log_desc, "startTransactionManager: Transaction %d waiting for read @ site %d for variablenumber %d to be complete\n", trnid, opn->sitenumber, opn->variablenumber) ;
                  //logString(log_desc) ;
                  printf("%s", log_desc) ;
                }

              }
              else if(opn->operationStatusAtSites[opn->sitenumber] == OPN_COMPLETE) {
                if(T[trnid].accessedSites[opn->sitenumber].firstAccessedTick == -1) {        //If this is the first time transaction transaction has accessed the site
                  T[trnid].accessedSites[opn->sitenumber].firstAccessedTick = tickNo ;
                }
                sprintf(log_desc, "startTransactionManager: Transaction ID: %d Read on var no. %d returns %d from site %d\n", trnid, opn->variablenumber, opn->readValue, opn->sitenumber) ;
                logString(log_desc) ;
                T[trnid].current_operation = T[trnid].current_operation->opnTM ;
              }
            }
          }

          else if(opn->opnType == WRITE_OPN ) {
            if(opn->variablenumber % 2 == 1) {	//If operation is to be performed on an unreplicated variable
              int sitenumber = (opn->variablenumber % 10) + 1 ;
              opn->sitenumber = sitenumber ;
              if(opn->operationStatusAtSites[sitenumber] == OPN_PENDING ) {	//Which means the operation has not been sent to the site yet
                if(siteInfo[sitenumber].flag_site_available == 0 ) {		//If the site @ which operation was sent is now unavailable, and if there is no other site at which operation can be performed, transaction is aborted

#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction ID: %d ABORTED since write on var %d with value %d failed due to site %d failure\n", trnid, opn->variablenumber, opn->writtenValue, opn->sitenumber) ;
                  T[trnid].current_operation = NULL ;			//Transaction cannot proceed further, hence it's aborted
#endif
#ifdef WAIT_SITE_FAIL
                  if(opn->waitlistedOpnTicknumber == -1 ) {
                    opn->waitlistedOpnTicknumber = tickNo ;
                  }
                  opn->operationStatusAtSites[sitenumber] = OPN_PENDING ;
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked at write on var %d with value %d since site %d has temporarily failed\n", trnid, opn->variablenumber, opn->writtenValue, opn->sitenumber) ;
                  logString(log_desc) ;
#endif
                }
                else {
                  opn->sitenumber = sitenumber ;
                  opn->waitlistedOpnTicknumber = -1 ;
                  performOperation(opn, sitenumber) ;					//Send operation to be performed at the site
                  //Check if operation has been rejected/completed or blocked
                  if(opn->operationStatusAtSites[sitenumber] == OPN_REJECTED ) {
                    sprintf(log_desc, "startTransactionManager: Transaction %d ABORTED since write on variablenumber %d with value %d rejected by site %d\n", trnid, opn->variablenumber, opn->writtenValue, sitenumber) ;
                    logString(log_desc) ;
                    abortTransaction(opn) ;
                    T[trnid].trn_Completion_Status = TRN_ABORTED ;
                    T[trnid].current_operation = NULL ;
                  }
                  else if(opn->operationStatusAtSites[sitenumber] == OPN_BLOCKED) {
                    sprintf(log_desc,"startTransactionManager: Transaction ID: %d blocked for write on variablenumber %d with value %d @ site %d since site could not provide it with the lock\n", trnid, opn->variablenumber, opn->writtenValue, opn->sitenumber) ;
                    logString(log_desc) ;
                    //Do nothing
                  }
                  else if(opn->operationStatusAtSites[sitenumber] == OPN_COMPLETE) {
                    //Operation has been completed
                   sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on var %d with value %d completed\n", trnid, opn->variablenumber, opn->writtenValue, opn->sitenumber) ;
                   logString(log_desc) ;
                    T[trnid].current_operation = T[trnid].current_operation->opnTM ;
                    if(T[trnid].accessedSites[sitenumber].firstAccessedTick == -1) {        //If this is the first time transaction transaction has accessed the site
                      T[trnid].accessedSites[sitenumber].firstAccessedTick = tickNo ;
                    }
                    if(T[trnid].accessedSites[sitenumber].writeAccessed_Flag == 0) {
                      T[trnid].accessedSites[sitenumber].writeAccessed_Flag = 1 ;       //Set a flag indicating transaction has accessed the site for write operation
                    }
                  }
                }
              }
              else if(opn->operationStatusAtSites[sitenumber] == OPN_BLOCKED) {	//If operation was previously blocked by the site
                if(siteInfo[sitenumber].flag_site_available == 0 ) {
#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction %d ABORTED since write on variablenumber %d with value to be written %d at site %d timedout\n", trnid, opn->variablenumber, opn->writtenValue, opn->sitenumber ) ;
                  abortTransaction(opn) ;
                  T[trnid].trn_Completion_Status = TRN_ABORTED ;
                  T[trnid].current_operation = NULL ;
#endif
#ifdef WAIT_SITE_FAIL
		  if(opn->waitlistedOpnTicknumber == -1 ) {
		    opn->waitlistedOpnTicknumber = tickNo ;    //Record the tickNo at which the operation was blocked due to site failure
		  }
		  sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked at write on var %d with value %d since site %d has temporarily failed\n", trnid, opn->variablenumber, opn->writtenValue, opn->sitenumber ) ;
                  logString(log_desc) ;
		  opn->operationStatusAtSites[sitenumber] = OPN_PENDING ;
#endif
                }
                else {
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d still waiting for write on var %d with value %d blocked @ site %d\n", trnid, opn->variablenumber, opn->writtenValue, opn->sitenumber) ;
                    //logString(log_desc) ;
                    printf("%s", log_desc) ;
                }
              }
              else if(opn->operationStatusAtSites[sitenumber] == OPN_COMPLETE) {
                sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on var %d with value %d completed\n", trnid, opn->variablenumber, opn->writtenValue, opn->sitenumber) ;
                logString(log_desc) ;
                T[trnid].current_operation = T[trnid].current_operation->opnTM ;
              }
            }
            else {
              int sitenumber, flag_writeStatus = WRITE_CHECK_FOR_COMPLETE ;
              int flag_writePerformed = 0 ;
              for(sitenumber = 1; sitenumber < MAXIMUM_SITES ; sitenumber++ ) {
                if(opn->operationStatusAtSites[sitenumber] == OPN_IGNORE || opn->operationStatusAtSites[sitenumber] == OPN_COMPLETE) {	//We choose to ignore this site since it has/had failed during write
                  continue ;
                }
                else if(opn->operationStatusAtSites[sitenumber] == OPN_PENDING) {
                  if(siteInfo[sitenumber].flag_site_available == 0) {
                    opn->operationStatusAtSites[sitenumber] = OPN_IGNORE ;	//We will be igoring this site since it was not available at the time of writing
                    continue ;
                  }
                  else {	//site is available, perform the operation on that site
                    opn->waitlistedOpnTicknumber = -1 ;
                    performOperation(opn, sitenumber) ;                                       //Send operation to be performed at the site
                    //Check if operation has been rejected/completed or blocked
                    if(opn->operationStatusAtSites[sitenumber] == OPN_REJECTED ) {
                      sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on variablenumber %d with value %d rejected by site %d\n", trnid, opn->variablenumber, opn->writtenValue, sitenumber) ;
                      logString(log_desc) ;
                      flag_writeStatus = WRITE_FAILED ;
                      break ;
                    }
                    else if(opn->operationStatusAtSites[sitenumber] == OPN_BLOCKED) {
                      //Do nothing
                      sprintf(log_desc, "startTransactionManager: Transaction ID: %d blocked for write on var %d with value %d @ site %d since site could not provide it with the lock\n", trnid, opn->variablenumber, opn->writtenValue, sitenumber) ;
                      logString(log_desc) ;
                      printf("%s", log_desc) ;
                      if(flag_writeStatus != WRITE_PENDING) {
                        flag_writeStatus = WRITE_PENDING ;		//Set a flag indicating write is pending at one of the sites
                      }
                    }
                    else if(opn->operationStatusAtSites[sitenumber] == OPN_COMPLETE) {
                      //Operation has been completed at one of the sites
                      if(flag_writePerformed == 0){
                        flag_writePerformed = 1 ;	//Indicate we have successfully written at atleast one of the sites
                      }
                      if(T[trnid].accessedSites[sitenumber].firstAccessedTick == -1) {        //If this is the first time transaction transaction has accessed the site
                        T[trnid].accessedSites[sitenumber].firstAccessedTick = tickNo ;
                      }
                      if(T[trnid].accessedSites[sitenumber].writeAccessed_Flag == 0) {
                        T[trnid].accessedSites[sitenumber].writeAccessed_Flag = 1 ;       //Set a flag indicating transaction has accessed the site for write operation
                      }
                      sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on var %d with value %d @ site %d completed\n", trnid, opn->variablenumber, opn->writtenValue, sitenumber) ;
                      logString(log_desc) ;
                    }
                  }
                }
                else if(opn->operationStatusAtSites[sitenumber] == OPN_BLOCKED) {
                  if(siteInfo[sitenumber].flag_site_available == 0 ) {  //If site on which write was sent has now failed
                    opn->operationStatusAtSites[sitenumber] = OPN_IGNORE ;
                  }
                  else {
                    sprintf(log_desc, "startTransactionManager: Transaction ID: %d still waiting for write on var %d with value %d @ site %d\n", trnid, opn->variablenumber, opn->writtenValue, sitenumber) ;
                    //logString(log_desc) ;
                    printf("%s", log_desc) ;
                    if(flag_writeStatus != WRITE_PENDING) {
                      flag_writeStatus = WRITE_PENDING ;         //Set a flag indicating write is pending at one of the sites
                    }
                  }
                }
                else if(opn->operationStatusAtSites[sitenumber] == OPN_COMPLETE) {
		  if(T[trnid].accessedSites[sitenumber].firstAccessedTick == -1) {        //If this is the first time transaction transaction has accessed the site
		    T[trnid].accessedSites[sitenumber].firstAccessedTick = tickNo ;
		  }
		  if(T[trnid].accessedSites[sitenumber].writeAccessed_Flag == 0) {
		    T[trnid].accessedSites[sitenumber].writeAccessed_Flag = 1 ;       //Set a flag indicating transaction has accessed the site for write operation
		  } 
                  if(flag_writePerformed == 0){
		    flag_writePerformed = 1 ;	//Indicate we have successfully written at atleast one of the sites
		  }
                  sprintf(log_desc,"startTransactionManager: Transaction ID: %d write on var %d with value %d @ site %d completed\n", trnid, opn->variablenumber, opn->writtenValue, sitenumber) ;
                  logString(log_desc) ;
                }
              }
              if(flag_writeStatus == WRITE_CHECK_FOR_COMPLETE) {	//Either write happened to all sites or all sites have failed
		//Check if we have performed atleast one write successfully
                if(flag_writePerformed == 1) { 
		  T[trnid].current_operation = T[trnid].current_operation->opnTM ;
                  sprintf(log_desc, "startTransactionManager: Transaction ID: %d write on variablenumber %d with value %d completed at all available sites\n", trnid, opn->variablenumber, opn->writtenValue) ;
                  logString(log_desc) ;
                }
                else {
#ifdef ABORT_SITE_FAIL
                  printf("startTransactionManager: Transaction ID: %d ABORTED since write on var %d with value %d could not be completed at any of the sites\n", trnid, opn->variablenumberopn->writtenValue) ;
                  abortTransaction(opn) ;
                  T[trnid].trn_Completion_Status = TRN_ABORTED ;
                  T[trnid].current_operation = NULL ;
#endif
#ifdef WAIT_SITE_FAIL
                  if(opn->waitlistedOpnTicknumber == -1) {
                    opn->waitlistedOpnTicknumber = tickNo ;
                  }
                  int i = 1 ;
                  for(i = 1; i < MAXIMUM_SITES; i++) {		//We will retry this operation again at all sites
                    opn->operationStatusAtSites[i] = OPN_PENDING ;
                  }
                  sprintf(log_desc ,"startTransactionManager: Transaction ID: %d blocked at write on var %d with value %d since all sites have failed. Retrying on the sites at the next tick..\n", trnid, opn->variablenumber, opn->writtenValue) ;
                  logString(log_desc) ;
#endif
                }
	      } 
              else if(flag_writeStatus == WRITE_FAILED) {
                sprintf(log_desc, "startTransactionManager: Transaction ID: %d ABORTED since write on variablenumber %d with value to be written %d rejected by site %d\n", trnid, opn->variablenumber, opn->writtenValue, sitenumber) ;
                logString(log_desc) ;
                abortTransaction(opn) ;
                T[trnid].trn_Completion_Status = TRN_ABORTED ;
                T[trnid].current_operation = NULL ;
              }
            }
          }

          else if(opn->opnType == END_OPN ) {
            int sitenumber, flagCommit = 1 ;
	    for(sitenumber = 1; sitenumber < MAXIMUM_SITES && flagCommit == 1 && T[trnid].trnType != RONLY_TRANSACTIONS; sitenumber++) {
	      if(T[trnid].accessedSites[sitenumber].firstAccessedTick != -1) {	//If the transaction has accessed this site
		if(siteInfo[sitenumber].flag_site_available == 0 ) {
		  flagCommit = 0 ;
		  sprintf(log_desc, "startTransactionManager: Transaction %d could not commit and has been ABORTED since site %d has failed\n", trnid, sitenumber) ;
                  logString(log_desc) ;
		  abortTransaction(opn) ;
		  T[trnid].trn_Completion_Status = TRN_ABORTED ;
		  T[trnid].current_operation = NULL ;
		}
		else if(siteInfo[sitenumber].tick_time > T[trnid].accessedSites[sitenumber].firstAccessedTick) {	//The site had failed after transaction accessed it for first time
                  flagCommit = 0 ;
                  sprintf(log_desc, "startTransactionManager: Transaction %d ABORTED since site %d had failed at some point after the transaction first accessed it\n", trnid, sitenumber) ;
                  logString(log_desc) ;
                  abortTransaction(opn) ; 
		  T[trnid].trn_Completion_Status = TRN_ABORTED ;
                  T[trnid].current_operation = NULL ;
		} 
	      }
	    }
	    if(flagCommit == 1) {
	      sprintf(log_desc, "startTransactionManager: Transaction %d committed @ tick %d\n", trnid, tickNo) ; 
              logString(log_desc) ;
	      for(sitenumber = 1; sitenumber < MAXIMUM_SITES; sitenumber++) {
		if(T[trnid].accessedSites[sitenumber].firstAccessedTick != -1 && siteInfo[sitenumber].flag_site_available == 1) {
		    performOperation(T[trnid].current_operation,sitenumber); ///// CHECK HERE , when i try to print anything at site nothing gets printed for op
		} 
	      }
	      T[trnid].current_operation = NULL ; 
              T[trnid].trn_Completion_Status = TRN_COMMITED ;
	    }
          }
        }
      }
      else if(T[trnid].current_operation == NULL && (T[trnid].trn_Completion_Status == -1 && T[trnid].trn_valid_flag == 1)) {
	if(flagPending == 0) {
	  flagPending = 1 ;
        }
        if(T[trnid].inactive_tick_number == -1) {
          T[trnid].inactive_tick_number = tickNo ;
	  sprintf(log_desc, "startTransactionManager: Transaction %d is waiting for some operation to be received from the input sequence\n", trnid) ;
          logString(log_desc) ;
        }
        else {
	  sprintf(log_desc, "startTransactionManager: Transaction %d still WAITING for a new operation from the input for over %d ticks\n", trnid, tickNo - T[trnid].inactive_tick_number) ;
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
  int sitenumber ;
  int trnid = opn->trnid ;
  memcpy(&abort_opn, opn, sizeof(struct operation)) ;		// CHECK HERE
  abort_opn.opnType = ABORT_OPN ;

  for(sitenumber = 1; sitenumber < MAXIMUM_SITES; sitenumber++) {
    abort_opn.sitenumber = sitenumber ; 
    abort_opn.opnSite = NULL ; 
    if(T[trnid].accessedSites[sitenumber].firstAccessedTick != -1 && siteInfo[sitenumber].flag_site_available == 1) {
      performOperation(&abort_opn, sitenumber) ;
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


  T[OTHER_TRNID].trnType = OTHER_TRANSACTIONS ;
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

    int trnType = RONLY_TRANSACTIONS ;
    ret = createNewTransaction(trnid, trnType, operationtimestamp) ;
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
    int trnType = READ_WRITE_TRANSACTIONS ;
    ret = createNewTransaction(trnid, trnType, operationtimestamp) ;
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
    int variablenumber = -1, writtenValue = -1, sitenumber = -1 ;
    ret = prepareOperationNode(trnid, END_OPN, variablenumber, writtenValue, sitenumber, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for end operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;


    
  }
  else if(strncmp(operationString,"dump", strlen("dump")) == 0) {
    int variablenumber = -1, sitenumber ;
    trnid = OTHER_TRNID ;
    temp = strstr(operationString, "(") ;
    if(temp == NULL) {
     printf("storeOperation: Could not parse dump operation %s\n", operationString) ;
     return -1 ;
    }
    while(*temp != ')' && !isalpha(*temp) && !isdigit(*temp))
     temp++ ;


    if(*temp == ')') {		
     variablenumber = ALL_VARIABLES ;
     sitenumber = ALL_SITES ;
    }

    else if(isdigit(*temp)) {		
     sitenumber = atoi(temp) ;
     variablenumber = ALL_VARIABLES ;

     
    }
    else if(isalpha(*temp)) {
     while(!isdigit(*temp))
      temp++ ;
     variablenumber = atoi(temp) ;
     sitenumber = ALL_SITES ;

     
    }
    int writtenValue = -1 ;
    ret = prepareOperationNode(trnid, DUMP_OPN, variablenumber, writtenValue, sitenumber, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for dump operation\n") ;
      return -1 ;
    }

    addOperationToTransactionQueue(trnid, opn) ;
  }
  else if(strncmp(operationString,"fail", strlen("fail")) == 0) {
    trnid = OTHER_TRNID ;
    temp = strstr(operationString, "(") ;
    if(temp == NULL) {
     printf("storeOperation: Could not parse fail operation %s\n", operationString) ;
     return -1 ;
    }
    while(!isdigit(*temp))
     temp++ ;
    int sitenumber = atoi(temp) ;
    int variablenumber = -1, writtenValue = -1 ;
    ret = prepareOperationNode(trnid, FAIL_OPN, variablenumber, writtenValue, sitenumber, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for fail operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;
  }
  else if(strncmp(operationString,"recover", strlen("recover")) == 0) {
    trnid = OTHER_TRNID ;
    temp = strstr(operationString, "(") ;
    if(temp == NULL) {
     printf("storeOperation: Could not parse recover operation %s\n", operationString) ;
     return -1 ;
    }
    while(!isdigit(*temp))
     temp++ ;
    int sitenumber = atoi(temp) ;

    
    int variablenumber = -1, writtenValue = -1 ;
    ret = prepareOperationNode(trnid, RECOVER_OPN, variablenumber, writtenValue, sitenumber, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for recover operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;
  }
  else if(strncmp(operationString,"querystate", strlen("querystate")) == 0) {
    trnid = OTHER_TRNID ;
    int sitenumber = ALL_SITES, variablenumber = ALL_VARIABLES, writtenValue = -1 ;
    ret = prepareOperationNode(trnid, QUERY_STATE_OPN, variablenumber, writtenValue, sitenumber, operationtimestamp, opn) ;
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
    int variablenumber = atoi(temp) ;
    int writtenValue = -1, sitenumber = -1 ;
    ret = prepareOperationNode(trnid, READ_OPN, variablenumber, writtenValue, sitenumber, operationtimestamp, opn) ;
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
    int variablenumber = atoi(temp) ;

    while(isdigit(*temp))
     temp++ ;
    while(!isdigit(*temp) && *temp != '-')
     temp++ ;
    int writtenValue = atoi(temp), sitenumber = -1 ;
    ret = prepareOperationNode(trnid, WRITE_OPN, variablenumber, writtenValue, sitenumber, operationtimestamp, opn) ;
    if(ret == -1) {
      printf("storeOperation: prepareOperationNode returns error for new operation\n") ;
      return -1 ;
    }
    addOperationToTransactionQueue(trnid, opn) ;

    
  }
  return  0 ;
}

//A new transaction entry is created
int createNewTransaction(int trnid, int trnType, int timestamp) {
  if(trnid >= MAXIMUM_TRANSACTIONS ) {	//Transaction Manager can't support transaction ids greater than MAXIMUM_TRANSACTIONS
    printf("createNewTransaction: Transaction %d exceeds Max limit %d\n", trnid, MAXIMUM_TRANSACTIONS) ;
    return -1 ;
  }
  if(T[trnid].timestamp != -1 ) {
    printf("createNewTransaction: Error duplication of trnid %d in input transaction sequence\n", trnid) ;
    return -1 ;
  }
  T[trnid].timestamp = timestamp ;
  T[trnid].trnType = trnType ;
  T[trnid].trn_valid_flag = 1 ;
  return 0 ;
}

void addOperationToTransactionQueue(int trnid, struct operation *opn) {
  if(T[trnid].first_operation == NULL) {	//This is the first operation assigned to that transaction
    T[trnid].first_operation = T[trnid].last_operation = T[trnid].current_operation = opn ;
  }
  else {
    T[trnid].last_operation->opnTM = opn ;
    T[trnid].last_operation = opn ;
  }
  return  ;
}

int prepareOperationNode(int trnid, int opnType, int variablenumber, int writtenValue, int sitenumber, int operationtimestamp, struct operation *opn) {
  opn->trnid = trnid ;
  opn->opnType = opnType ;
  opn->opnTimestamp = operationtimestamp ;
  opn->opnTM = NULL ;
  opn->variablenumber = variablenumber ;
  opn->writtenValue = writtenValue ;
  opn->sitenumber = sitenumber ;
  opn->waitlistedOpnTicknumber = -1 ;
  int site_No ;
  for(site_No = 1 ; site_No < MAXIMUM_SITES; site_No++) {
    opn->operationStatusAtSites[site_No] = OPN_PENDING ;
  }
  
  if(T[trnid].timestamp == -1 && trnid == OTHER_TRNID) {
    T[trnid].timestamp = operationtimestamp ;
  }
  opn->trnType = T[trnid].trnType ;
  opn->trnTimestamp = T[trnid].timestamp ;

  return 0 ;
}






void initializeTransactionManager() {
  //Initialize all transaction structures
  int trnid, sitenumber ;
  for(trnid = 0; trnid < MAXIMUM_TRANSACTIONS; trnid++) {
    T[trnid].timestamp = -1 ;
    T[trnid].trnType = -1 ;
    T[trnid].trn_Completion_Status = -1 ;
    T[trnid].inactive_tick_number = -1 ;
    T[trnid].trn_valid_flag = 0 ;
    T[trnid].first_operation = T[trnid].last_operation = T[trnid].current_operation = NULL ;
    for(sitenumber = 0; sitenumber < MAXIMUM_SITES; sitenumber++)  {
      T[trnid].accessedSites[sitenumber].firstAccessedTick = -1 ;
      T[trnid].accessedSites[sitenumber].writeAccessed_Flag = 0 ; 
    }
  }

  //Initialize variable-site mapping
  int variablenumber ;
  for(variablenumber = 1; variablenumber < MAXIMUM_VARIABLES ; variablenumber++)  {
    if(variablenumber % 2 == 1) {	//if the variable is odd, use the given data in problem statement to find the sitenumber @ which it will be stored
      sitenumber = (variablenumber % 10) + 1 ;
      siteInfo[sitenumber].flag_variablenumber[variablenumber] = 1 ;
    }
    else {	//Even variables are present at all sites
      for(sitenumber = 1; sitenumber < MAXIMUM_SITES ; sitenumber++ ) {
        siteInfo[sitenumber].flag_variablenumber[variablenumber] = 1 ;
      }
    }
  }

  for(sitenumber = 1; sitenumber < MAXIMUM_SITES; sitenumber++) {	//All sites are assumed to be up initially
    siteInfo[sitenumber].flag_site_available = 1 ;
    siteInfo[sitenumber].tick_time = 0 ;		//All sites are assumed to be up from the 0th tick
  }
}


void Sleep_ms(int time_ms) {
  struct timeval tv ;
  tv.tv_sec = 0 ;
  tv.tv_usec = time_ms * 1000 ;
  select(0, NULL , NULL, NULL, &tv) ;
  return ;
}
