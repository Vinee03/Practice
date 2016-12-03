#define MAX_TRANSACTIONS	10000
#define MISCELLANEOUS_TID       9999             
#define MAX_WAITING_TICKS	20		
#include "globalvar.h"
#include <time.h>

#define TRANSACTION_COMMITED	1
#define TRANSACTION_ABORTED	0

/********************************************************************************************************************
                                        struct siteAccessInfo starts
                         The structure gives information for a particular site for a particular transaction
************************************************************************************************************************/
struct siteAccessInfo {		
 int tick_firstAccessed ;
 int flagWriteAccessed ;
} ;
/********************************************************************************************************************
                                        struct siteInformation starts
                         The structure gives information for a particular site for a particular transaction
************************************************************************************************************************/

struct siteInformation {



  int flag_varNo[MAXIMUM_SITES] ;
  int flag_siteAvailable ;	
  int tick_upTime ;		
} ;
typedef struct siteInformation siteInformation ;
siteInformation siteInfo[MAXIMUM_SITES] ;

/**************************************************************************************************************************
                                     struct Transaction Starts          
*****************************************************************************************************************************/
struct Transaction {
 struct siteAccessInfo sites_accessed[MAXIMUM_SITES];
 int timestamp ;
 int flag_transactionValid ;
 int transactionType ;
 int transactionCompletionStatus ;
 int inactiveTickNo ;
 struct operation *first_opn ;
 struct operation *last_opn ;
 struct operation *current_opn ;
} ;

struct Transaction T[MAX_TRANSACTIONS] ;
void initializeTransactionManager() ;
void startTransactionManager() ;
