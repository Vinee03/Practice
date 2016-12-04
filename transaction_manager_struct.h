#define MAXIMUM_TRANSACTIONS	10000
#define OTHER_TRNID       9999             
#define MAXIMUM_WAITING_TICKS	20		
#include "globalvar.h"
#include <time.h>

#define TRN_COMMITED	1
#define TRN_ABORTED	0

/********************************************************************************************************************
                                        struct siteAccessInfo starts
                         The structure gives information for a particular site for a particular transaction
************************************************************************************************************************/
struct siteAccessInfo {		
 int firstAccessedTick ;
 int writeAccessed_Flag ;
} ;
/********************************************************************************************************************
                                        struct siteInformation starts
                         The structure gives information for a particular site for a particular transaction
************************************************************************************************************************/

struct siteInformation {



  int flag_variablenumber[MAXIMUM_SITES] ;
  int flag_site_available ;	
  int tick_time ;		
} ;
typedef struct siteInformation siteInformation ;
siteInformation siteInfo[MAXIMUM_SITES] ;

/**************************************************************************************************************************
                                     struct Transaction Starts          
*****************************************************************************************************************************/
struct Transaction {
 struct siteAccessInfo accessedSites[MAXIMUM_SITES];
 int timestamp ;
 int trn_valid_flag ;
 int trnType ;
 int trn_Completion_Status ;
 int inactive_tick_number ;
 struct operation *first_operation ;
 struct operation *last_operation ;
 struct operation *current_operation ;
} ;

struct Transaction T[MAXIMUM_TRANSACTIONS] ;
void initializeTransactionManager() ;
void startTransactionManager() ;
