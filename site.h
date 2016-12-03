#include "globals.h"
#define MAX_TRANSACTION_TIMESTAMP 10000
void initializeSiteData() ;
void performOperation(struct operation *opn, int siteNo) ;
/**********************************************************************************************************
                                         struct lockTable Starts
                      It consists of two operation first active operation and first blocked operation.
                      The variables flag whether the flag exists at the site.
****************************************************************************************************************/
struct lockTable
{
int flag;                                                             
int readAvailable;
struct operation *first_active_operation;
struct operation *first_blocked_operation;
};
/**********************************************************************************************************
                                         struct version Starts
****************************************************************************************************************/

struct version
{
 int tid;                                           
 int value;
 int R_Timestamp;
 int W_Timestamp;
 struct version *next;
};
/**********************************************************************************************************
                                         struct versionTable Starts                                
****************************************************************************************************************/

struct versionTable
{
 int flag;                                            
 struct version *head;
};
/**********************************************************************************************************
                                         struct site Starts                                
****************************************************************************************************************/

struct site
{
struct versionTable variable[MAX_VARIABLES];
struct lockTable lock_Entries[MAX_VARIABLES];
}sites[MAX_SITES];








