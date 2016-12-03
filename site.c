#include <stdio.h>
#include <stdlib.h>

#include "site.h"

int availableSites[MAX_SITES] ; //If the given site is available, the array stores value '1'. If the given site is unavailable, it stores the value '0'.
void releaseLocks(int site_No,int tid);

/********************************************************************************************************************************
						 Function readEnableDisable Starts	
			Enable read for committed if the variable is commited and disable read for all 
			                      variables in the site when a site recovers from failure. 
                                                1 is used to Enable and 0 is used to Disable
************************************************************************************************************************************/
void readEnableDisable(int site_No,int varNo,int enable)
{
	sites[site_No].lock_Entries[varNo].readAvailable = enable; 
}
/******************************************************************************************************************************
                                                 Function readEnableDisable Ends
*********************************************************************************************************************************/

/**********************************************************************************************************************************
                                                 Function checkReadAvailability Starts
				   It checks whether the variable is available for read or not. 
				   If the variable varNo is available to read it returns 1 otherwise it returns 0. 
***********************************************************************************************************************************/
int checkReadAvailability(int site_No,int varNo,int tid)
{

struct operation *current = sites[site_No].lock_Entries[varNo].first_active_operation;
if(current!=NULL)
{
	if(current->tid==tid && current->operationType==WRITE_OPERATION)  
	{
		return 1;  
	}
}

return sites[site_No].lock_Entries[varNo].readAvailable;
}
/**********************************************************************************************************************
                                               Function checkReadAvailability Ends 
************************************************************************************************************************/
/*********************************************************************************************************************
                                            Function readonly_Versiontable starts
					  It updates the version table with the latest Timestamp. 
***********************************************************************************************************************/

int readonly_Versiontable(int site_No,int var,int timestamp)
{
		char log_desc[1000];
	        int valueRead=0;
                if(sites[site_No].variable[var].flag==1)
                {
			struct version* current = sites[site_No].variable[var].head;
			int largest=-1;
		    
 		while(current!=NULL)
			 {
				if(current->W_Timestamp<=timestamp && current->W_Timestamp>largest)
				{
					largest=current->W_Timestamp;
					valueRead=current->value;
				}   
				current=current->next;
			 } 

		}
		


return valueRead;
}
/********************************************************************************************************************
                                      Function readonly_Versiontable Ends
**********************************************************************************************************************/
/***************************************************************************************************************************
                                          Function siteFail Starts 
                                It deletes the values from the Lock Table
				It checks if the variable is present in the site, using the if condition and deletes accordingly.
****************************************************************************************************************************/
void siteFail(int site_No)
{
int j;
	for(j=1;j<=MAX_VARIABLES;j++)
	{
	    if(sites[site_No].lock_Entries[j].flag==1)  
	    {
			sites[site_No].lock_Entries[j].first_active_operation=NULL;	
			sites[site_No].lock_Entries[j].first_blocked_operation=NULL;
	    }
	}

}
/*******************************************************************************************************************************
                                           Function siteFail Ends
**********************************************************************************************************************************/
/**********************************************************************************************************************************
					   Function UpdateVersionTable Starts 
					   There are three things which happen:
1. If a transaction issues a first write: The value of Read Timestamp and Write Timestamp is set to very high. A new entry is made in the Lock Table.
2. If a transaction is aborted, delete the entries from the Lock Table.
3. If a transaction is commited, we will set the Read-Timestamp and Write-Timestamp.
*************************************************************************************************************************************/
void UpdateVersionTable(int site_No,int varNo,struct operation *op)
{
   int j;
  if(varNo>0)
{ 
   if(sites[site_No].variable[varNo].flag==1)
     {
	    if(op->operationType==WRITE_OPERATION)     
	       { 	
		 	struct version* current = sites[site_No].variable[varNo].head;
	         	int exist=0;
		     	while(current!=NULL)
			 {
				if((op->tid==current->tid)&&(current->W_Timestamp==MAX_TRANSACTION_TIMESTAMP))
				{
					current->value=op->valueToWrite;
					exist=1;
				}   
				current=current->next;
			 } 

		
			if(exist==0)
			{
				struct version *newNode = (struct version *) malloc (sizeof(struct version));
				newNode->tid=op->tid;
                       		newNode->value=op->valueToWrite;
 				newNode->R_Timestamp=MAX_TRANSACTION_TIMESTAMP;
				newNode->W_Timestamp=MAX_TRANSACTION_TIMESTAMP;
				newNode->next=sites[site_No].variable[varNo].head;
				sites[site_No].variable[varNo].head=newNode;	
			}
                }

		if(op->operationType==READ_OPERATION)   
		{
		 	struct version* current = sites[site_No].variable[varNo].head;
	         	int exist=0;
		     	while(current!=NULL)
			 {
				if((op->tid==current->tid)&&(current->W_Timestamp==MAX_TRANSACTION_TIMESTAMP))
				{
					op->valueRead=current->value;  
					exist=1;
				}   
				current=current->next;
			 }
			 
			 if(exist==0)  
			 {
			    op->valueRead=readonly_Versiontable(site_No,varNo,op->transactionTimestamp);   
			 }
			 

	        }
     }   
}
     if(op->operationType==END_OPERATION)  
      { 
		
	   for(j=1;j<=MAX_VARIABLES;j++)
             { 
                if(sites[site_No].variable[j].flag==1)
                {
			struct version* current = sites[site_No].variable[j].head;
		     if(current!=NULL)
		     {
		     	while(current!=NULL)
			 {
				if((op->tid==current->tid)&&(current->W_Timestamp==MAX_TRANSACTION_TIMESTAMP))
				{
					current->W_Timestamp=op->transactionTimestamp;
					current->R_Timestamp=op->transactionTimestamp;    
					if(checkReadAvailability(site_No,j,op->tid)==0) 
						readEnableDisable(site_No,j,1);   		 
				}   
				current=current->next;
			 }		  		 
		     }	
		}
	    } 
       }
    



}
/********************************************************************************************************************************
					Function UpdateVersionTable Ends
***********************************************************************************************************************************/
/**********************************************************************************************************************************
					 Function addToActiveList Starts
			It first checks the if the same transaction is being added i.e. request=0. If the same transaction is added,
			it sets the first active operation as node. Otherwise it traverses the entire site and sets the trasaction as node.
************************************************************************************************************************************/

void addToActiveList(int site_No,int varNo,struct operation *node,int request)
{

if(request==0)    
{
	sites[site_No].lock_Entries[varNo].first_active_operation = node;
	node->nextOperationSite = NULL;
}
else
{
	struct operation *current = sites[site_No].lock_Entries[varNo].first_active_operation;
	while(current->nextOperationSite != NULL)
		current=current->nextOperationSite;

	current->nextOperationSite=node;
	node->nextOperationSite=NULL;
}

}
/********************************************************************************************************************
				          Function addToActiveList Ends
***********************************************************************************************************************/
/**********************************************************************************************************************
					  Function addToBlockedList Starts
		It first checks whether there is blocked operation or not. If there is no first blocked operation,
					  it sets the first blocked operation as the node. 
		Else it will traverse the list, and set the next operation as the node. 
*************************************************************************************************************************/


void addToBlockedList(int site_No,int varNo,struct operation *node)
{

if(sites[site_No].lock_Entries[varNo].first_blocked_operation == NULL)  
{       
	sites[site_No].lock_Entries[varNo].first_blocked_operation=node;
	node->nextOperationSite = NULL;
} 
else
{
struct operation *current = sites[site_No].lock_Entries[varNo].first_blocked_operation;
while(current->nextOperationSite != NULL)
	current=current->nextOperationSite;

current->nextOperationSite=node;
node->nextOperationSite=NULL;
}

}
/**************************************************************************************************************************
					     Function addToBlockedList Ends
******************************************************************************************************************************/
/******************************************************************************************************************************
					     Function checkLockIsNecessary Starts
			     It checks whether the transaction requires a lock on the data item or not 
			     by checking whether the transaction has same or higher lock.
			     If the transaction requires a lock, it returns 0. Otherwise it returns 1. 
			     
********************************************************************************************************************************/

int checkLockIsNecessary (int site_No,int varNo,int tid,int lock_Mode)
{
		struct operation *first=sites[site_No].lock_Entries[varNo].first_active_operation ;
                if(first != NULL )
		{
		  if(first->tid!=tid)  
		  {
			return 0;
		  }		
		  else
		  {
			if(first->operationType >= lock_Mode)   
				return 1;
			else
			   return 0; 
			     
		  }
               }
               return 0 ;
		
}
/*********************************************************************************************************************************
						Function checkLockIsNecessary Ends
************************************************************************************************************************************/
/*********************************************************************************************************************************
						Function checkConflictAndDeadlockPrevention Starts
checkConflictAndDeadlockPrevention checks if transaction tid’s access on data item ‘var’ conflicts with any other transaction in conflicting mode.
Returns 0 of is there is no conflict and therefore lock can be granted; 1 if requesting tid should be blocked ; -1 if requesting tid should be aborted

************************************************************************************************************************************/


int checkConflictAndDeadlockPrevention(int site_No,int varNo,int tid,int timestamp,int operationType)
{
		char log_desc[1000];
		int found=0;
                struct operation *first=sites[site_No].lock_Entries[varNo].first_active_operation;
	        	

if(first ==NULL)
{        
 	return 0; 		
}
else
{



//If the same transaction is requesting;No Conflict;This will happen if transaction has a read lock on variable but can't get the lock 
	       if(first->nextOperationSite == NULL)
		if(first->tid == tid) //This function is called only when the transaction had a low strength lock;so block the request
		 {      
			return 0;
		 }
		
		if((first->operationType == operationType) && (operationType==READ_OPERATION))// No conflict if lock req is for read; current is read
		 {
			return 2;    /// This indicates that variable is to be appended to the ;; Change this
		 }    	
		 else		//Compare tids for deadlock prevention 
		 {
		        
			while(first!=NULL)
			{
			   if(timestamp > first->transactionTimestamp)   // If Younger one is requesting;Kill it
			     { 
                          sprintf(log_desc,"At Site %d: tid %d timestamp %d rejected in favor of tid %d timestamp %d has lock on varNo %d due to wait-die\n", site_No, tid, timestamp, first->tid, first->transactionTimestamp, first->varNo) ;
			  logString(log_desc) ;
			      found=1;
			      return -1; 
			     }
			if(first->nextOperationSite==NULL)
			   break;
			first=first->nextOperationSite;
			}
			//printf("\n Timestamp of %d;TID:%d Timestamp of %d;TID:%d",timestamp,tid,first->transactionTimestamp,first->tid);

			if(found==0) {                  // If older one is requesting;block it  
                          sprintf(log_desc,"At Site %d: tid %d timestamp %d blocked since tid %d timestamp %d has the lock on varNo %d\n", site_No, tid, timestamp, first->tid, first->transactionTimestamp, first->varNo) ;
			  logString(log_desc);
			  return 1;
                        }


		 }
		
    }	 
}
/*********************************************************************************************************************************
						Function checkConflictAndDeadlockPrevention Ends
************************************************************************************************************************************/

/***********************************************************************************************************************************
						 Function doDump Starts		 
				It performs dump operation on the values passed from perfomOperation function.
***********************************************************************************************************************************/
void doDump(struct operation *op, int site_No)
{
char log_desc[1000];
int j,temp;
if(op->varNo==ALL_VARIABLES)       
{
 sprintf(log_desc,"Variables at Site:%d\n",site_No);
 logString(log_desc);
 for(j=1;j<MAX_VARIABLES;j++)
  {
    if(sites[site_No].variable[j].flag==1) {
	temp=readonly_Versiontable(site_No,j,op->transactionTimestamp);
	sprintf(log_desc," x%d: %d",j,temp);
	logString(log_desc);
	}
  }
}
else				 
{  
  sprintf(log_desc,"\nSite:%d x%d: %d",site_No,op->varNo,readonly_Versiontable(site_No,op->varNo,op->transactionTimestamp));	
  logString(log_desc);
}
sprintf(log_desc,"\n") ;
logString(log_desc);
}
/***********************************************************************************************************************************
						 Function doDump Ends
***********************************************************************************************************************************/
/***********************************************************************************************************************************
						 Function printActiveandBlockedList Starts
					It prints the active and blocked lists of all sites.	 
************************************************************************************************************************************/
void printActiveandBlockedList(struct operation *op, int site_No)
{

int j;
int flagListEmpty = 1 ;
char log_desc[1000];

sprintf(log_desc,"\n********Active List at Site:%d**********",site_No);
logString(log_desc);

for(j=1;j<MAX_VARIABLES;j++)
  {
    if(sites[site_No].variable[j].flag==1) {
		
		struct operation *first=sites[site_No].lock_Entries[j].first_active_operation;
		if(first!=NULL)
		{
                        if(flagListEmpty == 1) {
                           flagListEmpty = 0 ;
                        }
                        sprintf(log_desc,"\nVarNo:%d Transaction ID(s) holding the lock:",j);
			logString(log_desc);
			while(first!=NULL)
			{
                                if(first->operationType == READ_OPERATION) {
				  sprintf(log_desc,"TID %d lock type read; ",first->tid);
                                }
                                else if(first->operationType == WRITE_OPERATION) {
				  sprintf(log_desc,"TID %d lock type write; ",first->tid);
                                }
				logString(log_desc);
				first=first->nextOperationSite;
			}		  
		}
	}
  }
if(flagListEmpty == 1) {
  sprintf(log_desc,"\nList is empty\n") ;
  logString(log_desc);
}
flagListEmpty = 1 ;
sprintf(log_desc,"\n\n******Blocked List at Site:%d*********",site_No);
logString(log_desc);

for(j=1;j<MAX_VARIABLES;j++)
  {
    if(sites[site_No].variable[j].flag==1) {
		struct operation *first=sites[site_No].lock_Entries[j].first_blocked_operation;
		if(first!=NULL)
		{
                        if(flagListEmpty == 1) {
                           flagListEmpty = 0 ;
                        }
                        sprintf(log_desc,"\nVarNo:%d Transaction ID(s) waiting the lock:",j);
			logString(log_desc);
			while(first!=NULL)
			{
                                if(first->operationType == READ_OPERATION) {
				  sprintf(log_desc,"TID %d lock type read; ",first->tid);
                                }
                                else if(first->operationType == WRITE_OPERATION) {
				  sprintf(log_desc,"TID %d lock type write; ",first->tid);
                                }
				
				logString(log_desc);
				first=first->nextOperationSite;
			}		  
		}
	}
  }
if(flagListEmpty == 1) {
  sprintf(log_desc,"\nList is empty\n") ;
  logString(log_desc);
}
sprintf(log_desc,"\n") ;
logString(log_desc);


}

/***********************************************************************************************************************************
						 Function printActiveandBlockedList Ends	
************************************************************************************************************************************/




/***********************************************************************************************************************************
						 Function performOperation Starts
		It first checks whether the site is down or not. If the site is down, it rejects the operation.
		It then checks the transaction type and performs the operation accordingly.
************************************************************************************************************************************/

void performOperation(struct operation *op, int site_No)
{

char log_desc[1000];

if((availableSites[site_No]==0)  && op->operationType != RECOVER_OPERATION ) 
{
	op->operationStatusAtSites[site_No]=OPERATION_REJECTED;
	return;
}

if(op->transactionType == READONLY_TRANSACTION )
{
	      if(op->operationType==READ_OPERATION) 				   	
	      {
		if(checkReadAvailability(site_No,op->varNo,op->tid) == 0) 			   
		{
			   op->operationStatusAtSites[site_No]=OPERATION_REJECTED;
			   sprintf(log_desc,"Rejecting read for tid %d on var %d @ site %d because site has just recovered\n", op->tid, op->varNo, site_No) ;
			   logString(log_desc);	
			   return;
		}
		else
		{
			   op->valueRead=readonly_Versiontable(site_No,op->varNo,op->transactionTimestamp);		
			   op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;
			   return;
		}
	      }
}



if(op->transactionType == READWRITE_TRANSACTION )
{

	      if(op->operationType==READ_OPERATION) 				   	
	      	{
		 if(checkReadAvailability(site_No,op->varNo,op->tid) == 0) 		   
			{			
			   op->operationStatusAtSites[site_No]=OPERATION_REJECTED;
                           sprintf(log_desc,"Rejecting read for tid %d on var %d @ site %d because site has just recovered\n", op->tid, op->varNo, site_No) ;
			   logString(log_desc);	
          		   return;
			}	
	
		}
	
	     		
		if(op->operationType==READ_OPERATION || op->operationType== WRITE_OPERATION)
		{
		if(checkLockIsNecessary(site_No,op->varNo,op->tid,op->operationType) == 1)  // If the transaction already has the stronger lock
			{  UpdateVersionTable(site_No,op->varNo,op);
			   op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;  // Lock Granted;
			   return;
			}   
		
               	
		int request=checkConflictAndDeadlockPrevention(site_No,op->varNo,op->tid,op->transactionTimestamp,op->operationType);
		
		if((request==0) || (request==2)) //Lock Granted,add it to active list
		  {	
			addToActiveList(site_No,op->varNo,op,request);
			UpdateVersionTable(site_No,op->varNo,op);
			op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;
			return;
		  }
		else if(request==1)
		  { 	
			addToBlockedList(site_No,op->varNo,op);
			op->operationStatusAtSites[site_No]=OPERATION_BLOCKED;
			return;
		  }
		else    
		  {
			op->operationStatusAtSites[site_No]=OPERATION_REJECTED;
			return;
		  }		
		}
	if(op->operationType==END_OPERATION)
	{			
		UpdateVersionTable(site_No,-1,op);        
		releaseLocks(site_No,op->tid);		  
		op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;
		return;
	}

	if(op->operationType == ABORT_OPERATION)
	{
		releaseLocks(site_No,op->tid);
		op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;
		return;
	}
	
}		


if(op->transactionType == MISCELLANEOUS_TRANSACTION)
{
	if(op->operationType==FAIL_OPERATION)		
	{       
  		sprintf(log_desc,"Site Failed: %d\n",site_No);
		logString(log_desc);	
		availableSites[site_No]=0;		
		siteFail(site_No);                      
		op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;
		return;
	}

	if(op->operationType==RECOVER_OPERATION)		
	{
		int j;
		availableSites[site_No]=1;		
		sprintf(log_desc,"Site Recovered: %d\n",site_No);
		logString(log_desc);	
		for(j=1;j<MAX_VARIABLES;j++)	
		{	
                   if(sites[site_No].variable[j].flag==1) {
		     if(j%2==0)
		      {		
                	readEnableDisable(site_No,j,0);
		      }
		      else
		      {
			readEnableDisable(site_No,j,1);	
		      }					
                  }
                }
		op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;
		return;
	}	
	if(op->operationType == DUMP_OPERATION)
	{
		doDump(op,site_No);	
		op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;
		return;	
	}
	if(op->operationType == QUERY_STATE_OPERATION)
	{
		doDump(op,site_No);
		printActiveandBlockedList(op,site_No);
		op->operationStatusAtSites[site_No]=OPERATION_COMPLETE;
		return;
	}
					
	
}




}

/***********************************************************************************************************************************
						 Function performOperation Ends	
************************************************************************************************************************************/
/***********************************************************************************************************************************
						 Function releaseLocks Starts
			 It releases all the Locks after checking whether the Lock is in Active List or Blocked List.
************************************************************************************************************************************/


void releaseLocks(int site_No,int tid)
{
int j;
	for(j=1;j<=MAX_VARIABLES;j++)
	{
		if(sites[site_No].lock_Entries[j].flag==1)  
		{
			

			
	     			struct operation *node=sites[site_No].lock_Entries[j].first_active_operation;
				if(node!=NULL)
				{
				if(node->tid==tid)
				 {
				  sites[site_No].lock_Entries[j].first_active_operation=NULL;    
				  if(sites[site_No].lock_Entries[j].first_blocked_operation!=NULL) 
				   {	
					sites[site_No].lock_Entries[j].first_active_operation=sites[site_No].lock_Entries[j].first_blocked_operation;
					sites[site_No].lock_Entries[j].first_blocked_operation=sites[site_No].lock_Entries[j].first_blocked_operation->nextOperationSite;
					performOperation(sites[site_No].lock_Entries[j].first_active_operation, site_No);   //Perform the operation 
				   }
				 }
				}
		
				struct operation *current=sites[site_No].lock_Entries[j].first_blocked_operation;
				if(current!=NULL)
				{
				  if(current->nextOperationSite==NULL)  
			 	     if(current->tid==tid)
					   sites[site_No].lock_Entries[j].first_blocked_operation=NULL;
				  else             
				  {
				     	struct operation *previous=current;
					while(current!=NULL)
					{
						if(current->tid==tid)
						 {   
						     current=current->nextOperationSite;	
						     previous->nextOperationSite=current;
						     previous=current;
						 } 
						 else
					 	 {
						     previous=current;
						     current=current->nextOperationSite;
						 }  
					} 

				  }	           													   					
			       }
				   
			
		}		

	}

}

/***********************************************************************************************************************************
						 Function releaseLocks Ends
************************************************************************************************************************************/

/***********************************************************************************************************************************
						Function initializeSiteData Starts
				             It initializes all sites and  variables. 
************************************************************************************************************************************/

void initializeSiteData()
{
int i,j,k;
for(i=1;i<=(MAX_SITES-1);i++)
{  
	  for(j=1;j<=(MAX_VARIABLES-1);j++)
             { 
                  if(j%2==0)
                      { 
			
			struct version *newNode = (struct version *) malloc (sizeof(struct version));
                        sites[i].variable[j].flag=1;
			newNode->tid=-1;
                        newNode->value=10*j;
 			newNode->R_Timestamp=0;
			newNode->W_Timestamp=0;
			newNode->next=NULL;
			sites[i].variable[j].head=newNode;
			sites[i].lock_Entries[j].flag=1;
			sites[i].lock_Entries[j].readAvailable=1; 
					

		      }
                  else
		      {
			if(i==((j%10)+1))
			{
			struct version *newNode = (struct version *) malloc (sizeof(struct version));
                        sites[i].variable[j].flag=1;
			newNode->tid=-1;
                        newNode->value=10*j;
 			newNode->R_Timestamp=0;
			newNode->W_Timestamp=0;
			newNode->next=NULL;
			sites[i].variable[j].head=newNode;

			sites[i].lock_Entries[j].flag=1;
			sites[i].lock_Entries[j].readAvailable=1; 
			}
		      }			

             }
    

}

int siteNo ;
for(siteNo = 1; siteNo < MAX_SITES; siteNo++) {
  availableSites[siteNo] = 1 ;  

}
}

/***********************************************************************************************************************************
						 Function initializeSiteData Ends
************************************************************************************************************************************/


