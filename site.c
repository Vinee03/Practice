#include <stdio.h>
#include <stdlib.h>

#include "site.h"

int availableSites[MAXIMUM_SITES] ; //If the given site is available, the array stores value '1'. If the given site is unavailable, it stores the value '0'.
void releaseLocks(int site_No,int trnid);

/********************************************************************************************************************************
						 Function readEnableDisable Starts	
			Enable read for committed if the variable is commited and disable read for all 
			                      variables in the site when a site recovers from failure. 
                                                1 is used to Enable and 0 is used to Disable
************************************************************************************************************************************/
void readEnableDisable(int site_No,int variablenumber,int enable)
{
	sites[site_No].lock_Entries[variablenumber].readAvailable = enable; 
}
/******************************************************************************************************************************
                                                 Function readEnableDisable Ends
*********************************************************************************************************************************/

/**********************************************************************************************************************************
                                                 Function checkReadAvailability Starts
				   It checks whether the variable is available for read or not. 
				   If the variable variablenumber is available to read it returns 1 otherwise it returns 0. 
***********************************************************************************************************************************/
int checkReadAvailability(int site_No,int variablenumber,int trnid)
{

struct operation *current = sites[site_No].lock_Entries[variablenumber].first_active_operation;
if(current!=NULL)
{
	if(current->trnid==trnid && current->opnType==WRITE_OPN)  
	{
		return 1;  
	}
}

return sites[site_No].lock_Entries[variablenumber].readAvailable;
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
	        int readValue=0;
                if(sites[site_No].variable[var].flag==1)
                {
			struct version* current = sites[site_No].variable[var].head;
			int largest=-1;
		    
 		while(current!=NULL)
			 {
				if(current->W_Timestamp<=timestamp && current->W_Timestamp>largest)
				{
					largest=current->W_Timestamp;
					readValue=current->value;
				}   
				current=current->next;
			 } 

		}
		


return readValue;
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
	for(j=1;j<=MAXIMUM_VARIABLES;j++)
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
void UpdateVersionTable(int site_No,int variablenumber,struct operation *op)
{
   int j;
  if(variablenumber>0)
{ 
   if(sites[site_No].variable[variablenumber].flag==1)
     {
	    if(op->opnType==WRITE_OPN)     
	       { 	
		 	struct version* current = sites[site_No].variable[variablenumber].head;
	         	int exist=0;
		     	while(current!=NULL)
			 {
				if((op->trnid==current->trnid)&&(current->W_Timestamp==MAXIMUM_TRN_TIMESTAMP))
				{
					current->value=op->writtenValue;
					exist=1;
				}   
				current=current->next;
			 } 

		
			if(exist==0)
			{
				struct version *newNode = (struct version *) malloc (sizeof(struct version));
				newNode->trnid=op->trnid;
                       		newNode->value=op->writtenValue;
 				newNode->R_Timestamp=MAXIMUM_TRN_TIMESTAMP;
				newNode->W_Timestamp=MAXIMUM_TRN_TIMESTAMP;
				newNode->next=sites[site_No].variable[variablenumber].head;
				sites[site_No].variable[variablenumber].head=newNode;	
			}
                }

		if(op->opnType==READ_OPN)   
		{
		 	struct version* current = sites[site_No].variable[variablenumber].head;
	         	int exist=0;
		     	while(current!=NULL)
			 {
				if((op->trnid==current->trnid)&&(current->W_Timestamp==MAXIMUM_TRN_TIMESTAMP))
				{
					op->readValue=current->value;  
					exist=1;
				}   
				current=current->next;
			 }
			 
			 if(exist==0)  
			 {
			    op->readValue=readonly_Versiontable(site_No,variablenumber,op->trnTimestamp);   
			 }
			 

	        }
     }   
}
     if(op->opnType==END_OPN)  
      { 
		
	   for(j=1;j<=MAXIMUM_VARIABLES;j++)
             { 
                if(sites[site_No].variable[j].flag==1)
                {
			struct version* current = sites[site_No].variable[j].head;
		     if(current!=NULL)
		     {
		     	while(current!=NULL)
			 {
				if((op->trnid==current->trnid)&&(current->W_Timestamp==MAXIMUM_TRN_TIMESTAMP))
				{
					current->W_Timestamp=op->trnTimestamp;
					current->R_Timestamp=op->trnTimestamp;    
					if(checkReadAvailability(site_No,j,op->trnid)==0) 
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

void addToActiveList(int site_No,int variablenumber,struct operation *node,int request)
{

if(request==0)    
{
	sites[site_No].lock_Entries[variablenumber].first_active_operation = node;
	node->opnSite = NULL;
}
else
{
	struct operation *current = sites[site_No].lock_Entries[variablenumber].first_active_operation;
	while(current->opnSite != NULL)
		current=current->opnSite;

	current->opnSite=node;
	node->opnSite=NULL;
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


void addToBlockedList(int site_No,int variablenumber,struct operation *node)
{

if(sites[site_No].lock_Entries[variablenumber].first_blocked_operation == NULL)  
{       
	sites[site_No].lock_Entries[variablenumber].first_blocked_operation=node;
	node->opnSite = NULL;
} 
else
{
struct operation *current = sites[site_No].lock_Entries[variablenumber].first_blocked_operation;
while(current->opnSite != NULL)
	current=current->opnSite;

current->opnSite=node;
node->opnSite=NULL;
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

int checkLockIsNecessary (int site_No,int variablenumber,int trnid,int lock_Mode)
{
		struct operation *first=sites[site_No].lock_Entries[variablenumber].first_active_operation ;
                if(first != NULL )
		{
		  if(first->trnid!=trnid)  
		  {
			return 0;
		  }		
		  else
		  {
			if(first->opnType >= lock_Mode)   
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
checkConflictAndDeadlockPrevention checks if transaction trnid’s access on data item ‘var’ conflicts with any other transaction in conflicting mode.
Returns 0 of is there is no conflict and therefore lock can be granted; 1 if requesting trnid should be blocked ; -1 if requesting trnid should be aborted

************************************************************************************************************************************/


int checkConflictAndDeadlockPrevention(int site_No,int variablenumber,int trnid,int timestamp,int opnType)
{
		char log_desc[1000];
		int found=0;
                struct operation *first=sites[site_No].lock_Entries[variablenumber].first_active_operation;
	        	

if(first ==NULL)
{        
 	return 0; 		
}
else
{



//If the same transaction is requesting;No Conflict;This will happen if transaction has a read lock on variable but can't get the lock 
	       if(first->opnSite == NULL)
		if(first->trnid == trnid) //This function is called only when the transaction had a low strength lock;so block the request
		 {      
			return 0;
		 }
		
		if((first->opnType == opnType) && (opnType==READ_OPN))// No conflict if lock req is for read; current is read
		 {
			return 2;    /// This indicates that variable is to be appended to the ;; Change this
		 }    	
		 else		//Compare trnids for deadlock prevention 
		 {
		        
			while(first!=NULL)
			{
			   /*if(timestamp > first->trnTimestamp)   // If Younger one is requesting;Kill it
			     { 
                          sprintf(log_desc,"At Site %d: trnid %d timestamp %d rejected in favor of trnid %d timestamp %d has lock on variablenumber %d due to wait-die\n", site_No, trnid, timestamp, first->trnid, first->trnTimestamp, first->variablenumber) ;
			  logString(log_desc) ;
			      found=1;
			      return -1; 
			     }*/
			if(first->opnSite==NULL)
			   break;
			first=first->opnSite;
			}
			//printf("\n Timestamp of %d;TRNID:%d Timestamp of %d;TRNID:%d",timestamp,trnid,first->trnTimestamp,first->trnid);

			if(found==0) {                  // If older one is requesting;block it  
                          sprintf(log_desc,"At Site %d: trnid %d timestamp %d blocked since trnid %d timestamp %d has the lock on variablenumber %d\n", site_No, trnid, timestamp, first->trnid, first->trnTimestamp, first->variablenumber) ;
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
if(op->variablenumber==ALL_VARIABLES)       
{
 sprintf(log_desc,"Variables at Site:%d\n",site_No);
 logString(log_desc);
 for(j=1;j<MAXIMUM_VARIABLES;j++)
  {
    if(sites[site_No].variable[j].flag==1) {
	temp=readonly_Versiontable(site_No,j,op->trnTimestamp);
	sprintf(log_desc," x%d: %d",j,temp);
	logString(log_desc);
	}
  }
}
else				 
{  
  sprintf(log_desc,"\nSite:%d x%d: %d",site_No,op->variablenumber,readonly_Versiontable(site_No,op->variablenumber,op->trnTimestamp));	
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

for(j=1;j<MAXIMUM_VARIABLES;j++)
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
                                if(first->opnType == READ_OPN) {
				  sprintf(log_desc,"TRNID %d lock type read; ",first->trnid);
                                }
                                else if(first->opnType == WRITE_OPN) {
				  sprintf(log_desc,"TRNID %d lock type write; ",first->trnid);
                                }
				logString(log_desc);
				first=first->opnSite;
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

for(j=1;j<MAXIMUM_VARIABLES;j++)
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
                                if(first->opnType == READ_OPN) {
				  sprintf(log_desc,"TRNID %d lock type read; ",first->trnid);
                                }
                                else if(first->opnType == WRITE_OPN) {
				  sprintf(log_desc,"TRNID %d lock type write; ",first->trnid);
                                }
				
				logString(log_desc);
				first=first->opnSite;
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

if((availableSites[site_No]==0)  && op->opnType != RECOVER_OPN ) 
{
	op->operationStatusAtSites[site_No]=OPN_REJECTED;
	return;
}

if(op->trnType == RONLY_TRANSACTIONS )
{
	      if(op->opnType==READ_OPN) 				   	
	      {
		if(checkReadAvailability(site_No,op->variablenumber,op->trnid) == 0) 			   
		{
			   op->operationStatusAtSites[site_No]=OPN_REJECTED;
			   sprintf(log_desc,"Rejecting read for trnid %d on var %d @ site %d because site has just recovered\n", op->trnid, op->variablenumber, site_No) ;
			   logString(log_desc);	
			   return;
		}
		else
		{
			   op->readValue=readonly_Versiontable(site_No,op->variablenumber,op->trnTimestamp);		
			   op->operationStatusAtSites[site_No]=OPN_COMPLETE;
			   return;
		}
	      }
}



if(op->trnType == READ_WRITE_TRANSACTIONS )
{

	      if(op->opnType==READ_OPN) 				   	
	      	{
		 if(checkReadAvailability(site_No,op->variablenumber,op->trnid) == 0) 		   
			{			
			   op->operationStatusAtSites[site_No]=OPN_REJECTED;
                           sprintf(log_desc,"Rejecting read for trnid %d on var %d @ site %d because site has just recovered\n", op->trnid, op->variablenumber, site_No) ;
			   logString(log_desc);	
          		   return;
			}	
	
		}
	
	     		
		if(op->opnType==READ_OPN || op->opnType== WRITE_OPN)
		{
		if(checkLockIsNecessary(site_No,op->variablenumber,op->trnid,op->opnType) == 1)  
			{  UpdateVersionTable(site_No,op->variablenumber,op);
			   op->operationStatusAtSites[site_No]=OPN_COMPLETE;  
			   return;
			}   
		
               	
		int request=checkConflictAndDeadlockPrevention(site_No,op->variablenumber,op->trnid,op->trnTimestamp,op->opnType);
		
		if((request==0) || (request==2)) 
		  {	
			addToActiveList(site_No,op->variablenumber,op,request);
			UpdateVersionTable(site_No,op->variablenumber,op);
			op->operationStatusAtSites[site_No]=OPN_COMPLETE;
			return;
		  }
		else if(request==1)
		  { 	
			addToBlockedList(site_No,op->variablenumber,op);
			op->operationStatusAtSites[site_No]=OPN_BLOCKED;
			return;
		  }
		else    
		  {
			op->operationStatusAtSites[site_No]=OPN_REJECTED;
			return;
		  }		
		}
	if(op->opnType==END_OPN)
	{			
		UpdateVersionTable(site_No,-1,op);        
		releaseLocks(site_No,op->trnid);		  
		op->operationStatusAtSites[site_No]=OPN_COMPLETE;
		return;
	}

	if(op->opnType == ABORT_OPN)
	{
		releaseLocks(site_No,op->trnid);
		op->operationStatusAtSites[site_No]=OPN_COMPLETE;
		return;
	}
	
}		


if(op->trnType == OTHER_TRANSACTIONS)
{
	if(op->opnType==FAIL_OPN)		
	{       
  		sprintf(log_desc,"Site Failed: %d\n",site_No);
		logString(log_desc);	
		availableSites[site_No]=0;		
		siteFail(site_No);                      
		op->operationStatusAtSites[site_No]=OPN_COMPLETE;
		return;
	}

	if(op->opnType==RECOVER_OPN)		
	{
		int j;
		availableSites[site_No]=1;		
		sprintf(log_desc,"Site Recovered: %d\n",site_No);
		logString(log_desc);	
		for(j=1;j<MAXIMUM_VARIABLES;j++)	
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
		op->operationStatusAtSites[site_No]=OPN_COMPLETE;
		return;
	}	
	if(op->opnType == DUMP_OPN)
	{
		doDump(op,site_No);	
		op->operationStatusAtSites[site_No]=OPN_COMPLETE;
		return;	
	}
	if(op->opnType == QUERY_STATE_OPN)
	{
		doDump(op,site_No);
		printActiveandBlockedList(op,site_No);
		op->operationStatusAtSites[site_No]=OPN_COMPLETE;
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


void releaseLocks(int site_No,int trnid)
{
int j;
	for(j=1;j<=MAXIMUM_VARIABLES;j++)
	{
		if(sites[site_No].lock_Entries[j].flag==1)  
		{
			

			
	     			struct operation *node=sites[site_No].lock_Entries[j].first_active_operation;
				if(node!=NULL)
				{
				if(node->trnid==trnid)
				 {
				  sites[site_No].lock_Entries[j].first_active_operation=NULL;    
				  if(sites[site_No].lock_Entries[j].first_blocked_operation!=NULL) 
				   {	
					sites[site_No].lock_Entries[j].first_active_operation=sites[site_No].lock_Entries[j].first_blocked_operation;
					sites[site_No].lock_Entries[j].first_blocked_operation=sites[site_No].lock_Entries[j].first_blocked_operation->opnSite;
					performOperation(sites[site_No].lock_Entries[j].first_active_operation, site_No);   //Perform the operation 
				   }
				 }
				}
		
				struct operation *current=sites[site_No].lock_Entries[j].first_blocked_operation;
				if(current!=NULL)
				{
				  if(current->opnSite==NULL)  
			 	     if(current->trnid==trnid)
					   sites[site_No].lock_Entries[j].first_blocked_operation=NULL;
				  else             
				  {
				     	struct operation *previous=current;
					while(current!=NULL)
					{
						if(current->trnid==trnid)
						 {   
						     current=current->opnSite;	
						     previous->opnSite=current;
						     previous=current;
						 } 
						 else
					 	 {
						     previous=current;
						     current=current->opnSite;
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
for(i=1;i<=(MAXIMUM_SITES-1);i++)
{  
	  for(j=1;j<=(MAXIMUM_VARIABLES-1);j++)
             { 
                  if(j%2==0)
                      { 
			
			struct version *newNode = (struct version *) malloc (sizeof(struct version));
                        sites[i].variable[j].flag=1;
			newNode->trnid=-1;
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
			newNode->trnid=-1;
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

int sitenumber ;
for(sitenumber = 1; sitenumber < MAXIMUM_SITES; sitenumber++) {
  availableSites[sitenumber] = 1 ;  

}
}

/***********************************************************************************************************************************
						 Function initializeSiteData Ends
************************************************************************************************************************************/


