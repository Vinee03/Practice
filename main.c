#include "globalvar.h"
#include "site.h"
#include "transaction_manager_struct.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
  char inputFile[100];
  int ret ;
  char log_desc[1000] ;
  
  if(argc != 2) {
    printf("Main Function : Check the input file path %s", argv[0]) ;
    return  0 ;
  }
  strcpy(inputFile, argv[1]) ;
  ret = checkFileExists(inputFile) ;
  if(ret == -1) {
   printf("Main function: File %s does not exist or is empty", inputFile) ;
   return  0 ;
  }
  FILE *fp = fopen("logfile.log", "w") ;
  if(fp == NULL) {
    printf("Main function: failed to open log file in write mode. Error: %s\n", (char *)strerror(errno)) ;
  }
  else {
    fclose(fp) ;
  }
  initializeTransactionManager() ;
  initializeSiteData() ;
  ret = parseInput(inputFile) ;
  if(ret == -1) {
   printf("main: parseInput could not parse file %s. Exiting\n", inputFile) ;
   return  0 ;
  }
  startTransactionManager() ;
  sprintf(log_desc, "main: Transaction Manager exiting\n") ;
  logString(log_desc) ;
  return  0 ;
}
/**************************************************************************************************************************
                                              Function checkFileExists Starts
                            It checks whether the file exists or not. It returns error if the file doesn't exist.  
****************************************************************************************************************************/
int checkFileExists(char *fname) {
  int ret;
  struct  stat _curr ;
  ret = lstat(fname, &_curr) ;

  if (ret == -1) {
   printf("checkFileExists: lstat error for file %s: %s\n", fname, (char *)strerror(errno)) ;
   return -1 ;
  }
  if(_curr.st_size == 0){
   printf("checkFileExists: empty file %s\n", fname);
   return -1 ;
  }
  return 0 ;
}
/**************************************************************************************************************************
                                             Function checkFileExists Ends 
*********************************************************************************************************************************/
/*********************************************************************************************************************************
                                              Function logString Starts
                                              Failed to open log file            
**********************************************************************************************************************************/
void logString(char * log_desc) {

   FILE *fp = fopen("logfile.log", "a") ;
   printf("%s", log_desc) ;
   if(fp == NULL) {
     printf("logString: failed to open log file in append mode. Error: %s\n", (char *)strerror(errno)) ;
     return ;
   }
   fprintf(fp, "%s", log_desc) ;
   fclose(fp) ;
}
/**************************************************************************************************************************
                                             Function logString Ends 
*********************************************************************************************************************************/
