#include <stdio.h>
#include <gdbm.h>
#include <stdlib.h>
#include <string>
#include "qconf_format.h"
using namespace std;
int main(int argc, char const *argv[])
{
	if(argc != 2){
		printf("%s\n","usage : gdbm_dump filename");
		return -1;
	}
	const char *dbname = argv[1];
	GDBM_FILE dbf = gdbm_open (dbname, 0, GDBM_READER|GDBM_NOLOCK, 00664, NULL);
	if(!dbf){
        printf("Failed to open gdbm file:%s; gdbm err:%s\n", 
                dbname, gdbm_strerror(gdbm_errno));
        return -1;
	}
	datum key;
	datum data;
	key = gdbm_firstkey (dbf);
	while (key.dptr){

      datum nextkey = gdbm_nextkey (dbf, key);

      string tbl_key ;
      tbl_key.assign(key.dptr, key.dsize);
      char data_type;
      string idc, path ,value;
      deserialize_from_tblkey(tbl_key, data_type, idc, path);
      if(path.size() == 0){
      	free (key.dptr);
      	key = nextkey;
      	continue;
      }
      fwrite (path.c_str(), path.size(), 1, stdout);
      fwrite(":",1,1,stdout);
      data = gdbm_fetch (dbf, key);
      
      string tbl_val;
      tbl_val.assign(data.dptr, data.dsize);
      tblval_to_nodeval(tbl_val,value);

      fwrite (value.c_str(), value.size(), 1, stdout);
      
      free (data.dptr);
      fputc ('\n', stdout);
      free (key.dptr);
      key = nextkey;
  	}
	return 0;
}
