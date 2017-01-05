import sys
import mysql.connector
import argparse

class MigrationTool(object):
    
    def __init__(self, db_host, db_user, db_password, database_name, admin_handle, key_file, output_batch_filename):
        super(MigrationTool, self).__init__()
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.database_name = database_name
        self.admin_handle = admin_handle
        self.key_file = key_file
        self.output_batch_filename = output_batch_filename
        self.all_handles = []
        
    def execute(self):
        self.connection = mysql.connector.connect(user=self.db_user, database=self.database_name, host=self.db_host, password=self.db_password)
        self.collect_all_handles()
        self.migrate_handles()
        
    def collect_all_handles(self):
        cursor_all_handles = self.connection.cursor()
        query_all_handles = "SELECT distinct handle FROM handles LIMIT 100"
        cursor_all_handles.execute(query_all_handles)
        for handle_name in cursor_all_handles:
            self.all_handles.append(handle_name)
            
    def retrieve_handle_record(self, handle_name):
        """
        Retrieve and return a handle record, already cleaned up (not containing HS specific fields).
        The returned record format is a dictionary with Handle indexes as keys pointing to (type, value) tuples.
        """
        query = ("SELECT idx, type, data FROM handles WHERE handle = %s")
        self.cursor.execute(query, (handle_name))
        handle_record = {}
        for handlevalues in self.cursor:
            handle_idx = int(handlevalues[0])
            handle_type = str(handlevalues[1])
            handle_value = str(handlevalues[2])
            if handle_type in ("HS_ADMIN", "HS_SITE", "HS_PUBKEY", "HS_SECKEY", "HS_ALIAS", "HS_VLIST", "HS_SERV"):
                continue
            handle_record[handle_idx] = (handle_type, handle_value)
        return handle_record

    def write_authentication_info(self, batch_file, admin_handle, key_file):
        batch_file.write("AUTHENTICATE PUBKEY:%s\n" % admin_handle)
        batch_file.write("%s\n" % key_file)
            
    def migrate_handles(self):
        self.cursor = self.connection.cursor()
        batch_file = open(self.output_batch_filename, "w")
        self.write_authentication_info(batch_file, self.admin_handle, self.key_file)
        try:
            for handle_name in self.all_handles:
                handle_record = self.retrieve_handle_record(handle_name)
                # Lists with action statements
                st_modify = []
                st_remove = []
                st_add = []
                # -- Analyse old record, append modification actions --
                # TODO: these are just toy examples for now - need to replace with actual EUDAT actions!
                for h_idx, (h_type, h_value) in handle_record.iteritems():
                    if h_type == "URL":
                        st_modify.append("%s URL 86400 1110 UTF8 %s" % (h_idx, h_value+"test"))
                    if h_type == "VERSION_NUMBER":
                        st_modify.append("%s VERSION_NUMBER 86400 1110 %s" % (h_idx, int(h_value)+1))
                # Execute queued statements
                if st_remove:
                    for l in st_remove:
                        batch_file.write("REMOVE %s\n" % l)
                if st_modify:
                    batch_file.write("MODIFY %s\n" % handle_name)
                    for l in st_modify:
                        batch_file.write(l+"\n")
                if st_add:
                    batch_file.write("ADD %s\n" % handle_name)
                    for l in st_add:
                        batch_file.write(l+"\n")
        finally:
            batch_file.close()
                

if __name__ == "__main__":
    # argparse
    parser = argparse.ArgumentParser(description="Tool to migrate Handle records to the EUDAT PID Profile v1.")
    parser.add_argument("handleuser", help="Handle user (index:handle)")
    parser.add_argument("handlekey", help="Handle private key file")
    parser.add_argument("databasehost", help="SQL database server host")
    parser.add_argument("databaseuser", help="SQL database server user name")
    parser.add_argument("databasepassword", help="SQL database server user password")
    parser.add_argument("databasename", help="SQL database name on server")
    parser.add_argument("outputfile", help="Output batch file name")
    
    args = parser.parse_args()
    
    migration_tool = MigrationTool(args.databasehost, args.databaseuser, args.databasepassword, args.databasename,
                                   args.handleuser, args.handlekey, args.outputfile)
    migration_tool.execute()