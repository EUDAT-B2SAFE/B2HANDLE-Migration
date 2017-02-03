import sys
import mysql.connector
import argparse
from datetime import datetime
from b2handle.handleclient import EUDATHandleClient
from xml.etree import ElementTree

INDEX_PROFILE_VERSION = 1000
INDEX_FIXED_CONTENT = 1010
INDEX_CHECKSUM_TIMESTAMP = 1110
INDEX_ROR = 1120
INDEX_FIO = 1130
INDEX_REPLICA = 1140
INDEX_PARENT = 1150

DO_REMOTE_CALLS = True

class MigrationTool(object):
    
    def __init__(self, db_host, db_user, db_password, database_name, admin_handle, output_batch_filename, queried_prefixes, fixed_content, handle_key_file = None, handle_secret_key = None):
        super(MigrationTool, self).__init__()
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.database_name = database_name
        self.admin_handle = admin_handle
        self.key_file = handle_key_file
        self.secret_key = handle_secret_key
        self.output_batch_filename = output_batch_filename
        self.queried_prefixes = queried_prefixes
        if fixed_content:
            self.fixed_content = "TRUE"
        else:
            self.fixed_content = "FALSE"
        self.all_handles = []
        self.b2handleclient = EUDATHandleClient.instantiate_for_read_access()
        
    def execute(self):
        try:
            self.connection = mysql.connector.connect(user=self.db_user, database=self.database_name, host=self.db_host, password=self.db_password)
            self.collect_all_handles()
            print("Total number of Handles collected: %s" % len(self.all_handles))
            self.migrate_handles()
        finally:
            self.connection.close()
        
    def collect_all_handles(self):
        cursor_all_handles = self.connection.cursor()
        for prefix in self.queried_prefixes:
            if not prefix:
                continue
            prefix_like = '"%s/%%"' % prefix
            query_all_handles = "SELECT distinct handle FROM handles where handle like %s" % prefix_like
            cursor_all_handles.execute(query_all_handles)
            for handle_name in cursor_all_handles:
                self.all_handles.append(handle_name[0])
        self.total_number_of_handles = len(self.all_handles)
            
    def retrieve_handle_record(self, handle_name):
        """
        Retrieve and return a handle record via SQL, already cleaned up (not containing HS specific fields).
        The returned record format is a dictionary with Handle indexes as keys pointing to (type, value) tuples.
        """
        query = ('SELECT idx, type, data, timestamp FROM handles WHERE handle = "%s"' % handle_name)
        self.cursor.execute(query)
        handle_record = {}
        for handlevalues in self.cursor:
            handle_idx = int(handlevalues[0])
            handle_type = str(handlevalues[1]).upper()
            handle_value = str(handlevalues[2])
            handle_timestamp = str(handlevalues[3])
            if handle_type in ("HS_ADMIN", "HS_SITE", "HS_PUBKEY", "HS_SECKEY", "HS_ALIAS", "HS_VLIST", "HS_SERV"):
                continue
            handle_record[handle_idx] = (handle_type, handle_value, handle_timestamp)
        return handle_record
    
    def retrieve_handle_record_remotely(self, handle_name):
        """
        Retrieve and return a handle record via HTTP/B2HANDLE, using regular Handle resolution. Use this method
        to retrieve Handles that are suspected to be not present in the database at hand.
        
        Note that the return dict format is different from the format returned by the retrieve_handle_record method!
        """
        return self.b2handleclient.retrieve_handle_record(handle_name)
            

    def write_authentication_info(self, batch_file):
        if self.key_file:
            batch_file.write("AUTHENTICATE PUBKEY:%s\n" % self.admin_handle)
            batch_file.write("%s\n" % self.key_file)
        elif self.secret_key:
            batch_file.write("AUTHENTICATE SECKEY:%s\n" % self.admin_handle)
            batch_file.write("%s\n" % self.secret_key)
        
    @staticmethod
    def __remove_stmt(handle, index):
        if type(handle) == tuple:
            raise Exception("!!!")
        return "%s:%s" % (index, handle)
    
    @staticmethod
    def __add_stmt(index, handletype, value):
        return "%s %s 86400 1110 UTF8 %s" % (index, handletype, value)
    
    @staticmethod
    def __modify_stmt(index, handletype, value):
        return "%s %s 86400 1110 UTF8 %s" % (index, handletype, value)
            
    def migrate_handles(self):
        self.cursor = self.connection.cursor()
        batch_file = open(self.output_batch_filename, "w")
        self.write_authentication_info(batch_file)
        progress_stepping = max(1, len(self.all_handles) / 100)
        progress = 0
        t_start = datetime.now()
        try:
            for handle_name in self.all_handles:
                progress += 1
                if progress % progress_stepping == 0:
                    print("{0:.0%}".format(float(progress)/len(self.all_handles)))
                    if progress == progress_stepping:
                        # print time estimate
                        t_now = datetime.now()
                        t_delta = t_now - t_start
                        print("Migration estimated to take %s - expected to finish at %s" % (t_delta * 100, t_delta * 100 + t_start))
                handle_record = self.retrieve_handle_record(handle_name)
                # 1. Check whether record is actually a B2SAFE Handle and has not been migrated yet
                # (also build helper dicts here)
                helper_value = {}
                helper_index = {}
                for h_idx, (h_type, h_value, h_timestamp) in handle_record.iteritems():
                    helper_value[h_type.upper()] = h_value
                    helper_index[h_type.upper()] = h_idx
                # We assume that every valid old EUDAT record has a CHECKSUM entry
                # This also causes ignoring typical administrative Handles
                if helper_value.get("EUDAT/PROFILE_VERSION") == "1" or not "CHECKSUM" in helper_value:
                    continue
                # Lists with action statements
                st_modify = []
                st_remove = []
                st_add = []
                # -- Analyze old record, append modification actions --
                # CHECKSUM is just transferred to EUDAT/CHECKSUM
                st_modify.append(MigrationTool.__modify_stmt(helper_index["CHECKSUM"], "EUDAT/CHECKSUM", helper_value["CHECKSUM"]))
                # New field EUDAT/CHECKSUM_TIMESTAMP is populated with last modified date of old CHECKSUM field, ISO converted
                timestamp = int(handle_record[helper_index["CHECKSUM"]][2])
                checksum_datetime = datetime.fromtimestamp(timestamp)
                st_add.append(MigrationTool.__add_stmt(INDEX_CHECKSUM_TIMESTAMP, "EUDAT/CHECKSUM_TIMESTAMP", checksum_datetime.isoformat()))
                # FIXED_CONTENT is set according to fixed_content setting
                st_add.append(MigrationTool.__add_stmt(INDEX_FIXED_CONTENT, "EUDAT/FIXED_CONTENT", self.fixed_content))
                
                # Now determine FIO and ROR
                ror = ""
                fio = ""
                
                if helper_value.get("ROR"):
                    ror = helper_value["ROR"]
                    st_remove.append(MigrationTool.__remove_stmt(handle_name, helper_index["ROR"]))
                if helper_value.get("EUDAT/ROR"):
                    ror = helper_value["EUDAT/ROR"]
                    st_remove.append(MigrationTool.__remove_stmt(handle_name, helper_index["EUDAT/ROR"]))
                
                if DO_REMOTE_CALLS and (helper_value.get("PPID") or helper_value.get("EUDAT/PPID")):
                    # The current record is for a replica
                    # Now walk the chain of PPID pointers back to the original
                    if "PPID" in helper_value:
                        predecessor = helper_value["PPID"]
                    else:
                        predecessor = helper_value["EUDAT/PPID"]
                    original_record = {}
                    counter = 0
                    successor = handle_name
                    while True:
                        counter += 1
                        if counter > 100:
                            raise Exception("Error walking the replica chain: Infinite loop! Last predecessor: %s" % predecessor)
                        original_record = self.retrieve_handle_record_remotely(predecessor)
                        if not original_record:
                            print("Warning: Handle %s contains a PPID entry pointing to a non-existing Handle!" % successor)
                            break
                        successor = predecessor
                        if "PPID" in original_record:
                            predecessor = original_record["PPID"]
                        else:
                            predecessor = original_record.get("EUDAT/PPID")
                        if predecessor:
                            predecessor = predecessor.strip()
                        if not predecessor:                         
                            break
                        # clean up - cut http://hdl.handle.net
                        if predecessor.startswith("http://hdl.handle.net/"):
                            predecessor = predecessor[22:]
                        elif handle_name.startswith("https://hdl.handle.net/"):
                            predecessor = predecessor[23:]
                        if not predecessor:
                            print("Warning: Broken PPID value in Handle %s" % successor)                         
                            break
                        
                    fio = predecessor
                    if not ror:
                        ror = fio
                    # Now write ror & fio fields
                    st_add.append(MigrationTool.__add_stmt(INDEX_ROR, "EUDAT/ROR", ror))
                    st_add.append(MigrationTool.__add_stmt(INDEX_FIO, "EUDAT/FIO", fio))
                    # Also cover the new PARENT field, which simply takes the PPID value, replacing it
                    if "PPID" in helper_value:
                        ppid = helper_value["PPID"]
                        st_remove.append(MigrationTool.__remove_stmt(handle_name, helper_index["PPID"]))
                    else:
                        ppid = helper_value["EUDAT/PPID"]
                        st_remove.append(MigrationTool.__remove_stmt(handle_name, helper_index["EUDAT/PPID"]))
                    st_add.append(MigrationTool.__add_stmt(INDEX_PARENT, "EUDAT/PARENT", ppid))
                else:
                    # The current record is for an original
                    # 1. An FIO field will not be included (does not make sense)
                    # 2. If an ROR is in the old record, use as is. If it is empty, leave empty.
                    if ror:
                        st_add.append(MigrationTool.__add_stmt(INDEX_ROR, "EUDAT/ROR", ror))
                
                # Transform 10320/loc entry to a comma-separated list for the new EUDAT/REPLICA field
                if helper_value.get("10320/LOC"):
                    st_remove.append(MigrationTool.__remove_stmt(handle_name, helper_index["10320/LOC"]))
                    # parse XML structure
                    tree = ElementTree.fromstring(helper_value.get("10320/LOC"))
                    replica_locs = {}
                    for loc in tree.findall("location"):
                        replica_locs[int(loc.get("id"))] = loc.get("href")
                    # entry 0 should be the same as the Handle's base URL
                    if replica_locs.get(0) != helper_value["URL"]:
                        raise Exception("Broken 10320/LOC record on %s: href on id 0 does not match Handle's URL value!" % handle_name)
                    del replica_locs[0]
                    if replica_locs:
                        st_add.append(MigrationTool.__add_stmt(INDEX_REPLICA, "EUDAT/REPLICA", ",".join(replica_locs.itervalues())))
            
                # Record profile version '1'
                st_add.append("%s EUDAT/PROFILE_VERSION 86400 1110 UTF8 1" % INDEX_PROFILE_VERSION)
                # Execute queued statements
                if st_remove:
                    for l in st_remove:
                        batch_file.write("REMOVE %s\n" % l)
                if st_modify:
                    batch_file.write("MODIFY %s\n" % handle_name)
                    for l in st_modify:
                        batch_file.write(l+"\n")
                    batch_file.write("\n")
                if st_add:
                    batch_file.write("ADD %s\n" % handle_name)
                    for l in st_add:
                        batch_file.write(l+"\n")
                    batch_file.write("\n")
                batch_file.write("\n")
        finally:
            batch_file.close()
                

if __name__ == "__main__":
    # argparse
    parser = argparse.ArgumentParser(description="Tool to migrate Handle records to the EUDAT PID Profile v1.")
    parser.add_argument("handleuser", help="Handle user (index:handle); this is not used directly, but only written into the output batch file")
    parser.add_argument("--handlekeyfile", help="Handle private key file; this is not used directly, but only written as a reference into the output batch file")
    parser.add_argument("--handlesecretkey", help="Handle secret key; this is not used directly, but only written into the output batch file")
    parser.add_argument("databasehost", help="SQL database server host")
    parser.add_argument("databaseuser", help="SQL database server user name")
    parser.add_argument("databasepassword", help="SQL database server user password")
    parser.add_argument("databasename", help="SQL database name on server")
    parser.add_argument("outputfile", help="Output batch file name")
    parser.add_argument("prefix", help="Prefix(es) to query, comma separated")
    parser.add_argument("--fixedcontent", action="store_true", help="Content is fixed for all covered prefixes")
    parser.add_argument("--no-fixedcontent", action="store_true", help="Content is not fixed for all covered prefixes")
    
    args = parser.parse_args()

    if not args.fixedcontent and not args.no_fixedcontent:
        print("You must specify one of either --no-fixedcontent or fixedcontent!")
        sys.exit(1)
        
    if not args.handlekeyfile and not args.handlesecretkey:
        print("You msut specify one of either --handlekeyfile or --handlesecretkey!")
        sys.exit(1)
        
    if args.fixedcontent:
        print("fixedcontent = TRUE")
    else:
        print("fixedcontent = FALSE")
        
    migration_tool = MigrationTool(args.databasehost, args.databaseuser, args.databasepassword, args.databasename,
                                   args.handleuser, args.outputfile, args.prefix.split(","), args.fixedcontent, handle_key_file = args.handlekeyfile, handle_secret_key = args.handlesecretkey)
    t_start = datetime.now()
    print("Migration started: %s" % t_start)
    migration_tool.execute()
    t_end = datetime.now()
    print("Migration finished: %s" % t_end)
    print("Migration was done on %s Handles." % migration_tool.total_number_of_handles)
    t_delta = t_end - t_start
    print("Migration took: %s" % t_delta)
    if migration_tool.total_number_of_handles > 0:
        t_avg_per_handle = t_delta / migration_tool.total_number_of_handles
        print("Migration time per Handle: %s" % t_avg_per_handle)
    
