#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import argparse
import logging
import datetime
import sys
import os
import urllib3
from pprint import pformat
import pyhandle
from pyhandle.handleclient import PyHandleClient


'''
Script to migrate existing handles/PIDs from an old prefix 
(21.T12996) to a new prefix (21.12118), while keeping the
same suffix.

Needed: Credentials for both prefixes as JSON files, and a text file
containing all the handle names which should be "migrated". The name
of that file must be given as input param.

Merret Buurman, DKRZ, 2021-03-28

'''

# Example PIDs:
# http://hdl.handle.net/21.T12996/0610bd92-5ee4-11eb-af23-fa163eef9493?noredirect
# http://hdl.handle.net/21.T12996/735b3de4-c9a5-11e9-9caf-fa163eef9493?noredirect
# http://hdl.handle.net/21.T12996/3ae8f24a-0c66-11ea-9ce8-fa163eef9493?noredirect


# Path to file containing all old handles (one handle by line, lines with # are ignored)
#INPUTFILENAME = './test_input.txt'
CREDENTIALS_OLD_PREFIX = '../credentials/pid_credentials.test.json'
CREDENTIALS_NEW_PREFIX = '../credentials/pid_credentials.prod.json'
OLD_PREFIX = '21.T12996'
NEW_PREFIX = '21.12118'
URL_RETRIEVE = 'http://hdl.handle.net/api/handles/'
URL_CHECK = 'http://hdl.handle.net/XXX?noredirect&auth'

# Arguments
PROGRAM_DESCRIP = 'This script reads handles from file and creates the same records under a new prefix (to migrate from the test prefix to the production prefix in SeaDataCloud).'
VERSION = 20210328
parser = argparse.ArgumentParser(description=PROGRAM_DESCRIP)
parser.add_argument('--version', action='version', version='Version: %s' % VERSION)
parser.add_argument('--verbose', '-v', action='count')        # also accepts -vvv
parser.add_argument('--inputfilename')
parser.add_argument('--dry-run', action="store_true")
parser.add_argument('--test', action="store_true")
myargs = parser.parse_args()
if myargs.verbose is None:
    myargs.verbose = 0

INPUTFILENAME = myargs.inputfilename


###
### Logging
###
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
root = logging.getLogger()
root.setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('pyhandle').setLevel(logging.WARNING)
if myargs.verbose >= 1:
    root.setLevel(logging.DEBUG)
LOGGER = logging.getLogger(__name__)
# Logging to file
LOG_DATE = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
handler_file = logging.FileHandler('pid_migration_%s.%s.log' % (INPUTFILENAME, LOG_DATE))
handler_file.setFormatter(formatter)
root.addHandler(handler_file)
# Logging to stdout:
handler_stdout = logging.StreamHandler(sys.stdout)
handler_stdout.setFormatter(formatter)
root.addHandler(handler_stdout)



#################
### Functions ###
#################

def is_data_url(some_url):
    if some_url.startswith('https://seadata.csc.fi/'):
        return True
    return False

def is_redirection_url(some_url):
    if some_url.startswith('http://hdl.handle.net/'+NEW_PREFIX):
        return True
    return False

def get_new_hs_admin_for_new_prefix(entry):
    if not entry['data']['format'] == 'admin':
        raise ValueError('Wrong format of HS_ADMIN!')

    ownerhandle = entry['data']['value']['handle']

    if not OLD_PREFIX in ownerhandle:
        raise ValueError('HS_ADMIN owner handle does not contain %s!' % OLD_PREFIX)

    if not ownerhandle == '0.NA/'+OLD_PREFIX:
        raise ValueError('HS_ADMIN owner handle is not 0.NA/%s!' % OLD_PREFIX)

    newownerhandle = ownerhandle.replace(OLD_PREFIX, NEW_PREFIX)
    return newownerhandle

def create_new_record(handlename_new, record_old, client_new_prefix, dry_run=False):

    # If the old record had not been modified yet, all entries
    # stay exactly the same, only the HS_ADMIN entry has to be
    # adapted.

    # In case the old record had already been modified, the
    # DEPRECATED, DEPRECATION_REASON, NEW_HANDLE have to be omitted.
    # ALso, the URL field cannot be copied like this, as the
    # old record's URL field will already contain the redirection url,
    # not the original data url. In this case, we need to fetch the
    # data url from the OLD_URL_FIELD from the old record, keep it,
    # and add it to the new record's URL field at the end.
 
    url_field_contains_correct_url = None
    original_data_url = None

    # Note: The timestamp of all entries will be overwritten by
    # the Handle System.

    # Iterate over old record entries and copy/modify them:
    record_new = []
    for entry in record_old['values']:

        # Just omit these (these only exists if the old record 
        # had already been modified before)::
        if entry['type'].lower() in ['deprecated', 'deprecation_reason', 'new_handle']:
            LOGGER.debug('Omitting "%s" when copying the record to new prefix...' % entry['type'])
            continue

        # Check if it contains the correct URL:
        elif entry['type'].lower() == 'url':
            content = entry['data']['value']
            
            if is_data_url(content):
                url_field_contains_correct_url = True

            elif is_redirection_url(content):
                url_field_contains_correct_url = False # must correct later!
                LOGGER.debug('Oops, not the expected url here in URL, must correct further down...')

            else:
                errmsg = 'Unexpected URL in old record: %s' % content
                LOGGER.error(errmsg)
                raise ValueError(errmsg)

        # Omit this, but keep its content to add it to the URL
        # field at the end (this only exists if the old record 
        # had already been modified before):
        elif entry['type'].lower() == 'old_url_field':
            content = entry['data']['value']
            
            if is_data_url(content):
                original_data_url = content # keep!
                LOGGER.debug('Omitting "%s" when copying the record to new prefix... but keeping its content for the URL field!' % entry['type'])
          
            elif is_redirection_url(content):
                errmsg = "Houston, we have a problem: Redirection-url in old record's OLD_URL_FIELD: %s" % content
                LOGGER.error(errmsg)
                raise ValueError(errmsg)
            
            else:
                errmsg = "Weird content in old record's OLD_URL_FIELD: %s" % content
                LOGGER.error(errmsg)
                raise ValueError('Old record: Wrong content in OLD_URL_FIELD: %s' % content)
            
            continue # don't copy this field!

        elif entry['type'].lower() == 'hs_admin':
            newownerhandle = get_new_hs_admin_for_new_prefix(entry)
            LOGGER.debug('Owner of new handle record: %s' % newownerhandle)
            entry['data']['value']['handle'] = newownerhandle

        # Add entry to new record:
        record_new.append(entry)

    # Finally:
    # In case the URL field contains the wrong URL, copy the correct
    # URL into it, which we should have found by now.
    if not url_field_contains_correct_url:
        for entry in record_new:
            if entry['type'].lower() == 'url':
                if original_data_url is not None:
                    entry['data']['value'] = original_data_url
                    LOGGER.debug('Corrected the URL field with the data url...')
                else:
                    errmsg = 'Very weird: Found wrong url in URL field, but did not find correct url in OLD_URL_FIELD.'
                    LOGGER.error(errmsg)
                    raise ValueError(errmsg)

    # Add a comment:
    # Note: This needs pyhandle > 1.0.4 (not released yet)
    new_index = pyhandle.client.RESTHandleClient.make_another_index(record_new)
    notice = ("This record was created by the pid-migration (version %s) on %s." %
        (VERSION, LOG_DATE))
    entry_notice = {
        "index": new_index,
        "type": "COMMENT",
        "data": notice
    }
    record_new.append(entry_notice)

    # Printing
    if myargs.verbose >= 2:
        LOGGER.debug('NEW RECORD:\n\n%s\n' % pformat(record_new))

    # Create handle record on new prefix
    if dry_run:
        LOGGER.debug('dry-run: Not creating: %s' % handlename_new)

    else:
        client_new_prefix.register_handle_json(
            handlename_new,
            record_new,
            overwrite=False
        )

def find_original_url_from_old_record(record_old):
    # We want the original content of URL (the original data url) to
    # end up in the OLD_URL_FIELD.
    # But careful, if this record has already been modified before, then
    # URL already contains the updated value (the redirection url), so
    # copying it to OLD_URL_FIELD would overwrite the original value with
    # the updated value.

    # Find the content of URL field:
    url_field_old_record = None
    for entry in record_old['values']:
        if entry['type'].lower() == 'url':
            url_field_old_record = entry['data']['value']
            break

    # Check if URL contains the original, or the updated url:
    if is_data_url(url_field_old_record):
        LOGGER.debug('Found data URL in URL field. Yay!')
        return url_field_old_record
        # Fine! This is the original url!

    elif is_redirection_url(url_field_old_record):
        LOGGER.debug('Found redirection url in URL field - must look for data url elsewhere!')
        # This is the redirection URL that we added to the old record's
        # URL field to redirect to the new ones. (This can only happen
        # if the old record had already been modified before.) If we 
        # write that into the OLD_URL_FIELD, we'd overwrite the real old url.

        # So we need to find the real original data url:
        for entry in record_old['values']:
            if entry['type'].lower() == 'old_url_field':
                content_from_old_url_field = entry['data']['value']
                
                if is_data_url(content_from_old_url_field):
                    LOGGER.debug('Found data URL in field OLD_URL_FIELD. Yay!')
                    return content_from_old_url_field # fine!

                elif is_redirection_url(content_from_old_url_field):
                    errmsg = 'Old record: OLD_URL_FIELD contains redirection url: %s (expected data url)' % content_from_old_url_field
                    LOGGER.error(errmsg)
                    raise ValueError(errmsg)

        # Extremely unlikely:
        errmsg = 'Old record: We found the redirection url in the URL field already, but OLD_URL_FIELD does not exist: %s' % url_field_old_record
        LOGGER.error(errmsg)
        raise ValueError(errmsg)

    # Extremely unlikely:
    errmsg = 'Old record: URL field contains neither data url nor redirection url, but unexpected content: %s' % url_field_old_record
    raise ValueError(errmsg)

def modify_record_on_test_prefix(handlename_new, handlename_old, record_old,
    client_old_prefix, dry_run=False):

    original_url = find_original_url_from_old_record(record_old)
    
    to_be_modified = {
        'DEPRECATED': 'True', # string with capital T, otherwise it shows as string "true" in the record, which differs from other "True" values.
        'DEPRECATION_REASON': 'This handle was mistakenly minted in a test prefix. The real handle is: '+handlename_new,
        'URL': 'http://hdl.handle.net/'+handlename_new,
        'NEW_HANDLE': handlename_new,
        'OLD_URL_FIELD': original_url
    }

    if myargs.verbose >= 2:
        LOGGER.debug('\n\nTO MODIFY: %s' % pformat(to_be_modified))

    if not dry_run:
        client_old_prefix.modify_handle_value(handlename_old,
            add_if_not_exist = True,
            **to_be_modified
        )  

def instantiate_client(credentials_file, https_verify, dry_run=False):
    if dry_run:
        readonlyclient = PyHandleClient('rest').instantiate_for_read_access(
            HTTPS_verify=https_verify)
        return readonlyclient

    creds = pyhandle.clientcredentials.PIDClientCredentials.load_from_JSON(credentials_file)
    writeablerestclient = PyHandleClient('rest').instantiate_with_credentials(
        creds, HTTPS_verify=https_verify)
    return writeablerestclient

def read_pids_from_file(inputfilename):
    inputfile = open(inputfilename, "r")
    n = 0

    for l in inputfile:
        line = l.rstrip("\n ")
        if line and not line.startswith("#"):

            # Mattia D'Antonio (CINECA) sends me a file with
            # path and PID (tab-delimited):

            res = line.split('\t')
            if not len(res) == 2:
                raise ValueError('Expecting two tab-delimited items per input line, received %s: %s' % (len(res), ';'.join(res)))

            path, handle = res[0], res[1]

            if not path.startswith('/'):
                raise ValueError('Expecting path to start with "/", instead got: %s' % path[:10])

            # Handlenames should not have "hdl:" in front!
            if handle.startswith('hdl:'):
                handle = handle[4:]
            else:
                handle = handle

            n += 1
            yield (path, handle)

    inputfile.close()
    LOGGER.info('Found %s handles in input file.' % n)

def final_logging(handles_created, handles_existed, handles_missing):
    
    if len(handles_existed) > 0:
        LOGGER.info('Handles already existed in new prefix: %s' % len(handles_existed))
    
    if len(handles_missing) > 0:
        LOGGER.info('Handles missing in old prefix:         %s' % len(handles_missing))
    
    LOGGER.info('Handles created in new prefix:         %s' % len(handles_created))

def write_processed_handles_to_file(handles_created, handles_existed, handles_missing):

    limit = 100000

    n1 = len(handles_existed)
    batch = 1
    if n1 > 0:
        i=0
        filename = 'handles_existed_%s_%s.txt' % (INPUTFILENAME, batch)
        f1 = open(filename, 'a')
        f1.write('These %s existed at %s...\n' % (n1, LOG_DATE))
        for path, handle in handles_existed:
            i+=1
            f1.write(path+'\t'+handle+'\n')
            if i>=limit:
                i=0
                batch+=1
                f1.close()
                filename = 'handles_existed_%s_%s.txt' % (INPUTFILENAME, batch)
                f1 = open(filename, 'a')
        f1.close()

    n2 = len(handles_missing)
    batch = 1
    if n2 > 0:
        i=0
        filename = 'handles_missing_%s_%s.txt' % (INPUTFILENAME, batch)
        f2 = open(filename, 'a')
        f2.write('These %s were missing at %s...\n' % (n2, LOG_DATE))
        for path, handle in handles_missing:
            i+=1
            f2.write(path+'\t'+handle+'\n')
            if i>=limit:
                i=0
                batch+=1
                f2.close()
                filename = 'handles_missing_%s_%s.txt' % (INPUTFILENAME, batch)
                f2 = open(filename, 'a')
        f2.close()


    n3 = len(handles_created)
    batch = 1
    if n3 > 0:
        i=0
        filename = 'handles_created_%s_%s.txt' % (INPUTFILENAME, batch)
        f3 = open(filename, 'a')
        f3.write('These %s were created at %s...\n' % (n3, LOG_DATE))
        for path, handle in handles_created:
            i+=1
            f3.write(path+'\t'+handle+'\n')
            if i>=limit:
                i=0
                batch+=1
                f3.close()
                filename = 'handles_created_%s_%s.txt' % (INPUTFILENAME, batch)
                f3 = open(filename, 'a')
        f3.close()

def delete_most_values_old_record(handlename_old, record_old, client_old_prefix, dry_run=False):

    # TODO NOT TESTED YET

    # Which fields to delete?
    do_delete = []
    not_delete = ['DEPRECATED', 'DEPRECATION_REASON', 'URL',
                  'OLD_URL_FIELD', 'NEW_HANDLE', 'HS_ADMIN'] 
    
    for entry in record_old['values']:
        key = entry['type']
        if not key in not_delete:
            do_delete.append(key)

    # Do it!
    if dry_run:
        LOGGER.debug('dry-run: Not deleting from %s: %s'  % (handlename, ', '.join(do_delete)))
    else:
        LOGGER.debug('Will delete from %s: %s' % (handlename, ', '.join(do_delete)))
        client_old_prefix.delete_handle_value(handlename, do_delete)


#############
### Start ###
#############


if __name__ == '__main__':

    # Some more settings:
    https_verify = False
    if not https_verify:
        LOGGER.warning('Not verifying https certificate.')
        LOGGER.warning('Silencing InsecureRequestWarnings for your convenience.')
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Announcements:
    if myargs.dry_run:
        LOGGER.warning("*** dry-run ***")
        LOGGER.warning("Won't create or modify any handles, because it's a dry-run.")

    # Instatiations...
    LOGGER.debug('Instantiating handle clients...')
    client_old_prefix = instantiate_client(CREDENTIALS_OLD_PREFIX, https_verify, myargs.dry_run)
    client_new_prefix = instantiate_client(CREDENTIALS_NEW_PREFIX, https_verify, myargs.dry_run)

    # Read handles from file:
    if myargs.test:
        all_paths_and_handlenames = [
            ('/foo/bar', '21.T12996/0610bd92-5ee4-11eb-af23-fa163eef9493'),
            ('/foo/baz', '21.T12996/735b3de4-c9a5-11e9-9caf-fa163eef9493'),
            ('/foo/boo', '21.T12996/3ae8f24a-0c66-11ea-9ce8-fa163eef9493')
        ]
    else:
        LOGGER.info('Reading handles from file "%s"...' % INPUTFILENAME)
        if not os.path.isfile(INPUTFILENAME):
            raise IOError('This is not a file: %s' % INPUTFILENAME)
        all_paths_and_handlenames = read_pids_from_file(INPUTFILENAME)


    # Stats:
    # Lists will contain tuples of (path, handlename), path being
    # an attribute (storage path in B2SAFE/iRODS) which we don't need
    # in this script, but we need it in the output so that the prefix
    # can also be adapted in B2SAFE/iRODS.
    handles_existed = []
    handles_missing = []
    handles_created = []
    num_existed = 0
    num_missing = 0
    num_created = 0

    try:
        # Iteration over handles:
        LOGGER.debug('Starting to iterate over all handles...')
        i = 0
        for path, handlename_old in all_paths_and_handlenames:
            i += 1

            handlename_new = handlename_old.replace(OLD_PREFIX, NEW_PREFIX)

            # Skip if new handle already exists (in case
            # the process was interrupted and restarted):
            checkurl = URL_CHECK.replace('XXX', handlename_new)
            resp = requests.get(checkurl)
            if resp.status_code == 200:
                LOGGER.info('%s. Already exists, skipping: %s' % (i, handlename_new))
                handles_existed.append(tuple((path, handlename_new)))
                num_existed += 1
                continue
            
            elif not resp.status_code == 404:
                errmsg = 'Unexpected http code %s when retrieving %s' % (resp.status_code, handlename_new)
                LOGGER.error(errmsg)
                raise ValueError(errmsg)

            resp = requests.get(URL_RETRIEVE+handlename_new)
            record_new = resp.json()
            if record_new['responseCode'] == 1:
                LOGGER.debug('%s. pretends to exists, but when checking the non-json API it did not exist - probably a cache problem: %s' % (i, handlename_new))

            # Retrieve handle record
            resp = requests.get(URL_RETRIEVE+handlename_old)
            record_old = resp.json()
            if resp.status_code == 404:
                LOGGER.warning('%s. Old record does not exist - why? %s' % (i, handlename_old))
                handles_missing.append(tuple((path, handlename_new)))
                num_missing += 1
                continue

            if myargs.verbose >= 2:
                LOGGER.debug('EXISTING RECORD:\n\n%s\n' % pformat(record_old['values']))


            # Create new record
            #LOGGER.debug('%s. Starting... %s' % (i, handlename_new))
            LOGGER.debug('%s. Starting (%s)... %s' % (i, INPUTFILENAME, handlename_new))
            create_new_record(handlename_new, record_old, client_new_prefix, myargs.dry_run)
            handles_created.append(tuple((path, handlename_new)))
            num_created += 1
            LOGGER.info('%s. Created %s' % (i, handlename_new))

            # Modify old record
            modify_record_on_test_prefix(handlename_new, handlename_old, record_old,
                client_old_prefix, myargs.dry_run)
            LOGGER.info('%s. Modified %s' % (i, handlename_old))

            # Delete old record's values (all but the above)
            #LOGGER.info('*****************************************')
            #LOGGER.info('*** Currently, deleting the handle values in the old record is commented-out.')
            #LOGGER.info('*****************************************')
            ### Commented out
            ###delete_most_values_old_record(handlename_old, record_old, client_old_prefix, myargs.dry_run)


    # Loop finished.

    except KeyboardInterrupt as e:
        LOGGER.info('User asked to stop...')
        final_logging(handles_created, handles_existed, handles_missing)
        write_processed_handles_to_file(handles_created, handles_existed, handles_missing)
        sys.exit(0)

    # Note: This needs pyhandle > 1.0.4 (not released yet)
    except pyhandle.handleexceptions.PyhandleException as e:
        LOGGER.error('Stopping. Exception occurred: %s' % e)
        final_logging(handles_created, handles_existed, handles_missing)
        write_processed_handles_to_file(handles_created, handles_existed, handles_missing)
        sys.exit(1)

    LOGGER.info('Finished...')
    final_logging(handles_created, handles_existed, handles_missing)
    write_processed_handles_to_file(handles_created, handles_existed, handles_missing)
    sys.exit(0)



# python3 pid_migration.py -vv --test --dry-run --inputfilename xyz.txt
# python3 pid_migration.py -v --test --dry-run --inputfilename xyz.txt

