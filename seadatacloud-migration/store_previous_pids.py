import requests

# Merret, 2021-03-27
# Simply reading handles from file, retrieving their records,
# and writing the records back to a file. As a backup.

INPUTFILENAME = 'prod.pids'
OUTPUTFILENAME = INPUTFILENAME+'_records_stored'
URL_RETRIEVE = 'http://hdl.handle.net/api/handles/'


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

if __name__ == '__main__':

    all_paths_and_handlenames = read_pids_from_file(INPUTFILENAME)
    all_done_handles = []


    try:
        n = 0
        i = 0
        batch = 1
        current_outputfilename='%s_%s.txt' % (OUTPUTFILENAME, batch)
        print('First file: %s' % current_outputfilename)
        for path, handlename in all_paths_and_handlenames:
            i += 1
            n += 1
            if i > 1000:
                i = 1
                batch += 1
                current_outputfilename='%s_%s.txt' % (OUTPUTFILENAME, batch)
                print('Next file: %s' % current_outputfilename)

            # Retrieve record and write to outputfile:
            resp = requests.get(URL_RETRIEVE+handlename)
            record_new_json = resp.json()
            record_new_bytes = resp.content
            record_new_string = record_new_bytes.decode('UTF-8')
            with open(current_outputfilename, "a") as myfile:
                myfile.write(record_new_string+'\n')
            all_done_handles.append(handlename)
            print('Written (%s): %s' % (n, handlename))

    except (Exception, KeyboardInterrupt) as e:
        print('Exception: %s' % e)
        current_outputfilename='%s_completed_handles.txt' % (INPUTFILENAME)
        print('Writing all done handles to file "%s"' % current_outputfilename)
        with open(current_outputfilename, "w") as myfile:
            for h in all_done_handles:
                myfile.write(h+';')
        print('Done writing...')
        raise e


print('Done! See: %s' % OUTPUTFILENAME)

