import os, tempfile, shutil, subprocess
from connection import auth_params
import shlex

# file backend:
# -------------
# open file
# write everything
# close file
# load with subprocess
# clean up file

# fifo backend:
# -------------
# mkfifo
# hook process
# open file
# write everything
# close file
# clean up file


# Warning
#
# I've seen a lot of concurrency issues with the FIFO based bulkloader
# Closing the connection explicitly, while querying does help a lot
#

import sys

class Bulkloader(object):
    def __init__(self, cleanup = False):
        self._temp_dir = tempfile.mkdtemp()
        self._cleanup = cleanup
        self._result = None
        self._inited = False
    
    def stream(self):
        if not self._inited:
            self._pre()
            self._inited = True
        else:
            raise ValueError('You should not call stream() more than once')
        return self._stream
    
    def close(self):
        self._result = self._post()
        if self._cleanup:
            shutil.rmtree(self._temp_dir)
    
    def result(self):
        return self._result
        
    def _pre(self):
        raise NotImplementedError('pure virtual')

    def _post(self):
        raise NotImplementedError('pure virtual')
    

class FileBasedBulkloader(Bulkloader):
    def __init__(self, cleanup = False):
        super(FileBasedBulkloader, self).__init__(cleanup)
    
    def _pre(self):
        self._stream = tempfile.NamedTemporaryFile(dir = self._temp_dir, 
                                                   delete = self._cleanup)
    
    def _post(self):
        self._stream.close()
        auth = auth_params()
        # os.putenv('PGPASSWORD', '{0[password]}'.format(auth))
        my_env = os.environ.copy()
        my_env['PGPASSWORD'] = '{0[password]}'.format(auth)
        exe = 'psql -U {0[username]} -d {0[database]} -h {0[host]} -f {1}'.format(auth, self._stream.name)
        cli = exe.split()
        op = subprocess.Popen(cli, 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE,
                              env=my_env)
        R = op.poll()
        if R:
            res = op.communicate()
            raise ValueError(res[1])
        stdoutdata, stderrdata = op.communicate()
        # this will be written in _result, see close()
        return stdoutdata, stderrdata 


class FifoBasedBulkloader(Bulkloader):
    def __init__(self, cleanup = True):
        super(FifoBasedBulkloader, self).__init__(cleanup)
        self._filename = self._temp_dir + '/fifo'
    
    def _pre(self):
        os.mkfifo(self._filename)
        auth = auth_params()
        cli = 'psql -d {} -f {}'.format(auth['database'], self._filename)
        self._op = subprocess.Popen(cli.split(' '), 
                                    stdout=subprocess.PIPE, 
                                    stderr = subprocess.PIPE, 
                                    close_fds = True)
        if self._op.poll():
            res = self._op.communicate()
            raise ValueError(res[1])
        self._stream = open(self._filename, 'w', 0)
    
    def _post(self):
        self._stream.close()
        return self._op.communicate()


def test():
    sql = 'SELECT version();'
    text = 'SELECT postgis_full_version();'
    
    man0 = FileBasedBulkloader()
    #man1 = FifoBasedBulkloader()
    
    stream0 = man0.stream()
    #stream1 = man1.stream()
    
    stream0.write(sql)
    #stream1.write(sql)
    
    stream0.write(text)
    #stream1.write(text)
    
    man0.close()
    print """stdout"""
    print man0.result()[0]
    print "--"
    print """stderr"""
    print man0.result()[1]
    print "--"
    #man1.close()
    print 'done'
    #print man1.result()[0]
    
    #try:
    #    cli = 'psql -d test -f ' + filename
    #    op = subprocess.Popen(cli.split(' '), stdout=subprocess.PIPE, stderr = subprocess.PIPE)
    #    f = open(filename, 'w')    
    #    
    #    f.write(sql)
    #    f.write(text)
    #    f.close()
    #finally:
    #    shutil.rmtree(temp_dir)
    #print op
    #print "done."
    #
    #res = op.communicate()
    #print "stdout"
    #print res[0]
    #
    #print "stderr"
    #print res[1]
    ##try:
    ##    os.unlink(fifoname)
    ##except OSError:
    ##    pass
    ##os.mkfifo(fifoname)
    ##cli = 'psql -d gisbase -h casagrande.geo.tudelft.nl -f ' + fifoname
    ##op = subprocess.Popen(cli.split(' '), stdout=subprocess.PIPE)

if __name__ == '__main__':
    test()