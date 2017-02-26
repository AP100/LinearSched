import argparse
from datetime import datetime
import os
import sqlite3 as lite
import subprocess
import sys

BASE_DIR = os.environ['BASE']
# SCHED_FOLDER = os.path.join(BASE_DIR, 'temp', 'sched')
SCHED_FOLDER = os.path.join(BASE_DIR, 'sched')
DB_FOLDER = os.path.join(SCHED_FOLDER, 'database')
DB_FILE = os.path.join(DB_FOLDER, 'sched.db')


class Job:
    """
    Class that encapsulates a job. Each job consists of a name, a sequence
    and a run command. This class is responsible of launching
    the run command.
    """

    def __init__(self, name, command):
        self.name = name
        self.command = command

    def __call__(self):
        ret = self.__launch()
        return ret

    def __repr__(self):
        return "Job %s" % self.name

    def __launch(self):
        p = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        ret = p.wait()
        output, err = p.communicate()
        print output
        return ret


class LinearSched():

    def __init__(self, sched_date, job_list_file=None):
        self.job_list_file = job_list_file
        self.sched_date = sched_date

    def _connect_db(self):
        try:
            con = lite.connect(DB_FILE, isolation_level=None)
            cur = con.cursor()
        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        return con, cur

    def _disconnect_db(self, con):
        if con:
            con.close()

    def _insert_sched(self, values):
        con, cur = self._connect_db()
        insert_sql = '''
                        Insert into schedule
                        (run_seq, job_name, sched_date, status) values
                        (?,?,date(?),?)
        '''

        try:
            cur.executemany(insert_sql, values)
        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        self._disconnect_db(con)

    def _select_sched_count(self):

        con, cur = self._connect_db()
        select_sql = ''' select count(*) from schedule
                         where sched_date = ?
        '''

        try:
            cur.execute(select_sql, [self.sched_date])
        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        count = cur.fetchone()[0]

        self._disconnect_db(con)

        return count

    def _select_sched_incomplt(self):

        con, cur = self._connect_db()
        select_sql = ''' select * from schedule
                         where sched_date = ? and status <> 'C'
                         order by run_seq
        '''

        try:
            cur.execute(select_sql, [self.sched_date])
        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        rows = cur.fetchall()

        self._disconnect_db(con)

        return rows

    def _select_sched_all(self):

        con, cur = self._connect_db()
        select_sql = ''' select * from schedule
                         where sched_date = ?
                         order by run_seq
        '''

        try:
            cur.execute(select_sql, [self.sched_date])
        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        rows = cur.fetchall()

        self._disconnect_db(con)

        return rows

    def _select_sched_jobstatus(self, job_name):

        con, cur = self._connect_db()
        con.text_factory = str
        select_sql = ''' select status
                         from schedule
                         where job_name = ? and sched_date = ?
        '''

        try:
            cur.execute(select_sql, [job_name, self.sched_date])
        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        status = cur.fetchone()

        self._disconnect_db(con)

        return status

    def _update_sched_jobstatus(self, job_name, job_status):
        con, cur = self._connect_db()

        update_sql = ''' update schedule
                         set status = ?
                         where job_name = ? and sched_date = ?
        '''
        val = [job_status, job_name, self.sched_date]         

        try:
            cur.execute(update_sql, val)
        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        self._disconnect_db(con)

    def _delete_sched(self):

        con, cur = self._connect_db()
        delete_sql = ''' delete  from schedule
                         where sched_date = ?
        '''

        try:
            cur.execute(delete_sql, [self.sched_date])
        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        self._disconnect_db(con)

    def is_sched_present(self):
        count = self._select_sched_count()        
        if count > 0:
            return True
        else:
            return False

    def prepare_sched(self):

        if self.is_sched_present():
            print "Sched already exists"
            sys.exit(1)
        
        with open(self.job_list_file) as file1:
            insert_vals = []
            for i, line in enumerate(file1):
                job = line.rstrip('\n')                
                insert_vals.append([i, job, self.sched_date, "I"])
            
            self._insert_sched(insert_vals)                
        

    def _prepare_queue(self):
        
        q = []
        rows = self._select_sched_incomplt()
        for row in rows:
            job_name = row[0]
            job_status = row[3]
            if job_status == 'R':
                print job_name, "is in running status schedule"
                sys.exit(1)
            job = Job(job_name, job_name + '.sh')
            q.append(job)        

        return q

    def update_job_status(self, job_name, job_status):
        
        status = self._select_sched_jobstatus(job_name)
        if  status == None:
            print "No matching job found in sched"
            sys.exit(1)
        else:
            self._update_sched_jobstatus(job_name, job_status)        


    def execute_sched(self):

        job_queue = self._prepare_queue()

        for job in job_queue:
            self.update_job_status(job.name, 'R')
            ret = job()
            if ret == 0:
                self.update_job_status(job.name, 'C')
            else:
                self.update_job_status(job.name, 'F')
                break

    def view_sched(self):        
        rows = self._select_sched_all()
        for row in rows:
            print row[0], row[3]


    def clear_sched(self):
        self._delete_sched()

def valid_date(s):
    try:
        temp =  datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def valid_file(f):
    file = os.path.join(SCHED_FOLDER, f)
    if os.path.isfile(file):
        return f
    else:
        msg = "File not present: '{0}'.".format(file)
        raise argparse.ArgumentTypeError(msg)

def valid_option(o):    
    if o in ['crt', 'run', 'upd', 'del', 'chk']:
        return o
    else:
        msg = "Option must be 'crt' or 'run' or 'upd' ,'del'"
        raise argparse.ArgumentTypeError(msg)

def valid_status(s):    
    if s in ['I', 'C']:
        return s
    else:
        msg = "Status must be 'I' or 'C'"
        raise argparse.ArgumentTypeError(msg)

if __name__ == "__main__":
    
    desc = 'Python Linear Scheduler v1.0'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-d', '--date', dest="sched_date", type=valid_date, 
                         required=True, help="Schedule date format YYYY-MM-DD" )
    parser.add_argument('-f', '--file', dest="job_list_file", type=valid_file,
                         help="File containing job list" )
    parser.add_argument('-o', '--option', dest="option", type=valid_option,
                         required=True, help="Option - Valid values crt, run, upd, del, chk" )
    parser.add_argument('-j', '--job', dest="job_name", type=str,
                         help="Job name" )
    parser.add_argument('-s', '--status', dest="job_status", type=valid_status,
                         help="Job status to be updated" )
    args = parser.parse_args()

    option = args.option    

    if option == 'chk':        
        if args.job_list_file or args.job_name or args.job_status:
            print "For check otion, only date parameter is required"
            sys.exit(1)
        lsched = LinearSched(args.sched_date)
        lsched.view_sched()
    
    if option == 'del':        
        if args.job_list_file or args.job_name or args.job_status:
            print "For delete otion, only date parameter is required"
            sys.exit(1)
        lsched = LinearSched(args.sched_date)
        lsched.clear_sched()

    if option == 'crt':
        if args.job_name or args.job_status:
            print "For create otion, only date and  file parameter is required"
            sys.exit(1)
        if not args.job_list_file:
            print "For create otion, file parameter is required"
            sys.exit(1)
        file = os.path.join(SCHED_FOLDER, args.job_list_file)
        lsched = LinearSched(args.sched_date, file)
        lsched.prepare_sched()
        
    if option == 'run':        
        if args.job_list_file or args.job_name or args.job_status:
            print "For run otion, only date parameter is required"
            sys.exit(1)
        lsched = LinearSched(args.sched_date)
        lsched.execute_sched()

    if option == 'upd':
        if args.job_list_file:
            print "For update option file name is not required"
            sys.exit(1)
        if args.job_name and args.job_status:
            lsched = LinearSched(args.sched_date)
            lsched.update_job_status(args.job_name, args.job_status)
        else:
            print "For update option job name and status is required"
            sys.exit(1)
