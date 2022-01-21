# -*- coding: utf-8 -*-
"""
Created on Tue Oct 19 23:34:54 2021

@author: mitra
"""

import snowflake.connector
from sqlalchemy import create_engine
import NCW_meta

# script loads CSV file into SF Table using PUT & COPY commands
# extended to show simple examples of python/pandas usage 

def connectSnow():
    print("Connecting...")
    conn = snowflake.connector.connect(
        account = 'dr78105.us-east-2.aws',
        user = 'smitra1',
        password = 'Virtus@088',
        schema = 'PUBLIC',
        database ='NCW_TEST',
        warehouse ='COMPUTE_WH',
        role = 'SYSADMIN'
    )
    print("Connected! (SF Connector)")
    return conn;

def connectAlchemy():
    engine2 = create_engine(
        'snowflake://{user}:{pwd}@{actid}/{mydb}/{myschema}?warehouse={wh}&role={myrole}'.format(
            user = 'smitra1',
            pwd = 'Virtus@088',
            actid = 'dr78105.us-east-2.aws',
            wh = 'COMPUTE_WH',
            mydb = 'NCW_TEST',
            myrole = 'SYSADMIN',
            myschema = 'PUBLIC'
        )
    )
    print("Connected! (Alchemy SF Connector)")
    return engine2.raw_connection()


def validateJob( jobid ):
    jobtype, filepath, stage, isheader, tab = NCW_meta.getJobDetail( jobid )
    
    # check filename validity
    ss = filepath.split('/')
    fname = ss[len(ss)-1]
    if fname not in ['cell1.csv', 'cell2.csv']:
        return ErrStat, 'Jobid ' + str(jobid) + ' filename is invalid'
    # check if header matches columnnames in field map meta table
    return SuccessStat, ''
    
    
def runJob( conn, jobid ):
    jobtype, filepath, stage, isheader, tab = NCW_meta.getJobDetail( jobid )
    
    cur = conn.cursor()
    #cur.execute('use database NCW_TEST')
    if jobtype == 'CSV':
        qry = 'put ' + filepath + ' @' + stage
        cur.execute( qry )
        qry = 'copy into ' + tab + ' from @' + stage + ' force = true \
            file_format = (type = csv field_delimiter = \',\' skip_header = 1)'
        res = cur.execute(qry)
        cur.close()
    return SuccessStat, ''
    
    
    
def setJobStatus( start_or_end, conn, jobid, stat1=None, err1=None, stat2=None, err2=None ):
    i=0
    
# starts here... first, some global constants
SuccessStat = 'SUCCESS'
ErrStat = 'ERR'
JobStart = 'STARTED'
JobEnd = 'COMPLETED'

def main():
    #conn1 = connectSnow()    # use this for direct SF connector
    conn1 = connectAlchemy()    # uses SQLAlchemy
    joblist = NCW_meta.initData( conn1 )
    for jobid in joblist:
        stat1, err1 = validateJob( jobid )
        if stat1 == SuccessStat:
            setJobStatus( JobStart, conn1, jobid )
            stat2, err2 = runJob( conn1, jobid )
        setJobStatus( JobEnd, conn1, jobid, stat1, err1, stat2, err2 )
        conn1.commit()
        
main()
print('Done')