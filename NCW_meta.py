# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 21:03:14 2022

@author: mitra
"""
# reads all metadata/config tables - stores in Global vars - serves the data via functions defined in this module
def initData( conn ):
    # initialize meta tables in python
    global JobData
    
    cur = conn.cursor()
    JobData = cur.execute("select * from meta_job where active='Y'").fetch_pandas_all()
    joblist = JobData['JOBID']
    return joblist

def getJobDetail( jobid ):
    jobtype = JobData.loc[JobData['JOBID']==jobid, 'JOBTYPE'][0]
    filepath = JobData.loc[JobData['JOBID']==jobid, 'FILEPATH'][0]
    stage = JobData.loc[JobData['JOBID']==jobid, 'STAGENAME'][0]
    isheader = JobData.loc[JobData['JOBID']==jobid, 'IS_HEADER'][0]
    tab = JobData.loc[JobData['JOBID']==jobid, 'TABLENAME'][0]
    return jobtype, filepath.lower(), stage, isheader, tab
    
"""
def get_sources():
    return hg_src_sys['SRCSYS_ID']

def get_channels(): 
    return hg_src_channel['CHN_NAME']

def get_chn_type(chn_name):
    return hg_src_channel.loc[(hg_src_channel['CHN_NAME']==chn_name),'CHN_TYPE'][0]

def get_api_details( clientid, srcid, chn_name ):
    connstr = hg_src_channel.loc[(hg_src_channel['SRCSYS_ID']==srcid) & (hg_src_channel['CHN_NAME']==chn_name), 'CONN_STR'][0]
    buf_page = hg_src_channel.loc[(hg_src_channel['SRCSYS_ID']==srcid) & (hg_src_channel['CHN_NAME']==chn_name),'BUF_PAGE'][0]
    pagesz = hg_src_channel.loc[(hg_src_channel['SRCSYS_ID']==srcid) & (hg_src_channel['CHN_NAME']==chn_name),'PAGE_SIZE'][0]
    lst = hg_src_channel_param.loc[(hg_src_channel_param['SRCSYS_ID']==srcid) & (hg_src_channel_param['CHN_NAME']==chn_name), ['PARAM_NAME', 'PARAM_VAL']]    
    payload = dict(zip(lst.PARAM_NAME, lst.PARAM_VAL))
    #for i in range(len(lst)):
    #    payload[lst['PARAM_NAME'][i]] = lst['PARAM_VAL'][i]
    return connstr, buf_page, pagesz, payload
"""
