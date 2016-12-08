import os
import subprocess
import sys
from json import loads
import argparse
import logging

# Logging to syslog messages

logger = logging.getLogger('zbx_netapp_log')
log_path = logging.FileHandler('/var/log/messages')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_path.setFormatter(formatter)
logger.addHandler(log_path)
logger.setLevel(logging.INFO)

# Script definition and usage
parser = argparse.ArgumentParser(description = "Script to publish data from NetApp to Zabbix")
parser.add_argument("-d", help="Debug Mode Enabled", action="store_true")
parser.add_argument('ZABBIX_HOST', help="Zabbix host name")
parser.add_argument('DATA_TYPE', help="Argument to push LLD/Data")
parser.add_argument('ZABBIX_SERVER', nargs='?', default='127.0.0.1', help="Argument to give Zabbix Server IP")
args = parser.parse_args()

#Script variables
TMP_FILER = '/var/tmp/zabbix'
ZABBIX_HOST = args.ZABBIX_HOST#sys.argv[1]
DATA_TYPE = args.DATA_TYPE#sys.argv[2]
PERF_SCRIPT = './perf_operation.pl'
FNULL = open(os.devnull, 'w')
hosts = {
        'it-fs1.sjc2.turn.corp':{'username':'perfmon','password':'za66ixshmabbix'}
        }

ZABBIX_SERVER = args.ZABBIX_SERVER


#Fuctions and Definitions

def execute_command(command):
    '''
    Execute the given command and raise exception if any
    '''
    try:
        p = subprocess.Popen(command, shell=True,  stdout=subprocess.PIPE, stderr=FNULL, executable="/bin/bash")
        status = p.wait()
        if status == 0:
             if args.d:
                 logger.info(p.communicate()[0])
        else:
             logger.error('Failed to execute command %s' % (command))
             exit(1)
    except Exception,e:
        logger.error('Failed to execute command %s' % (command))
        exit(1)

def LLD(filename):
    '''
    Execute the command zabbix_sender for Low Level Discovery.
    '''
    command = 'zabbix_sender -s %s -z %s -i %s' % (ZABBIX_HOST,ZABBIX_SERVER,filename)
    execute_command(command)


def post_data(instances):
    '''
    Execute the zabbix_sender command for each monitoring parameters on every instance
    '''
    command = 'zabbix_sender -z %s -p 10051 -s "%s" -k %s[%s] -o %s'
    aggr_params = ["total_transfers", "user_read_blocks", "user_write_blocks"]
    for name in (instances):
        monitoring_parms=["total_ops","avg_latency","read_ops","read_latency","write_ops","write_latency","read_data","write_data","read_blocks","write_blocks"]
        if name.startswith('aggr'):
            monitoring_parms = aggr_params
        json_file_name = os.path.join(TMP_FILER,name+'.txt')
        f = open(json_file_name,'r')
        lines = f.read()
        lines=lines.replace("- na.vol.discovery ","");
        json_data=loads(lines);
        for metric in monitoring_parms :
            execute_command(command %(ZABBIX_SERVER,ZABBIX_HOST,metric,name,json_data["data"][0][metric]))
        f.close()


def api_data_parser(api_file):
    '''
    Fetch the API details and parse the data and it will creates a data file for every instance.
    '''
    f = open(api_file, 'r')
    lines = f.readlines()
    f.close()
    count = 0
    instances = set()
    while(count<len(lines)-1):
        if lines[count].startswith('Instance ='):
                filename = lines[count].split(' = ')[1].strip()
                f = open(os.path.join(TMP_FILER,filename+'.txt'),'w')
                f.write('- na.vol.discovery {"data":[{ "{#INSTANCE}":"'+filename+'",')
                count += 1
                contents = ''
                while( (count<=len(lines)-1) and not (lines[count].startswith('Instance ='))):
                    line = lines[count].strip().split('\t')
                    if len(line)>1:
                        contents += '"'+line[0][line[0].index('=')+1:].strip()+'":"'+line[1][line[1].index('=')+1:].strip()+'",'
                    count += 1
                f.write(contents[:len(contents)-1])
                f.write('}]}')
                f.close()
                if(DATA_TYPE=="lld"):
                    LLD(os.path.join(TMP_FILER,filename+'.txt'))
                instances.add(filename)
    post_data(instances)


if __name__ == '__main__':
    #Creating tmp directory if it doesn't exist
    if not os.path.isdir(TMP_FILER):
        os.makedirs(TMP_FILER)

    for host in hosts:
        api_file = os.path.join(TMP_FILER,host+'.txt')
        command = 'perl %s %s %s %s get-counter-values volume > %s' % (PERF_SCRIPT,host,hosts[host]['username'],hosts[host]['password'],api_file)
        execute_command(command)
        command = 'perl %s %s %s %s get-counter-values aggregate user_read_blocks user_write_blocks total_transfers >> %s' % (PERF_SCRIPT,host,hosts[host]['username'],hosts[host]['password'],api_file)
        execute_command(command)

        api_data_parser(api_file)
