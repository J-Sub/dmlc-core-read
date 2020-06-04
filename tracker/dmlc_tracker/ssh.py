#!/usr/bin/env python3
"""
DMLC submission script by ssh

One need to make sure all slaves machines are ssh-able.
"""
from __future__ import absolute_import

from multiprocessing import Pool, Process
import os, subprocess, logging
from threading import Thread
from . import tracker

def sync_dir(local_dir, slave_node, slave_dir):
    """
    sync the working directory from root node into slave node
    """
    remote = slave_node[0] + ':' + slave_dir
    logging.info('rsync %s -> %s', local_dir, remote)
    prog = 'rsync -az --rsh="ssh -o StrictHostKeyChecking=no -p %s" %s %s' % (
        slave_node[1], local_dir, remote)
    subprocess.check_call([prog], shell = True)

def get_env(pass_envs):
    envs = []
    # get system envs
    keys = ['OMP_NUM_THREADS', 'KMP_AFFINITY', 'LD_LIBRARY_PATH', 'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY', 'DMLC_INTERFACE']
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            envs.append('export ' + k + '=' + v + ';')
    # get ass_envs
    for k, v in pass_envs.items():
        envs.append('export ' + str(k) + '=' + str(v) + ';')
    return (' '.join(envs))

def submit(args):
    assert args.host_file is not None
    with open(args.host_file) as f:
        tmp = f.readlines()
    assert len(tmp) > 0
    hosts=[]        # 배열
    for h in tmp:
        if len(h.strip()) > 0:
            # parse addresses of the form ip:port
            h = h.strip()

            # parse mpi host file form ip slots=??
            # this is to create an unified api for mpi and ssh
            i = h.find("slots=")
            if i != -1:
                h = h[:i].strip()

            i = h.find(":")
            p = "22"
            if i != -1:
                p = h[i+1:]
                h = h[:i]
            # hosts now contain the pair ip, port
            hosts.append((h, p))

    def ssh_submit(nworker, nserver, pass_envs):
        """
        customized submit script
        """
        # thread func to run the job
        def run(prog):
            subprocess.check_call(prog, shell = True)   
            # 인자와 함께 커맨드 실행
            # 커맨드가 완료될 때까지 wait
            # return 값이 0인 경우 그대로 return
            # 그게 아니면 CalledProcessError raise
            # 인자가 shell이 참임. shell을 통해서 커맨드를 수행한다는 것.
            # 그럼 prog는 무엇일까???


        # sync programs if necessary
        local_dir = os.getcwd()+'/'     # os.getcwd() : 현재 자신의 디렉토리 위치를 반환
        working_dir = local_dir         # 디렉토리 설정 ( local_dir, working_dir )
        if args.sync_dst_dir is not None and args.sync_dst_dir != 'None':
            # 명령행의 sync_dst_dir값이 있으면 수행. 없으므로 수행 x
            working_dir = args.sync_dst_dir
            pool = Pool(processes=len(hosts))
            for h in hosts:
                pool.apply_async(sync_dir, args=(local_dir, h, working_dir))
            pool.close()
            pool.join()
            

        # launch jobs
        for i in range(nworker + nserver):      # i = 0, 1, 2 (total 3이므로)
            pass_envs['DMLC_ROLE'] = 'server' if i < nserver else 'worker'
            # 첫번째 DMLC_ROLE 의 값은 server. 한번 iteration 이후 worker
            (node, port) = hosts[i % len(hosts)]        # <- 뭔지 모르겠는데, hosts 배열에서 특정 값을 나누어서 지정한 듯
            pass_envs['DMLC_NODE_HOST'] = node
            prog = get_env(pass_envs) + ' cd ' + working_dir + '; ' + (' '.join(args.command))
            prog = 'ssh -o StrictHostKeyChecking=no ' + node + ' -p ' + port + ' \'' + prog + '\''
            
            thread = Thread(target = run, args=(prog,))
            # 쓰레드가 실행할 함수 혹은 메서드를 작성하여 서브쓰레드 구현하는 방식
            # target 인자에 쓰렉드가 실행할 함수 지정. 여기선 run
            # 만약 쓰레드가 실행하는 함수에 입력 파라미터가 필요하면 args에 기입
            # 이때 args는 튜플로 파라미터를 전달.
            thread.setDaemon(True)
            # 이는 서브쓰레드가 데몬쓰레드인지 아닌지를 지정하는 것.
            # 데몬 쓰레드인 경우(true) 메인 쓰레드가 종료되면 같이 즉시 종료.
            thread.start()
            # 서브쓰레드 시작

            # 파이썬은 전역 인터프리터 락킹(Global Interpreter Lock) 때문에 
            # 특정 시점에 하나의 파이썬 코드만을 실행하게 되는데,
            # 이 때문에 파이썬은 실제 다중 CPU 환경에서 동시에 여러 파이썬 코드를 
            # 병렬로 실행할 수 없으며 인터리빙(Interleaving) 방식으로 코드를 분할하여 실행한다. 
            # 다중 CPU 에서 병렬 실행을 위해서는 다중 프로세스를 이용하는 multiprocessing 모듈을 사용한다.

        return ssh_submit

    tracker.submit(args.num_workers, args.num_servers,
                   fun_submit=ssh_submit,
                   pscmd=(' '.join(args.command)),
                   hostIP=args.host_ip)
    # 2, 1
    # 위에서 정의된 ssh_submit
    # ' '.join(list) : 앞서 launch에서 보내진 args.command는 리스트 형태. 이를 문자열로 변환한다.
    # 호스트 ip.