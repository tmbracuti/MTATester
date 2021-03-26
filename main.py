import requests
import urllib3
import properties
import tenants
import time
import sys
import logging.handlers
import callback_handler
import json
import pyodbc
import tornado.options
import numpy as np
import parallel_driver

from tornado.options import define, options

# define the available cmd-line options with defaults
define("file", default="test_inputs.txt", help="the test file", type=str)
define("keep", default=0, help="keep the timings=1, dump them=0", type=int)
define("failout", default=0, help="die on failure=1, keep going=0", type=int)
define("mode", default=0, help="synchronous=0, parallel by tenant=1", type=int)

# disable insecure messages
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# global logging config
myLogger = logging.getLogger('MTATester')
myLogger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler('./mta_testing.log', maxBytes=1000000, backupCount=5)
log_format = "%(asctime)s : %(name)-12s: %(levelname)s | %(message)s"
handler.setFormatter(logging.Formatter(log_format))
myLogger.addHandler(handler)

# globals
# a map of request Id to the result body
g_Responses = {}
tenant_conn_map = {}


def is_response_available(request_id: str):
    return request_id in g_Responses


def get_response(request_id: str):
    if is_response_available(request_id):
        return g_Responses[request_id]
    else:
        return None


def count_active_testlines(test_lines):
    active_lines = 0
    for line in test_lines:
        line = line.rstrip()
        if line == '':
            continue
        if line[0] != '#':
            active_lines += 1
    return active_lines


def load_tenant_conn_map(props):
    tenants.get_tenant_map(tenant_conn_map, props)
    print(f'loaded connection strings for {len(tenant_conn_map.keys())} tenants')


# can be run as
# python main.py or python main.py --file=<filename>
def mainline():
    tornado.options.parse_command_line()
    test_file_name = options.file
    keep_timings = options.keep
    failout = options.failout
    mode = options.mode
    rbase = str(int(time.time()))
    print(f'Batch-Test base Id is {rbase}')
    props = properties.Properties("./mta.properties")
    if not props.is_ready():
        print('mta.properties file failed to load...exiting')
        myLogger.error('mta.properties file failed to load...exiting')
        sys.exit(1)
    load_tenant_conn_map(props)
    cb = callback_handler.MockSnowHandler(props, g_Responses)
    cb.daemon = True
    cb.start()

    with (open(test_file_name)) as f:
        test_inputs = f.readlines()

    print(f'{count_active_testlines(test_inputs)} test lines read from {test_file_name}')

    if mode == 0:
        print('performing synchronous test plan')
        sync_test(props, rbase, test_inputs, keep_timings, int(failout), 0)
        print('all test items sent')
    else:
        print('performing parallel request streams (parallel by tenant)')
        parallel_test(props, rbase, test_inputs, keep_timings, int(failout))


def dispatch_test_request(rid, test_body: str, uri_str: str, user: str, pwd: str):
    # print(test_body)
    headers = {'Content-Type': 'application/json;charset=UTF-8',
               'Connection': 'Keep-Alive', 'Accept-Encoding': 'gzip,deflate, br'}
    
    o = json.loads(test_body)
    workflow = o["WORKFLOW"]
    print(f'\nsending request {rid} for {workflow}')
    r = requests.post(uri_str, verify=False, data=test_body, headers=headers, auth=(user, pwd))
    t1 = time.perf_counter()
    if r.status_code not in [200, 202]:
        print(rid, 'test failed on HTTP level with response code ', r.status_code, r.text)
        return -1
    else:
        print(rid, r.status_code, r.text)
        return t1


def add_round_trip_data(workflow_name, rt_secs: float, rts):
    if workflow_name in rts:
        the_list = rts[workflow_name]
        assert isinstance(rt_secs, float)
        the_list.append(rt_secs)
    else:
        the_list = []
        assert isinstance(rt_secs, float)
        the_list.append(rt_secs)
        rts[workflow_name] = the_list


def add_to_bucket(buckets: {}, line: str, tenant: str):
    if tenant in buckets:
        lines = buckets[tenant]
        lines.append(line)
    else:
        lines = [line]
        buckets[tenant] = lines


def parallel_test(props, rbase, test_inputs, keep_timings, failout: int):
    # for sticklers: this is NOT parallel execution as we are on one core unless we convert to multiprocess
    # as is, we are just concurrent
    # create a bucket for each tenant
    buckets = {}
    for tline in test_inputs:
        tline = tline.rstrip()
        if tline == '' or tline[0] == '#':
            continue
        obj = json.loads(tline)
        the_tenant = obj['TENANTID']
        add_to_bucket(buckets, tline, the_tenant)
    worker_list = []
    for i, tenant in enumerate(buckets, 1):
        # props, rbase, test_inputs, keep_timings, fail_out, tid, test_function
        print(f'initializing test driver for tenant {tenant}')
        t = parallel_driver.PDriver(props, f'{tenant}_{rbase}', buckets[tenant], keep_timings, failout, i, sync_test)
        t.daemon = False
        t.start()
        worker_list.append(t)
    print(f'{len(worker_list)} tenant test-workers are loaded')
    for t in worker_list:
        t.join()
    map_of_rt_maps = {}  # maps tenant to their round_trip data
    for t in worker_list:
        if t.res_tuple is not None:
            last_tenant_handled, tests_run, test_success, test_fail, rt_map = t.res_tuple
            map_of_rt_maps[last_tenant_handled] = rt_map
            print(f'{last_tenant_handled} -> tests run: {tests_run}\tsuccesses: {test_success}\tfails: {test_fail}')
    print()
    print('Performance statistics by tenant and workflow -- ')
    for tenant in map_of_rt_maps:
        round_trips = map_of_rt_maps[tenant]
        if len(round_trips) == 0:
            print(f'no data for tenant: {tenant}! This is likely a major malfunction.\n')
            continue
        for wf in round_trips:
            lst = round_trips[wf]
            if len(lst) == 1:
                print(f'{tenant}: workflow {wf.upper()} mean round-trip secs: {lst[0]:.2f}')
            else:
                arr = np.array(lst)
                print(f'{tenant}: workflow {wf.upper()} mean round-trip secs: {arr.mean():.2f}, stddev={arr.std(ddof=1):.2f}')
        print()

    print("all parallel test workers are complete")
    print("parallel test run is complete...exiting")


def sync_test(props, rbase, test_inputs, keep_timings: int, failout: int, tid: int):
    last_tenant_handled = ''
    suf = 1
    round_trips = {}
    # set Starfish MT REST API credentials
    user = props.get_value('sf_user', '')
    pwd = props.get_value('sf_pwd', '')
    tests_run = 0
    test_success = 0
    test_fail = 0
    abort_flag = 0
    for tline in test_inputs:
        if abort_flag == 1:
            print('############### abort on fail flag is set, ending test run ###############')
            break
        tline = tline.rstrip()
        if tline == '' or tline[0] == '#':
            continue

        request_id = "REQ-%s.%d" % (rbase, suf)
        suf += 1
        tline = tline.replace('[x]', request_id)
        r_obj = json.loads(tline)
        the_tenant = r_obj['TENANTID']
        last_tenant_handled = the_tenant  # ignored on sync run, will be the tenant focused on in asycch
        the_wf = r_obj['WORKFLOW']
        t1 = dispatch_test_request(request_id, tline,
                                   props.get_value("sf_api", "https://192.168.52.52/MTWebService/api/User"), user, pwd)
        tests_run += 1
        if t1 == -1:
            print(f'dispatch failed for request: {request_id}')
            test_fail += 1
            if failout == 1:
                abort_flag = 1
            continue
        time.sleep(3)
        resp = None
        attempts = 1
        while attempts < 12:
            print(f'{attempts} - looking for a callback response for {request_id}')
            resp = get_response(request_id)
            if resp is not None:
                t2 = time.perf_counter()
                add_round_trip_data(the_wf, t2 - t1, round_trips)
                o = json.loads(to_str(resp))
                b_success = o["WorkflowSuccess"]
                if not b_success:
                    print(f"{request_id} was a failure - return time was {t2 - t1:.2f}")
                    test_fail += 1
                    if failout == 1:
                        abort_flag = 1
                    print(f"{request_id} failed! Saving to file")
                    write_fail_report_file(request_id, o, resp, the_tenant)
                else:
                    print(f"{request_id} was successful - return time was {t2 - t1:.2f}")
                    test_success += 1
                break
            else:
                attempts += 1
                # quick failout check for Failure (no call back will come)
                status = get_request_status(the_tenant, request_id)
                if status is not None and status.lower() == 'failure':
                    test_fail += 1
                    if failout == 1:
                        abort_flag = 1
                    resp = ''
                    print(f'in-run DB check of status for {request_id} was {status}, no callback will come')
                    msg = get_request_message(the_tenant, request_id)
                    if msg is not None:
                        write_fail_file(request_id, msg, the_tenant)  # get the message column of sf_request
                    break
                time.sleep(5)  # seconds to wait
        if resp is None:
            print(f'gave up on response for {request_id}, checking status table...')
            status = get_request_status(the_tenant, request_id)

            if status is None:
                print(f'no status found for {request_id}, assuming that it failed (hard)')
                test_fail += 1
                if failout == 1:
                    abort_flag = 1
            else:
                print(f'status found for {request_id} was {status}')
                if status.lower() == 'success':
                    test_success += 1
                else:
                    test_fail += 1
                    if failout == 1:
                        abort_flag = 1
                    msg = get_request_message(the_tenant, request_id)  # get the message column of sf_request
                    if msg is not None:
                        write_fail_file(request_id, msg, the_tenant)
            continue
    print(f"\nTEST RUN COMPLETE\ntest runs={tests_run}, test successes={test_success}, test fails={test_fail}\n")
    if len(round_trips) > 0:
        print('\nClient perspective statistics:')
        for wf in round_trips:
            lst = round_trips[wf]
            if len(lst) == 1:
                print(f'Workflow {wf} mean round-trip secs: {lst[0]:.2f}')
            else:
                arr = np.array(lst)
                print(f'Workflow {wf} mean round-trip secs: {arr.mean():.2f}, stddev={arr.std(ddof=1):.2f}')
        if keep_timings == 1:
            write_timings(rbase, round_trips, tid)
    if tid != 0:  # asynchronous run
        return last_tenant_handled, tests_run, test_success, test_fail, round_trips
    else:
        return None


def write_timings(rbase: str, round_trips: {}, tid: int):
    fname = f'{rbase}_timings_{tid}.csv'
    with(open(fname, 'w')) as fout:
        for wf in round_trips:
            lst = round_trips[wf]
            for item in lst:
                line = f'{wf},{str(item)}\n'
                fout.write(line)
    print(f'Timing data written to file: {fname}')


def write_fail_report_file(request_id, obj, raw_resp, tenant_name):  # for actual failed callback (e.g. Final Failure status)
    with(open(f"{tenant_name}_{request_id}.txt", 'w')) as f:
        for t in obj["SuccessfulTasks"]:
            f.write("Success " + t["TaskName"] + "\t" + t["TaskMessage"] + "\n")
        f.write("\n")
        for t in obj["FailedTasks"]:
            f.write("Failed " + t["TaskName"] + "\t" + t["TaskMessage"] + "\n")
            print("Failed " + t["TaskName"] + "\t" + t["TaskMessage"])
        f.write('\nFull response:\n')
        f.write(to_str(raw_resp))
        f.write('\n')
        f.flush()


def write_fail_file(request_id, msg, tenant_name):  # for failure without callback data (e.g. Failure status)
    try:
        msg_o = json.loads(msg)
        with(open(f"{tenant_name}_{request_id}.txt", 'w')) as f:
            for t in msg_o["SuccessTasks"]:
                f.write("Success " + t["TaskName"] + "\t" + t["TaskReportText"] + "\n")
            f.write("\n")
            for t in msg_o["FailTasks"]:
                f.write("Failed " + t["TaskName"] + "\t" + t["TaskReportText"] + "\n")
                print("Failed " + t["TaskName"] + "\t" + t["TaskReportText"])
            f.write('\nFull response:\n')
            f.write(msg)
            f.write('\n')
            f.flush()
    except Exception as fe:
        print(f'failed to create failure file for {request_id} -> {str(fe)}')


def get_request_message(the_tenant, request_id):
    if the_tenant not in tenant_conn_map:
        print(f'tenant {the_tenant} not found in connection map, assume failure')
        return None
    else:
        conn_str = tenant_conn_map[the_tenant]
        try:
            qry = f"select message from sf_request where request='{request_id}' and message is not  null"
            c = pyodbc.connect(conn_str)
            cur = c.cursor()
            cur.execute(qry)
            rows = cur.fetchall()
            msg = None
            for row in rows:
                msg = row[0]
            c.close()
            return msg
        except pyodbc.Error:
            msg = None
            return msg


def get_request_status(the_tenant, request_id):
    if the_tenant not in tenant_conn_map:
        print(f'tenant {the_tenant} not found in connection map, assume failure')
        return None
    else:
        conn_str = tenant_conn_map[the_tenant]
        try:
            qry = f"select status from sf_request where request='{request_id}'"
            c = pyodbc.connect(conn_str)
            cur = c.cursor()
            cur.execute(qry)
            rows = cur.fetchall()
            actual_status = None
            for row in rows:
                actual_status = row[0]
                # print(f'[DB:{actual_status}]')
            c.close()
            return actual_status
        except Exception as db_e:
            print(f'error checking status, assuming failure: {str(db_e)}')
            return None


def to_str(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        value = bytes_or_str.decode('utf-8')
    else:
        value = bytes_or_str
    return value


if __name__ == '__main__':
    mainline()
