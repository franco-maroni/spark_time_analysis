
import re
import datetime
import sys
import os
import json
from functools import reduce

if __name__ == "__main__":
    import cfg
else:
    from . import cfg


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type not serializable")


def parse_date_time(d, t):
    """ Convert log time string format 17/05/09 08:17:37.669 into a Python datetime object
    Args:
        d (str): date in format 17/05/09
        t (str): time in format 08:17:37.669
    Returns:
        datetime: datetime object (ignore timezone)
    """
    milliseconds = int(t[9:12]) * 1000 if t[9:12] else 0
    return datetime.datetime(int(d[0:2]),
                             int(d[3:5]),
                             int(d[6:8]),
                             int(t[0:2]),
                             int(t[3:5]),
                             int(t[6:8]),
                             milliseconds)


def parse_spark_logline(logline):
    match = re.search(cfg.SPARK_LOG_PATTERN,  logline)
    if match is None:
       return {
            "timestamp": None,
            "loglevel": '',
            "classname": '',
            "description": ''
            }
    else:
        return {
            "timestamp":parse_date_time(match.group(1), match.group(2)),
            "loglevel": match.group(3),
            "classname": match.group(4),
            "description": match.group(5)
            }


def equals_fields(log, timestamp=None, loglevel=None, classname=None, description=None):
    return (timestamp is None or timestamp == log["timestamp"]) and \
           (loglevel is None or loglevel == log["loglevel"]) and \
           (classname is None or classname == log["classname"]) and \
           (description is None or description in log["description"])


def populate_stage_time_struct(msg, time_struct, job_info, first_ev):
    match_add_taskset = (re.search(cfg.get_log_fields(cfg.ADD_TASKSET_MSG)["descr"],
                                   msg["description"]),
                         "add_taskset")
    match_start_stage = (re.search(cfg.get_log_fields(cfg.START_STAGE_MSG)["descr"],
                                   msg["description"]),
                         "start_taskset")
    match_end_stage = (re.search(cfg.get_log_fields(cfg.END_STAGE_MSG)["descr"],
                                 msg["description"]),
                       "remove_taskset")
    match_end_task = re.search(cfg.get_log_fields(cfg.END_TASK_MSG)["descr"], msg["description"])

    match = list(filter(lambda x: x[0] is not None, [match_add_taskset,
                                                  match_start_stage,
                                                  match_end_stage]))
    if len(match) > 0:
        stage_id, label = match[0][0].group(1), match[0][1]
        print("{}\t\t{}-{}".format((msg["timestamp"] - first_ev).total_seconds() * 1000, stage_id, label))
        if stage_id not in time_struct:
            time_struct[stage_id] = {}
        time_struct[stage_id][label] = msg["timestamp"]
#    elif re.search(cfg.get_log_fields(cfg.END_JOB_MSG)["descr"], msg["description"]) is not None:
#        job_info["end_job"] = msg["timestamp"]
    elif match_end_task is not None:
        if "sum_of_task_durations_ta_master" not in time_struct[match_end_task.group(2)]:
            time_struct[match_end_task.group(2)]["sum_of_task_durations_ta_master"] = 0
        time_struct[match_end_task.group(2)]["sum_of_task_durations_ta_master"] += int(match_end_task.group(4))
        job_info["latest_event"] = msg["timestamp"]


def main(app_dir, app_name=None):
    stage_time_struct = {}
    job_time_struct = {}

    if app_name is None:
        app_name = app_dir.split(os.sep)[-1]

    # open spark log file
    print("opening {}...".format(app_dir + os.sep + 'app.dat'))
    with open(app_dir + os.sep + 'app.dat') as log_file:
        spark_log_lines = map(lambda x: parse_spark_logline(x), log_file.readlines())

    # open ta_executor log file
    print("opening {}...".format(app_dir + os.sep + 'app.json'))
    with open(app_dir + os.sep + 'app.json') as stages_file:
        stages = json.load(stages_file)

    # open config.json
    print("opening {}...".format(app_dir + os.sep + 'config.json'))
    with open(app_dir + os.sep + 'config.json') as spark_config_file:
        spark_config = json.load(spark_config_file)
        num_cores = spark_config["Control"]["CoreVM"] * spark_config["Control"]["MaxExecutor"]

    # get first event
    first_event = next(i for i in spark_log_lines if i["timestamp"] is not None)["timestamp"]
    print("FIRST EVENT", first_event)
    job_time_struct['latest_event'] = first_event

    # extract stage-specific times
    for i in spark_log_lines:
        populate_stage_time_struct(i, stage_time_struct, job_time_struct, first_event)

    for s, t in stage_time_struct.items():
        if s != "job_time_struct":
            t["add_to_end_taskset"] = (t["remove_taskset"] - t["add_taskset"]).total_seconds() * 1000
            t["start_to_end_taskset"] = (t["remove_taskset"] - t["start_taskset"]).total_seconds() * 1000
            t["add_to_start_taskset_overhead"] = (t["start_taskset"] - t["add_taskset"]).total_seconds() * 1000
            t["sum_of_task_durations_ta_executor"] = stages[s]["duration"]
            t["avg_task_duration_ta_executor"] = stages[s]["duration"]/stages[s]["numtask"]
            t["avg_stage_duration_ta_executor"] = stages[s]["duration"] / num_cores + 1
            t["avg_task_duration_ta_master"] = t["sum_of_task_durations_ta_master"] / stages[s]["numtask"]
            t["avg_stage_duration_ta_master"] = t["sum_of_task_durations_ta_master"] / num_cores + 1
            diff_sum_of_task_durations = float(t["sum_of_task_durations_ta_master"] - t["sum_of_task_durations_ta_executor"])
            diff_avg_task_duration = float(t["avg_task_duration_ta_master"] - t["avg_task_duration_ta_executor"])
            diff_avg_stage_duration = float(t["avg_stage_duration_ta_master"] - t["avg_stage_duration_ta_executor"])
            print(""" STAGE {} \t({}):
                  SUM_OF_TASK_DURATIONS:
                   - ta_executor:\t{}
                   - ta_master:\t{}
                   - diff:\t\t{}\t(+{:.2f} %)
                  AVG_TASK_DURATION:
                   - ta_executor:\t{}
                   - ta_master:\t{}
                   - diff:\t\t{}
                  AVG_STAGE_DURATION:
                   - ta_executor:\t{}
                   - ta_master:\t{}
                   - diff:\t\t{}
                  add_to_end_taskset:\t\t{}
                  start_to_end_taskset:\t\t{}
                  add_to_start_taskset_overhead:\t{}\t({:.2f} %)
                  """\
                .format(s,
                        stages[s]["name"],
                        t["sum_of_task_durations_ta_executor"],
                        t["sum_of_task_durations_ta_master"],
                        diff_sum_of_task_durations,
                        diff_sum_of_task_durations/t["sum_of_task_durations_ta_executor"]*100,
                        t["avg_task_duration_ta_executor"],
                        t["avg_task_duration_ta_master"],
                        diff_avg_task_duration,
                        t["avg_stage_duration_ta_executor"],
                        t["avg_stage_duration_ta_master"],
                        diff_avg_stage_duration,
                        t["add_to_end_taskset"],
                        t["start_to_end_taskset"],
                        t["add_to_start_taskset_overhead"],
                        t["add_to_start_taskset_overhead"]/t["add_to_end_taskset"]*100))

    add_to_start_taskset_overhead = reduce(lambda x, y: x + y, [z["add_to_start_taskset_overhead"] for z in stage_time_struct.values()])
    total_start_to_end_taskset = reduce(lambda x, y: x + y, [z["start_to_end_taskset"] for z in stage_time_struct.values()])
    total_add_to_end_taskset = reduce(lambda x, y: x + y, [z["add_to_end_taskset"] for z in stage_time_struct.values()])
    total_ta_executor_stages = reduce(lambda x, y: x + y, [z["avg_stage_duration_ta_executor"] for z in stage_time_struct.values()])
    total_tasks_only_ta_master = reduce(lambda x, y: x + y, [z["avg_stage_duration_ta_master"] for z in stage_time_struct.values()])

    job_time_struct["start_job"] = stage_time_struct['0']["start_taskset"]
    job_duration = (job_time_struct["latest_event"] - job_time_struct["start_job"]).total_seconds() * 1000
    job_time_struct["total_add_to_start_taskset_overhead"] = add_to_start_taskset_overhead
    job_time_struct["total_start_to_end_taskset"] = total_start_to_end_taskset
    job_time_struct["total_add_to_end_taskset"] = total_add_to_end_taskset
    job_time_struct["total_ta_executor_stages"] = total_ta_executor_stages
    job_time_struct["total_tasks_only_ta_master"] = total_tasks_only_ta_master
    job_time_struct["actual_job_duration"] = job_duration

    print("TOTAL TASKS_ONLY:\t{}".format(total_start_to_end_taskset))
    print("TOTAL OVERHEAD:\t\t{}\t({:.2f} %)".format(add_to_start_taskset_overhead, add_to_start_taskset_overhead / total_add_to_end_taskset * 100))
    print("TOT WITH OVERHEAD:\t{}".format(total_add_to_end_taskset))
    print("JOB DURATION:\t\t{}".format(job_duration))
    print("TOTAL TA_EXECUTOR:\t\t{} - {}".format(stages['0']['totalduration']/num_cores, total_ta_executor_stages))


    SPARK_CONTEXT = {
        "app_name" : "{}_c{}_t{}_{}l_d{}_tc_{}_n_rounds_{}".format(app_name,
                                                                   num_cores,
                                                                   cfg.TIME_BOUND,
                                            "no_" if cfg.NO_LOOPS else "",
                                                                   job_duration,
                                            "parametric" if cfg.PARAMETRIC_TC else "by20",
                                            "by2"),
    #        "app_dir_acceleration_0_1000_c48_t40_no-l_d133000_tc_parametric_forall_nrounds_TEST",
        "verification_params" :
        {
            "plugin": cfg.PLUGIN,
            "time_bound" : cfg.TIME_BOUND,
            "parametric_tc": cfg.PARAMETRIC_TC,
            "no_loops" : cfg.NO_LOOPS
        },
        "tot_cores" : num_cores,
        "analysis_type" : "feasibility",
        "deadline" : job_duration,
        "max_time" : job_duration,
        "tolerance": cfg.TOLERANCE,
        "stages": stages
    }

    out_path_context = app_dir+os.sep+app_name+'_context.json'
    print("dumping to {}".format(out_path_context))
    with open(out_path_context, 'w') as outfile:
        json.dump(SPARK_CONTEXT, outfile, indent=4, sort_keys=True)

    out_path_time_structs = app_dir+os.sep+app_name+'_time_analysis.json'
    print("dumping to {}".format(out_path_time_structs))
    with open(out_path_time_structs, 'w') as outfile:
        json.dump({"stages": stage_time_struct, "job": job_time_struct},
                  outfile, indent=4, sort_keys=True, default=json_serial)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_app_dir = sys.argv[1].strip(os.sep)
        input_app_name = sys.argv[2] if len(sys.argv) > 2 else input_app_dir.split(os.sep)[-1]
    else:
        print('ERROR: You must provide at least one argument')
        sys.exit(0)
    main(app_dir=input_app_dir, app_name=input_app_name)