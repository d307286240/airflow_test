# -*- coding: UTF-8 -*-
# !/usr/bin/env python
import time
from datetime import datetime, timedelta
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from urllib.parse import urlparse
from typing import Sequence
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class ImportStarRocks2Operator(BaseOperator):

    # column type mapping
    type_mapping = {'string': 'string', 'int': 'int', 'bigint': 'bigint', 'float': 'float', 'double': 'double'}

    format_mapping = {'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': 'parquet',
                      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat': 'orc',
                      'org.apache.hadoop.mapred.TextInputFormat': 'CSV'}

    MAX_PART_COUNT = 32767

    max_task_size = 20

    lock = threading.Lock()

    tasks_num_not_full = threading.Condition(lock)

    current_exec_task_num = 0

    import_all_table = []

    starrocks_part_names = ['ds', 'run_ds', 'activation_date', 'dt', 'month']

    hive_mapping_db = "hive_mapping"

    @apply_defaults
    def __init__(self,
                 starrocks_hook,
                 metastore_hook,
                 mysql_ops,
                 *args, **kwargs):
        super(ImportStarRocks2Operator, self).__init__(*args, **kwargs)
        self.starrocks = starrocks_hook
        self.meta_hook = metastore_hook
        self.mysql_ops = mysql_ops
        self.meta_client = self.meta_hook.get_conn()


    def mysql_ops_execSql(self, sql, db=None, is_commit = False):
        print(sql)
        self.lock.acquire()
        try:
            with self.mysql_ops.get_conn() as conn:
                if db is not None:
                    conn.select_db(db)
                cursor = conn.cursor()
                cursor.execute(sql)
                if is_commit is True:
                    conn.commit()
            return cursor
        finally:
            self.lock.release()

    def execute(self, context):
        sql = "select db_name,table_name,part_names,id, import_type from starrocks_sync_log where status_flag = 0 order by create_time asc"
        cursor = self.mysql_ops_execSql(sql)
        datas = cursor.fetchall()
        if datas is not None:
            all_task = []
            table_executing = []
            queue_obj = Queue()
            for q_data in datas:
                queue_obj.put(q_data)
            with ThreadPoolExecutor(max_workers=20) as pool:
                #for data in datas:
                while not queue_obj.empty():
                    data = queue_obj.get()
                    db_name = data[0]
                    table_name = data[1]
                    part_names = data[2]
                    id = data[3]
                    import_type = data[4]
                    whole_table = "%s.%s" % (db_name, table_name)
                    if whole_table in table_executing:
                        print(f"{whole_table} is executing,id is {id},move to bottom of queue")
                        queue_obj.put(data)
                        time.sleep(0.5)
                    else:
                        if whole_table in self.import_all_table:
                            sql = "update starrocks_sync_log set status_flag = 4,update_time=unix_timestamp(now()) where id = %s" % (id, )
                            self.mysql_ops_execSql(sql, is_commit=True)
                        else:
                            table_executing.append(whole_table)
                            sql = "update starrocks_sync_log set status_flag = 3,update_time=unix_timestamp(now()) where id = %s" % (id, )
                            self.mysql_ops_execSql(sql, is_commit=True)
                            ######import start
                            all_task.append(pool.submit(self.import_table, id, db_name, table_name, part_names, table_executing, import_type))
                    ######import end
            for future in as_completed(all_task):
                print('完成：', future.result())

    def decrement_task_num(self, rusult):
        with self.tasks_num_not_full:
            self.current_exec_task_num -= 1
            self.tasks_num_not_full.notify()

    def import_table(self, id, db_name, table_name, part_names, table_executing, import_type):
        is_success = True
        try:
            self.create_table_import_all(_db=db_name, _table=table_name, import_type=import_type)
            if part_names != '' and part_names == '-1':         # -1 为无分区表更新
                self.import_starrocks_by_swap_table(db_name, table_name, import_type)
            elif part_names != '' and part_names != "All":
                has_ds = []
                whole_table = "%s.%s" % (db_name, table_name)

                lpns = part_names.split(',')

                sub_tasks = []
                with ThreadPoolExecutor(max_workers=20) as pool:
                    for part_name in lpns:
                        if whole_table in self.import_all_table:
                            break
                        part_list = []
                        fqzds = part_name.split('/')
                        is_jump_out = False
                        for i, fqzd in enumerate(fqzds):
                            #fqzd = fqzds[0]
                            part_val = fqzd[fqzd.index('=') + 1:]
                            p_name = fqzd[:fqzd.index('=')]
                            # include run_ds= and ds=
                            if i == 0 and p_name in self.starrocks_part_names:
                                try:
                                    ds = datetime.strptime(part_val, '%Y-%m-%d')
                                    if ds in has_ds:
                                        is_jump_out = True
                                except Exception as e:
                                    #日期不能解析，不导入
                                    is_jump_out = True
                                    break
                            part_list.append(part_val)
                        if is_jump_out is True:
                            continue
                        part_vals = tuple(part_list)
                        with self.tasks_num_not_full:
                            while self.current_exec_task_num >= self.max_task_size:
                                self.tasks_num_not_full.wait()
                            self.current_exec_task_num += 1
                            fu = pool.submit(self.importStarRocks, db_name, table_name, part_vals, has_ds, import_type)
                            sub_tasks.append(fu)
                            fu.add_done_callback(self.decrement_task_num)
                for future in as_completed(sub_tasks):
                    print('子任务完成：', future.result())
                        #self.importStarRocks(_db=db_name, _table=table_name, part_vals=part_vals, has_ds=has_ds, import_type=import_type)

        except Exception as e:
            is_success = False
            emessage = str(e)
            failed_sql = "update starrocks_sync_log set status_flag = 2,update_time=unix_timestamp(now()),error_message = '%s' where id = %s" % (
            emessage[:1000].replace("'", "''"), id,)
            self.mysql_ops_execSql(failed_sql, is_commit=True)
            raise e
        finally:
            whole_table = "%s.%s" % (db_name, table_name)
            table_executing.remove(whole_table)
        if is_success is True:
            success_sql = "update starrocks_sync_log set status_flag = 1,update_time=unix_timestamp(now()) where id = %s" % (id,)
            self.mysql_ops_execSql(success_sql, is_commit=True)
        return db_name+"."+table_name

    #使用交换表导入数据，先导入临时表，然后替换 # 完成insert插入方式
    def import_starrocks_by_swap_table(self, db_name, table_name, import_type):
        self.import_all_table.append(db_name + "." + table_name)
        # import All data
        tb = self.get_table_info(db_name, table_name)
        partition_keys = tb.partitionKeys
        search_tmp_sql = "select count(*) from information_schema.tables where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s_tmp'" % (
            db_name, table_name)
        tmp_cursor = self.execSql(search_tmp_sql)
        tmp_data = tmp_cursor.fetchone()
        del_swap_table_sql = "drop table %s.%s_tmp" % (db_name, table_name)
        if tmp_data[0] != 0:
            self.execSql(del_swap_table_sql)
        # create swap table
        create_swap_table_sql = "create table %s.%s_tmp like %s.%s" % (db_name, table_name, db_name, table_name)
        self.execSql(create_swap_table_sql)
        if import_type == 'broker':
            self.lock.acquire()
            try:
                with self.meta_client as c:
                    pars = c.get_partitions(db_name, table_name, ImportStarRocks2Operator.MAX_PART_COUNT)
            finally:
                self.lock.release()
            if pars is not None and len(pars) > 0:
                first_par_values = []
                for pt in pars:
                    pt_values = pt.values
                    first_p_v = pt_values[0]
                    if first_p_v in first_par_values:
                        continue
                    first_par_values.append(first_p_v)
                    pt_location = self.hdfs_path_fix(pt.sd.location)

                    # 分区字段多个时，只用第一个分区名
                    if len(partition_keys) > 1:
                        rpl = pt_location[pt_location.rfind(pt_values[0]) + len(pt_values[0]):]
                        pt_location = pt_location[:pt_location.rfind(pt_values[0]) + len(pt_values[0])]
                        rpl_len = len(rpl)
                        for i, s in enumerate(rpl):
                            if s == '/' and i + 1 < rpl_len:
                                pt_location += "/*"

                    self.actual_import(db_name, table_name + "_tmp", tb.sd.cols, pt, pt_values, partition_keys, pt_location)
            else:
                label = "%s_%s" % (table_name, 0)
                self.import_all_data(db_name, table_name + "_tmp", label, partition_keys, tb)
        else:
            label = table_name
            self.importStarRocksDataByInsert(self.hive_mapping_db, db_name+"_"+table_name, db_name, table_name+"_tmp", label, 0)
        # swap
        swap_table_sql = "ALTER TABLE %s.%s SWAP WITH %s_tmp" % (db_name, table_name, table_name)
        self.execSql(swap_table_sql)
        self.execSql(del_swap_table_sql)


    def generate_hive_mapping_table_sql(self, _db, _table):
        tb = self.get_table_info(_db, _table)

        hive_mapping_table = _db+"_"+_table

        createTableSql = "CREATE EXTERNAL TABLE " + self.hive_mapping_db + "." + hive_mapping_table + " ("
        fields = tb.sd.cols
        fields_sql = ""
        for field in fields:
            field_name = field.name
            field_type = field.type
            starRocks_type = self.type_mapping.get(field_type)
            if starRocks_type is None:
                starRocks_type = field_type
            fields_sql += "\n`" + field_name + "` " + starRocks_type + ","

        partition_keys = tb.partitionKeys
        if len(partition_keys) > 0:
            is_create_par = False
            for fs in partition_keys:
                if is_create_par is False and (fs.name in self.starrocks_part_names):
                    fields_sql = fields_sql + "\n" + fs.name + " date,"
                else:
                    fields_sql = fields_sql + "\n`" + fs.name + "` " + fs.type + ","


        createTableSql = createTableSql + fields_sql

        createTableSql = createTableSql[:-1]
        createTableSql += "\n)\nENGINE=HIVE\n"
        createTableSql += '\nPROPERTIES("database" = "'+_db+'","table" = "'+_table+'","resource" = "idc_bi_hive")'
        return createTableSql

    def generateStarRocksTable(self, _db, _table, distributed_name=''):
        tb = self.get_table_info(_db, _table)

        self.lock.acquire()
        try:
            with self.meta_client as c:
                pars = c.get_partitions(_db, _table, ImportStarRocks2Operator.MAX_PART_COUNT)
        finally:
            self.lock.release()

        createTableSql = "CREATE TABLE " + _db + "." + _table + " ("
        fields = tb.sd.cols
        first_name = ""
        is_sel_fir_con = True
        fields_sql = ""
        first_col = ""
        for field in fields:
            field_name = field.name
            field_type = field.type
            starRocks_type = self.type_mapping.get(field_type)
            if starRocks_type is None:
                starRocks_type = field_type
            if is_sel_fir_con is True and starRocks_type == 'string':
                is_sel_fir_con = False
                first_name = field_name
                first_col = "\n`" + field_name + "` " + starRocks_type + ","
            else:
                fields_sql += "\n`" + field_name + "` " + starRocks_type + ","

        partNames = ""
        partition_keys = tb.partitionKeys
        if len(partition_keys) > 0:
            is_create_par = False
            for fs in partition_keys:
                if is_create_par is False and (fs.name in self.starrocks_part_names):
                    is_create_par = True
                    partNames += fs.name + ","
                    if is_sel_fir_con is True:
                        is_sel_fir_con = False
                        first_name = fs.name
                        first_col = "\n" + fs.name + " date,"
                    else:
                        fields_sql = fields_sql + "\n" + fs.name + " date,"
                # elif fs.name == 'hour':
                #     partNames += fs.name + ","
                #     fields_sql = fields_sql + "\n" + fs.name + " int,"
                else:
                    if is_sel_fir_con is True and (fs.type == 'string' or fs.type == 'date'):
                        is_sel_fir_con = False
                        first_name = fs.name
                        first_col = "\n`" + fs.name + "` " + fs.type + ","
                    else:
                        fields_sql = fields_sql + "\n`" + fs.name + "` " + fs.type + ","
            if partNames != "":
                partNames = partNames[:-1]

        if first_col == "":
            raise Exception("first column starRocks not support")
        createTableSql = createTableSql + first_col + fields_sql

        createTableSql = createTableSql[:-1]
        createTableSql += "\n)\nENGINE=OLAP\n"
        if len(partNames) > 0:
            createTableSql += "PARTITION BY RANGE(" + partNames + ")"
            #ALTER TABLE datamining.sukan_kyy_retention_result_d ADD  PARTITION p20221114 VALUES [("2022-11-14"), ("2022-11-15"))
            par_str = ''
            if len(pars) > 0:
                ds_list = []
                for pt in pars:
                    pt_values = pt.values
                    ds_value = pt_values[0]
                    try:
                        ds_v = datetime.strptime(ds_value, '%Y-%m-%d')
                    except Exception as e:
                        continue
                        print("ds日期不合格")
                    if ds_value in ds_list:
                        continue
                    else:
                        ds_list.append(ds_value)
                    par_name = ""
                    nex_val = ""
                    start_val = ""
                    next_par = self.next_p(partition_keys, pt_values)
                    if next_par is not None:
                        #for i, fs in enumerate(partition_keys):
                        #must be first
                        if partition_keys[0].name in self.starrocks_part_names:
                            par_name += pt_values[0].replace('-', '')
                            start_val += pt_values[0] + "\",\""
                            nex_val += next_par[0] + "\",\""
                            print(nex_val)
                            # elif fs.name == 'hour':
                            #     par_name += part_vals[i]
                            #     start_val += part_vals[i] + "\",\""
                            #     nex_val += next_par[1] + "\",\""
                        start_val = start_val[:-3]
                        nex_val = nex_val[:-3]
                        par_str += "\n PARTITION p" + par_name + " VALUES [(\"" + start_val + "\"), (\"" + nex_val + "\")),"
                par_str = par_str[:-1] + "\n"

            createTableSql += "\n(" + par_str + ")"

        createTableSql += "\nDISTRIBUTED BY HASH(`" + first_name + "`) BUCKETS 12"
        createTableSql += '\nPROPERTIES("replication_num" = "1");'
        return createTableSql

    def createDatabase(self, _db):
        sql = "select count(*) from information_schema.SCHEMATA where SCHEMA_NAME = '%s'" % _db
        cursor = self.execSql(sql)
        data = cursor.fetchone()
        if data[0] == 0:
            self.execSql("create database " + _db)

    def createTable(self, _db, _table, import_type):

        if import_type == 'insert':#创建hive映射表
            mapping_table = _db+"_"+_table
            hive_mapping_sql = "select count(*) from information_schema.tables where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'" % (
                self.hive_mapping_db, mapping_table)
            mapping_cursor = self.execSql(hive_mapping_sql)
            mapping_data = mapping_cursor.fetchone()
            if mapping_data[0] == 0:
                create_mapping_sql = self.generate_hive_mapping_table_sql(_db, _table)
                self.execSql(create_mapping_sql)

        #创建内部表
        self.createDatabase(_db)
        sql = "select count(*) from information_schema.tables where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'" % (
        _db, _table)
        cursor = self.execSql(sql)
        data = cursor.fetchone()
        if data[0] == 0:
            createsql = self.generateStarRocksTable(_db, _table)
            self.execSql(createsql)
            return 1
        else:
            #检查是否有新增字段
            is_new_col_sql = "select COLUMN_NAME from information_schema.columns where table_name = '%s' and table_schema = '%s' order by ORDINAL_POSITION" % (_table, _db, )
            cursor = self.execSql(is_new_col_sql)
            col_data = cursor.fetchall()
            cols = []
            last_col = ""
            for col in col_data:
                cl = col[0].lower()
                cols.append(cl)
                last_col = cl
            tb = self.get_table_info(_db, _table)
            fields = tb.sd.cols
            for field in fields:
                field_name = field.name
                field_type = field.type
                if field_name.lower() not in cols:
                    add_col_sql = "ALTER TABLE %s.%s ADD COLUMN %s %s after %s" % (_db, _table, field_name, self.type_mapping.get(field_type), last_col)
                    self.execSql(add_col_sql)
                    last_col = field_name
                    print(f"add column:{_db}.{_table} add {field_name}")
                    time.sleep(5)
            partition_keys = tb.partitionKeys
            if len(partition_keys) > 0:
                for fs in partition_keys:
                    field_name = fs.name
                    field_type = fs.type
                    if field_name.lower() not in cols:
                        add_col_sql = "ALTER TABLE %s.%s ADD COLUMN %s %s after %s" % (_db, _table, field_name, self.type_mapping.get(field_type), last_col)
                        self.execSql(add_col_sql)
                        last_col = field_name
                        print(f"add column:{_db}.{_table} add {field_name}")
            return 0


    def execSql(self, sql, db=None):
        print(sql)
        self.lock.acquire()
        try:
            with self.starrocks.get_conn() as conn:
                if db is not None:
                    conn.select_db(db)
                cursor = conn.cursor()
                cursor.execute(sql)
            return cursor
        finally:
            self.lock.release()

    def create_partition(self, _db, _table, part_vals, is_temp=False):
        tb = self.get_table_info(_db, _table)
        partition_keys = tb.partitionKeys
        temporary = ""
        if is_temp is True:
            temporary = "TEMPORARY"


        par_name = ""
        nex_val = ""
        start_val = ""
        next_par = self.next_p(partition_keys, part_vals)
        if next_par is not None:
            #for i, fs in enumerate(partition_keys):
            #must be first
            if partition_keys[0].name in self.starrocks_part_names:
                par_name += part_vals[0].replace('-', '')
                start_val += part_vals[0] + "\",\""
                nex_val += next_par[0] + "\",\""
                print(nex_val)
                # elif fs.name == 'hour':
                #     par_name += part_vals[i]
                #     start_val += part_vals[i] + "\",\""
                #     nex_val += next_par[1] + "\",\""
            start_val = start_val[:-3]
            nex_val = nex_val[:-3]

            if is_temp is True:
                par_name = par_name + "_tmp"
                search_temppar_sql = "SHOW TEMPORARY PARTITIONS FROM %s.%s where PartitionName = 'p%s'" % (_db, _table, par_name)
                cursor = self.execSql(search_temppar_sql)
                data = cursor.fetchone()
                if data is not None:
                    delete_temppar_sql = "ALTER TABLE %s.%s DROP TEMPORARY PARTITION p%s" % (_db, _table, par_name)
                    self.execSql(delete_temppar_sql)

            create_par_sql = "ALTER TABLE %s.%s ADD %s PARTITION p%s VALUES [(\"%s\"), (\"%s\"))"
            create_par_sql = create_par_sql % (_db, _table, temporary, par_name, start_val, nex_val)
            try:
                self.execSql(create_par_sql)
            except Exception as e:
                print(e)
        return "p" + par_name

    def create_table_import_all(self, _db, _table, import_type):
        is_create = self.createTable(_db, _table, import_type)
        if is_create == 1:
            if import_type == 'insert':
                mapping_table = _db+"_"+_table
                self.importStarRocksDataByInsert(self.hive_mapping_db, mapping_table, _db, _table, _table, 0)
            elif import_type == 'broker':
                tb = self.get_table_info(_db, _table)
                partition_keys = tb.partitionKeys
                #导入所有数据
                self.lock.acquire()
                try:
                    with self.meta_client as c:
                        pars = c.get_partitions(_db, _table, ImportStarRocks2Operator.MAX_PART_COUNT)
                finally:
                    self.lock.release()

                is_parttable = self.is_part_table(partition_keys)

                #导入全部数据
                if pars is not None and len(pars) > 0:
                    first_par_values = []
                    sub_tasks = []
                    with ThreadPoolExecutor(max_workers=20) as pool:
                        for pt in pars:
                            pt_values = pt.values
                            first_p_v = pt_values[0]
                            if is_parttable is True:
                                try:
                                    ds_v = datetime.strptime(first_p_v, '%Y-%m-%d')
                                except Exception as e:
                                    continue
                                    print("分区日期不合格")
                            if first_p_v in first_par_values or first_p_v == '__HIVE_DEFAULT_PARTITION__':
                                continue
                            first_par_values.append(first_p_v)
                            pt_location = self.hdfs_path_fix(pt.sd.location)
                            #分区字段多个时，只用第一个分区名
                            if len(partition_keys) > 1:
                                rpl = pt_location[pt_location.rfind(pt_values[0])+len(pt_values[0]):]
                                pt_location = pt_location[:pt_location.rfind(pt_values[0])+len(pt_values[0])]
                                rpl_len = len(rpl)
                                for i, s in enumerate(rpl):
                                    if s == '/' and i+1 < rpl_len:
                                        pt_location += "/*"

                            fu = pool.submit(self.actual_import, _db, _table, tb.sd.cols, pt, pt_values, partition_keys, pt_location)
                            sub_tasks.append(fu)
                    for future in as_completed(sub_tasks):
                        print('import_all子任务完成：', future.result())
                else:
                    label = "%s_%s" % (_table, 0)
                    self.import_all_data(_db, _table, label, partition_keys, tb)

    def import_all_data(self, _db, _table, label, partition_keys, tb):
        fields = tb.sd.cols
        inputFormat = tb.sd.inputFormat
        delim = tb.sd.serdeInfo.parameters.get('field.delim')
        if delim is None:
            delim = "\\x01"
        tb_location = self.hdfs_path_fix(tb.sd.location)
        if len(partition_keys) > 0:
            for i in partition_keys:
                tb_location += "/*"
        self.importStarRocksData(_db, _table, fields, partition_keys, inputFormat, tb_location, label, delim, is_all_table=True)
        print("new table import all data success")

    # 导入数据分区数据 完成insert 插入方式
    def importStarRocks(self, _db, _table, part_vals, has_ds, import_type):
        #insert 导入方式
        if import_type == 'insert':
            self.lock.acquire()
            try:
                if part_vals[0] in has_ds:
                    print(f'---ignore---：{_db}.{_table}'+str(part_vals))
                    return ""
                else:
                    has_ds.append(part_vals[0])
            finally:
                self.lock.release()
            source_table = _db+"_"+_table
            par_value = part_vals[0].replace('-', '')
            par_name = "p"+par_value

            label = _table+par_value
            par_str = "PARTITION(%s)" % (par_name, )
            self.importStarRocksDataByInsert(self.hive_mapping_db,source_table,_db, _table, label, 0, par_str, True)
            return f"{_db}.{_table},{part_vals}"

        #broker 导入执行以下代码
        tb = self.get_table_info(_db, _table)
        partition_keys = tb.partitionKeys
        import_temp_part = False
        search_par_sql = "select count(1) from %s.%s where " % (_db, _table)
        for i, fs in enumerate(partition_keys):
            search_par_sql += fs.name + "='" + part_vals[i] + "' and "
        search_par_sql = search_par_sql[:-4]
        cursor = self.execSql(search_par_sql)
        data = cursor.fetchone()
        if data[0] > 0:
            import_temp_part = True
        self.lock.acquire()
        try:
            with self.meta_client as c:
                try:
                    pt = c.get_partition(_db, _table, part_vals)
                except Exception as e:
                    print(f"{_db}.{_table} ,{part_vals} not exist")
                    raise e
        finally:
            self.lock.release()

        if import_temp_part is False:
            par_name = self.create_partition(_db, _table, part_vals)
            pt_location = self.hdfs_path_fix(pt.sd.location)
            self.actual_import(_db, _table, tb.sd.cols, pt, part_vals, partition_keys, pt_location)
        else:
            self.lock.acquire()
            try:
                if part_vals[0] in has_ds:
                    print(f'---ignore---：{_db}.{_table}'+str(part_vals))
                    return ""
                else:
                    has_ds.append(part_vals[0])
            finally:
                self.lock.release()

            #判断starrocks是否是分区表
            if self.is_part_table(partition_keys) is False:
                self.import_starrocks_by_swap_table(_db, _table, import_type)
            else:
                #创建分区
                par_name = self.create_partition(_db, _table, part_vals, is_temp=True)
                #获取数据路径
                pt_location = self.hdfs_path_fix(pt.sd.location)
                #分区字段多个时，只用ds建分区，ds下的数据统统导入
                if len(part_vals) > 1:
                    rpl = pt_location[pt_location.rfind(part_vals[0])+len(part_vals[0]):]
                    pt_location = pt_location[:pt_location.rfind(part_vals[0])+len(part_vals[0])]
                    rpl_len = len(rpl)
                    for i, s in enumerate(rpl):
                        if s == '/' and i+1 < rpl_len:
                            pt_location += "/*"
                self.actual_import(_db, _table, tb.sd.cols, pt, part_vals, partition_keys, pt_location, is_temp=True, par_name=par_name)
                #替换临时分区
                replace_sql = "ALTER TABLE %s.%s REPLACE PARTITION (%s) WITH TEMPORARY PARTITION (%s)" % (_db, _table, par_name[:-4], par_name)
                self.execSql(replace_sql)
        return f"{_db}.{_table},{part_vals}"



    def actual_import(self,_db, _table, fields ,pt=[], part_vals=(), partition_keys=[], pt_location='', is_temp=False, par_name=""):
        suffer = ''
        for sin_par in part_vals:
            suffer += sin_par

        suffer = suffer.replace('-', '').replace('.', '')
        #fields = pt.sd.cols
        #print(fields)
        label = "%s_%s_%s" % (_table, suffer, 0)
        inputFormat = pt.sd.inputFormat
        delim = pt.sd.serdeInfo.parameters.get('field.delim')
        if delim is None:
            delim = "\\x01"
        self.importStarRocksData(_db, _table, fields, partition_keys, inputFormat, pt_location, label, delim, is_temp=is_temp, par_name=par_name)
        pass



    def importStarRocksDataByInsert(self, source_db, source_table, target_db, target_table, label, label_index, par_str="", is_overwrite=False):
        action = "INTO"
        if is_overwrite is True:
            action = "OVERWRITE"
        imort_sql = "INSERT %s %s.%s %s WITH LABEL %s SELECT * FROM %s.%s" % (action, target_db, target_table, par_str, label+str(label_index), source_db, source_table)

        try:
            self.execSql(imort_sql)
        except Exception as e:
            emessage = str(e)
            if emessage.find("has already been used") != -1:
                self.importStarRocksDataByInsert(source_db, source_table, target_db, target_table, label, label_index+1, par_str, is_overwrite)
            else:
                raise e
        selectStat = "show load where label = '%s' order by JobId desc limit 1" % (label+str(label_index),)
        isSearch = True
        while isSearch == True:
            cursor = self.execSql(selectStat, target_db)
            data = cursor.fetchone()
            if data[2] == 'FINISHED':
                isSearch = False
            elif data[2] == 'CANCELLED':
                errorMsg = data[8]
                raise Exception(f"insert error, label: {label+str(label_index)},{errorMsg}")

    def importStarRocksData(self, _db, _table, fields, partition_keys, inputFormat, location, label, delim='', truc_name='',
                            truc_count=0, is_temp=False, par_name="", is_all_table=False):
        hdfs_ = "LOAD LABEL %s.%s (\nDATA INFILE(\"%s/*\") INTO TABLE %s\n"
        if is_temp is True:
            hdfs_ += "TEMPORARY PARTITION ("+par_name+")\n"
        if inputFormat == 'org.apache.hadoop.mapred.TextInputFormat':
            hdfs_ += "COLUMNS TERMINATED BY " + repr(delim) + "\n"
        else:
            hdfs_ += "FORMAT AS \"" + self.format_mapping.get(inputFormat) + "\"\n"
        hdfs_ += "( "
        if inputFormat == 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' and truc_count != 2:
            col_index=0
            for field in fields:
                if truc_name != '' and truc_name == field.name:
                    break
                hdfs_ = hdfs_ + "`_col" + str(col_index) + "`,"
                col_index = col_index+1
        else:
            for field in fields:
                if truc_name != '' and truc_name == field.name:
                    break
                hdfs_ = hdfs_ + "`" + field.name + "`,"

        hdfs_ = hdfs_[:-1]
        hdfs_ = hdfs_ + ")\n"
        if len(partition_keys) > 0:
            hdfs_ += "COLUMNS FROM PATH AS ("
            for fs in partition_keys:
                hdfs_ += "`" + fs.name + "`,"
            hdfs_ = hdfs_[:-1] + ")"
        if inputFormat == 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' and truc_count != 2:
            hdfs_ += " SET ("
            col_index=0
            for field in fields:
                if truc_name != '' and truc_name == field.name:
                    break
                hdfs_ = hdfs_ + "`" + field.name + "`=`_col"+str(col_index)+"`,"
                col_index = col_index+1
            hdfs_ = hdfs_[:-1]
            hdfs_ = hdfs_ + ")\n"
        hdfs_ += ")\nWITH BROKER broker\n(\"username\"=\"hdfs\")\n"
        hdfs_ += "PROPERTIES\n(\n\"max_filter_ratio\"=\"0.1\")"

        # importSql = hdfs_ % (_db, label, "hdfs://nn1" + pt_location, _table,)
        #importSql = hdfs_ % (_db, label, "hdfs://10.100.105.13:8020" + location, _table,)
        importSql = hdfs_ % (_db, label, "hdfs://nn1" + location, _table,)
        try:
            self.execSql(importSql)
        except Exception as e:
            emessage = str(e)
            if emessage.find("has already been used") != -1:
                label_index = int(label[label.rindex('_')+1:])+1
                l_label = label[:label.rindex('_')+1]
                self.importStarRocksData(_db, _table, fields, partition_keys, inputFormat, location, l_label+str(label_index), delim, is_temp=is_temp, par_name=par_name, is_all_table=is_all_table)
                return
            else:
                raise e
        selectStat = "show load where label = '%s' order by JobId desc limit 1" % (label,)
        isSearch = True
        while isSearch == True:
            cursor = self.execSql(selectStat, _db)
            data = cursor.fetchone()
            if data[2] == 'FINISHED':
                isSearch = False
            elif data[2] == 'CANCELLED':
                errorMsg = data[8]
                prvi = "msg:Invalid Column Name:"
                if errorMsg.find("all partitions have no load data") != -1:
                    isSearch = False
                elif errorMsg.find(prvi) != -1:
                    if truc_count == 0:
                        column = errorMsg[errorMsg.find(prvi) + len(prvi):]
                        self.importStarRocksData(_db, _table, fields, partition_keys, inputFormat, location, label, delim,
                                            column, truc_count=1, is_temp=is_temp, par_name=par_name, is_all_table=is_all_table)
                    else:
                        raise Exception(f"error:{_db}.{_table} {location},{errorMsg}")
                elif errorMsg.find("msg:OrcChunkReader::init_include_columns") != -1:
                    if truc_count == 0:
                        num_start = errorMsg.index("name = _col") + 11
                        num_end = errorMsg.index(" ", num_start)
                        col_index = int(errorMsg[num_start:num_end])
                        if col_index == 0:
                            self.importStarRocksData(_db, _table, fields, partition_keys, inputFormat, location, label, delim,
                                                     '', truc_count=2, is_temp=is_temp, par_name=par_name, is_all_table=is_all_table)
                        else:
                            self.importStarRocksData(_db, _table, fields[:col_index], partition_keys, inputFormat, location, label, delim,
                                                 '', truc_count=1, is_temp=is_temp, par_name=par_name, is_all_table=is_all_table)
                    else:
                        raise Exception(f"error:{_db}.{_table} {location},{errorMsg}")
                elif is_all_table is False and inputFormat == 'org.apache.hadoop.mapred.TextInputFormat' and errorMsg.find("quality not good enough to cancel") != -1:
                    if len(fields) == 1:
                        raise Exception(f"error:{_db}.{_table} {location},{errorMsg}")
                    self.importStarRocksData(_db, _table, fields[:-1], partition_keys, inputFormat, location, label, delim,
                                              is_temp=is_temp, par_name=par_name, is_all_table=is_all_table)
                elif is_all_table is True and inputFormat == 'org.apache.hadoop.mapred.TextInputFormat' and errorMsg.find("quality not good enough to cancel") != -1:
                    raise Exception("import full table error, will import step by step")
                elif errorMsg.find("has already been used") != -1:
                    isSearch = False
                    label_index = int(label[label.rindex('_')+1:])+1
                    l_label = label[:label.rindex('_')+1]
                    self.importStarRocksData(_db, _table, fields, partition_keys, inputFormat, location, l_label+str(label_index), delim, is_temp=is_temp, par_name=par_name, is_all_table=is_all_table)
                elif errorMsg.find("msg:No source file in this table") != -1:
                    isSearch = False
                else:
                    raise Exception(f"error:{_db}.{_table} {location},{errorMsg}")
            time.sleep(1)
        print('import success')
        pass

    def next_p(self,partition_keys, part_vals):
        count = 0
        par_day = ""
        next_day = ""
        for i, fs in enumerate(partition_keys):
            if i == 0 and (fs.name in self.starrocks_part_names):
                par_day = part_vals[i]
                next_day = (datetime.strptime(par_day, '%Y-%m-%d') + timedelta(1)).strftime("%Y-%m-%d")
                count = count + 1
            # elif fs.name == 'hour':
            #     par_hour = part_vals[i]
            #     if par_hour == '23':
            #         next_hour = '0'
            #     else:
            #         next_day = par_day
            #         next_hour = str(int(par_hour) + 1)
            #     count = count + 1
        if count == 1:
            return [next_day]
        # elif count == 2:
        #     return [next_day, next_hour]


    # def im
    def get_table_info(self, _db, _table):
        self.lock.acquire()
        try:
            with self.meta_client as c:
                try:
                    tb = c.get_table(_db, _table)
                except Exception as e:
                    print(f"{_db}.{_table} not exist")
                    raise e

            return tb
        finally:
            self.lock.release()

    def hdfs_path_fix(self, location: str):
        parsed_result = urlparse(location)
        # 下面这几种是合法的
        # /abc
        # hdfs:///abc
        # hdfs://nn1/abc
        # hdfs://10.100.105.12:8020/abc
        # hdfs://10.100.105.13:8020/abc
        if (parsed_result.scheme == '' and parsed_result.netloc == '') or (
                parsed_result.scheme == 'hdfs' and parsed_result.netloc in ['', 'nn1', 'ns1:8020', '192.168.1.12:8020',
                                                                            '192.168.1.13:8020']):
            return parsed_result.path
        else:
            raise Exception(f"error:{location} is error")

    #判断starrocks是否是分区表,分区表返回True
    def is_part_table(self, partition_keys):
        for fs in partition_keys:
            if fs.name in self.starrocks_part_names:
                return True
        return False


# Defining the plugin class
class ImportStarRocks2Plugin(AirflowPlugin):
    name = "import-starRocks2"
    operators = [ImportStarRocks2Operator]