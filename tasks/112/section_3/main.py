from core.google import bigquery
from core.ports import Process
import logging
import pandas
import datetime
date_s = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')

class Process112_Generate_Partitions(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_target = kwargs["project_target"]

    def run(self):
        self._generate_partitions()
        self._generate_range_partitions()
    
    def _generate_partitions(self):
        """This function it's in charge of iterate over each block of 500K and send it every 1000 rows """        
        clustered_table_script = self._generate_script_partitions()
        bigquery.execute_query(clustered_table_script)

        generatet_tables = self._generate_tables()
        bigquery.execute_query(generatet_tables)
    
    def _generate_script_partitions(self):
        """This function will help us to assing a block number to each row (each 500k rows)"""
        sql_script = f"""
        
        declare n_rows, inicio, fin, n_bloque int64;
        declare iterations, n_iterations float64;
        set n_rows = (select count(*) from `{self._project_target}.tmp.table_final_{self._ds_nodash}`);
        set fin = 500000;
        set inicio = 0;
        set n_bloque = 1;
        set iterations = n_rows/fin;
        set n_iterations = iterations + 1;

        while n_iterations > 1 DO

            UPDATE `{self._project_target}.tmp.table_final_{self._ds_nodash}` 
            SET bloque = n_bloque
            WHERE row >= inicio
            AND row < fin + 1;

            SET inicio = fin + 1;
            SET fin = fin + 500000;
            SET n_bloque  = n_bloque + 1;
            SET n_iterations = n_iterations - 1;
            
        END WHILE;
        """
        return sql_script

    def _generate_tables(self):
        """This function help us to generate as many tables as partitions you have in the initial table (defined by a block number)"""
        sql_script = f"""

        declare n_bloque, max_bloque int64;
        declare table_name string;
        declare n_bloque_string string;
        set n_bloque = 1;
        set max_bloque = (select max (bloque) from `{self._project_target}.tmp.table_final_{self._ds_nodash}`);

            while n_bloque <= max_bloque DO

                set table_name = concat(concat('`{self._project_target}.tmp.table_final_{self._ds_nodash}_bloque_',n_bloque),'`');
                set n_bloque_string = cast(n_bloque as string);
                
                EXECUTE IMMEDIATE format(\"""

                    drop table if exists %s;

                \""", table_name);
                EXECUTE IMMEDIATE format(\"""

                    create table %s as (
                        select * from `{self._project_target}.tmp.table_final_{self._ds_nodash}`
                        where bloque = %s
                    )

                \""", table_name,n_bloque_string);

                set n_bloque = n_bloque + 1;

            END WHILE;
        """
        return sql_script

    def _generate_range_partitions(self):
        """This function generate partitionated tables by range for each table of 500K"""
        dataframe = pandas.read_gbq(f"select max(bloque) from {self._project_target}.tmp.table_final_{date_s}")
        n_bloques = int(dataframe.at[0,'f0_'])
        inicio_range_interval = 0
        range_interval = 500000 
        for bloque in range(1,n_bloques+1):
            sql_script = f"""

            DROP TABLE IF EXISTS `{self._project_target}.tmp.table_final_{date_s}_bloque_{bloque}_partitioned`;
            CREATE TABLE `{self._project_target}.tmp.table_final_{date_s}_bloque_{bloque}_partitioned` (identity STRING, profileData STRING, row INT64, bloque INT64)
                PARTITION BY
                RANGE_BUCKET( row, GENERATE_ARRAY({inicio_range_interval}, {range_interval}, 250001))
                OPTIONS (require_partition_filter = TRUE);

            INSERT INTO `{self._project_target}.tmp.table_final_{date_s}_bloque_{bloque}_partitioned`
            SELECT * FROM `{self._project_target}.tmp.table_final_{date_s}_bloque_{bloque}`;

            """
            bigquery.execute_query(sql_script)
            inicio_range_interval = range_interval
            range_interval += 500000