#/bin/bash
set -e 

bash $(pwd)/dbt_tenpo_bi/dbt.sh deps
bash $(pwd)/dbt_tenpo_bi/dbt.sh run $RUN_OTHER_ARGUMENTS

exit $?