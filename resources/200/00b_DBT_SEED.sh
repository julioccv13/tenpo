#/bin/bash

set -e 

bash $(pwd)/dbt_tenpo_bi/dbt.sh seed
exit $?