#/bin/bash

set -e 

export DBT_DIR_COPY_DESTINATION=$(mktemp -d) # esta variable cambia el comportamiento de dbt.sh
rm -rf $DBT_DIR_COPY_DESTINATION
mkdir -p $DBT_DIR_COPY_DESTINATION

export GAPP_TMP_DIR="$(mktemp -d)"
export PUBLIC_ROOT="$GAPP_TMP_DIR/public"
mkdir -p $PUBLIC_ROOT

# TODO: Buscar forma de obtener este directorio automaticamente (para mover el DAG a cualquier lado)
bash $(pwd)/dbt_tenpo_bi/dbt.sh docs generate

# making directory and copying app.yml
cp -rf $(pwd)/app.yaml $GAPP_TMP_DIR/app.yaml

# Copiar ficheros
cp -rf $DBT_DIR_COPY_DESTINATION/target/*.json $PUBLIC_ROOT/
cp -rf $DBT_DIR_COPY_DESTINATION/target/*.html $PUBLIC_ROOT/

cd $GAPP_TMP_DIR

## Deployment
# TODO: Deberia ser un script
export CLOUDSDK_CONFIG=$(mktemp -d)
export GOOGLE_APPLICATION_CREDENTIALS=$(mktemp)
echo "$GCP_CREDENTIALS" > "$GOOGLE_APPLICATION_CREDENTIALS"
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
gcloud config set project tenpo-datalake-prod
gcloud app deploy --project tenpo-datalake-prod

exit $?