# Seteo de condiciones iniciales

Para hacer un testing del cambio que se haga sobre uno de los modelos de DBT, es necesario ejecutar 
primero el script 

~~~
bash copy_tables.sh
~~~

Este script copiará las tablas que no son accesibles por la SA de ambientes intermedios. De esas manera
quedará completamente independiente de proyectos productivos.

El script toma algunos minutos.

# Uso
En primer lugar se debe activar el ambiente virtual en la ruta del repositorio asgard-data-company

~~~
pipenv shell
~~~

Luego deben setearse las variables de ambiente necesarias para la ejecución de DBT

~~~
export GCP_CREDENTIALS=< Path to JSON Credentials>
export SENSIBLE_SHA256_PEPPER_SECRET=<Encoded Secret>
export DBT_TARGET=<Environment>
~~~

Finalmente se ejecuta

~~~
dbt run
~~~
