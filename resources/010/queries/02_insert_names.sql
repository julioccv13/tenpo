DROP TABLE IF EXISTS `${project_target}.temp.P_{{ds_nodash}}_Out_Names_Aux`;
CREATE TABLE `${project_target}.temp.P_{{ds_nodash}}_Out_Names_Aux` AS (
    with non_existing_ids as (
        SELECT 
            a.*
        FROM `${project_target}.temp.P_{{ds_nodash}}_Names_Aux` a
        LEFT JOIN `${project_target}.aux_table.name_genders` b
        ON a.id = b.id and a.origin = b.origin
        WHERE b.id IS NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY fecha DESC) = 1
    )

    SELECT 
        a.id, a.first_name, a.fecha, a.origin,
        b.name, b.gender, b.probability, b.count
    FROM non_existing_ids a
    INNER JOIN `${project_target}.temp.P_{{ds_nodash}}_Out_Names` b
    ON a.first_name = b.name
);

CREATE TABLE IF NOT EXISTS `${project_target}.aux_table.name_genders` 
AS (
    SELECT * 
    FROM `${project_target}.temp.P_{{ds_nodash}}_Out_Names_Aux`
    LIMIT 0
);

INSERT INTO `${project_target}.aux_table.name_genders` 
SELECT * FROM `${project_target}.temp.P_{{ds_nodash}}_Out_Names_Aux`;