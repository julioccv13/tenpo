INSERT INTO `${project9}.clevertap_audit.clevertap_users_v2` 
SELECT a.email
FROM `${project4}.temp.P_${ds_nodash}_Clevertap_Users` a
INNER JOIN `${project1}.temp.validated_clevertap_users_v2` b
ON TRIM(lower(a.email)) = TRIM(lower(b.email));