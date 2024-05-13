DROP TABLE IF EXISTS `${project4}.temp.P_${ds_nodash}_Clevertap_Users`;

CREATE TABLE `${project4}.temp.P_${ds_nodash}_Clevertap_Users` 
AS 
(
    SELECT 
        distinct
        TRIM(lower(a.email)) as email,
        FORMAT_DATE('%Y%m%d', date(ob_completed_at, 'America/Santiago')) as dt
    FROM `${project3}.users.users` a
    LEFT JOIN `${project9}.clevertap_audit.clevertap_users_v2` b
        ON TRIM(lower(a.email)) = TRIM(lower(b.email))
    WHERE state in (4,7,8)
        and b.email is null
    --and ob_completed_at is not null
        and date(ob_completed_at, 'America/Santiago') = '${ds}'
);