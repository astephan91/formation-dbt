Vous venez d'arriver dans une nouvelle entreprise, et devez migrer des procédures stockées vers une architecture plus moderne et scalable.

Voici une des procédures, comment procéderiez vous pour lancer la migration ?

```sql
CREATE OR REPLACE PROCEDURE dwh.alim_dim_owner()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

        TRUNCATE TABLE dwh.ZZ_DIM_Owner_In;

        -- Le With permet de trouver l'équipe d'un utilisateur
        -- Si plusieurs équipes alors on met Multi pour éviter les doublons
        with Teams as (
                       SELECT 
						id,
						teams,
						case
							when jsonb_array_length(teams) = 1 then teams -> 0 ->> 'name' else 'Multi'
						    end as Teams_Name,
						case
							when jsonb_array_length(teams) = 1 then teams -> 0 ->> 'id' else '999'
						    end as Teams_Code
from hubspot.owners
where jsonb_typeof(teams) = 'array'
)
        INSERT INTO dwh.ZZ_DIM_Owner_In
        SELECT  'hubspot'  SOURCE,
                owners."updatedAt" Dt_Insert_ODS,
                current_date Dt_Insert_DWH,
                0 Flag_Delete,
                owners.id AS owner_code,
                concat(owners."firstName", ' ', owners."lastName") AS owner_name,
                COALESCE(Teams.Teams_Name,'')  owner_teams,
                COALESCE(Teams.Teams_Code,'')  owner_teams_code
        FROM hubspot.owners
        left outer join Teams 
                on Teams.id = owners.id;
           
        --UPDATE
        UPDATE dwh.DIM_Owner 
        SET SOURCE = ZZ_DIM_Owner_In.SOURCE ,
                Dt_Insert_ODS = ZZ_DIM_Owner_In.Dt_Insert_ODS,
                Dt_Insert_DWH = ZZ_DIM_Owner_In.Dt_Insert_DWH,
                Flag_Delete = ZZ_DIM_Owner_In.Flag_Delete,
                owner_code = ZZ_DIM_Owner_In.owner_code,
                owner_Name = ZZ_DIM_Owner_In.owner_Name,
                owner_teams = ZZ_DIM_Owner_In.owner_teams,
                owner_teams_code = ZZ_DIM_Owner_In.owner_teams_code
        FROM dwh.ZZ_DIM_Owner_In
        WHERE DIM_Owner.Owner_Code = ZZ_DIM_Owner_In.Owner_Code;

        --INSERT
        INSERT INTO dwh.DIM_Owner (SOURCE,Dt_Insert_ODS,Dt_Insert_DWH,Flag_Delete,Owner_Code,Owner_Name,owner_teams,owner_teams_code)
        SELECT SOURCE,Dt_Insert_ODS,Dt_Insert_DWH,Flag_Delete,Owner_Code,Owner_Name,owner_teams,owner_teams_code
        FROM dwh.ZZ_DIM_Owner_In
        WHERE NOT EXISTS (
                SELECT 1
                FROM dwh.DIM_Owner
                WHERE DIM_Owner.Owner_Code = ZZ_DIM_Owner_In.Owner_Code );

        --DELETE
        if (select count(*) from dwh.ZZ_Dim_Owner_In) >= 1 then
                UPDATE dwh.DIM_Owner
                SET Flag_Delete = 1
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM dwh.ZZ_Dim_Owner_In
                        WHERE DIM_Owner.Owner_Code = ZZ_Dim_Owner_In.Owner_Code )
                 AND Owner_ID <> -1;
        end if;

        --LOG
        INSERT INTO dwh.TECH_LOG_Alim SELECT 'dim_Owner' TABLE_NAME, NOW() Dt_Update, (SELECT COUNT(*) from dwh.ZZ_Dim_Owner_In) Nb_Lines;
        
        TRUNCATE TABLE dwh.ZZ_Dim_Owner_In;        
 END;
$procedure$
;
```