SELECT * FROM
(
SELECT 
    --tr.id,
    tr.phone,
    tr.name client,
    a.name an_name,
    case
        when a.simple is not null and  tr.simpl is not null and  a.simple = 'Y' and  tr.simpl = 'Y' then '?????????????'
        when tr.val is not null and  tr.val < a.min_value then '???????' 
        when tr.val is not null and  tr.val > a.max_value then '???????' 
        else '?????'
    end  as res
FROM
    (SELECT 
        t.id,
        t.name,
        t.phone,
        r.pat_code,
        r.an_code,
        r.val,
        r.simpl
    FROM DE.MED_NAMES t
    LEFT JOIN DEMIPT2.PANA_XLS r
        ON t.id = r.pat_code
    ORDER BY t.id
    ) tr
    LEFT JOIN DE.MED_AN_NAME a
        ON tr.an_code = a.code
WHERE tr.id in (
--
    SELECT 
        resul.id
    FROM
    (
    SELECT 
        tr.id,
        tr.phone,
        tr.name client,
        tr.an_code,
        a.name an_name,
        case
            when a.simple is not null and  tr.simpl is not null and  a.simple = 'Y' and  tr.simpl = 'Y' then '?????????????'
            when tr.val is not null and  tr.val < a.min_value then '???????' 
            when tr.val is not null and  tr.val > a.max_value then '???????' 
            else '?????'
        end  as res,
        
        case
            when a.simple is not null and  tr.simpl is not null and  a.simple = 'Y' and  tr.simpl = 'Y' then 1
            when tr.val is not null and  tr.val < a.min_value then 1 
            when tr.val is not null and  tr.val > a.max_value then 1 
            else 0
        end  as kol
        
    FROM
        (SELECT 
            t.id,
            t.name,
            t.phone,
            r.pat_code,
            r.an_code,
            r.val,
            r.simpl
        FROM DE.MED_NAMES t
        LEFT JOIN DEMIPT2.PANA_XLS r
            ON t.id = r.pat_code
        ORDER BY t.id
        ) tr
        LEFT JOIN DE.MED_AN_NAME a
            ON tr.an_code = a.code
        ) resul
    GROUP BY resul.id
    HAVING sum(resul.kol) >= 2
--
    )
)
WHERE res <> '?????'