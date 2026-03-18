\H


-- Paramétrage de la sortie pour le compte-rendu
\pset format html
\pset tableattr 'border="1" style="border-collapse:collapse; width:100%; margin-top:10px; margin-bottom:30px;"'

\echo '<h1>Compte-rendu de TDM - Analyse Socio-Économique</h1>'

-- ---------------------------------------------------------
-- QUESTION 3.1
-- ---------------------------------------------------------
\echo '<h3>Question 3.1 : Départements dont le transport en vélo en 2024 est entre 1% et 4%</h3>'
\echo '<pre>SELECT dep, valeur, indicateur FROM dep_eco WHERE indicateur LIKE ''%en vélo (%)%'' AND valeur::float BETWEEN 1.0 AND 4.0 ORDER BY valeur DESC;</pre>'

SELECT dep, valeur, indicateur
FROM dep_eco 
WHERE indicateur LIKE '%en vélo (%)%' 
AND valeur::float BETWEEN 1.0 AND 4.0 
ORDER BY valeur DESC;


-- ---------------------------------------------------------
-- QUESTION 3.2
-- ---------------------------------------------------------
\echo '<h3>Question 3.2 : Départements dont la région a > 78% de diplômés en 2017 (avec Taux d''emploi 2017/2022)</h3>'
\echo '<pre>-- Requête principale pour les régions diplômées\nSELECT d.libelle AS dep, re.valeur AS val_reg FROM region_eco re JOIN region r ON r.libelle = re.reg JOIN departement d ON r.reg = d.reg WHERE re.indicateur LIKE ''%Part des diplômés%'' AND re.annee LIKE ''%2017%'' AND re.valeur::float > 78;</pre>'

SELECT 
    d.libelle AS departement, 
    re.valeur AS diplomes_region_2017,
    e22.valeur AS taux_emploi_2022,
    e14.valeur AS taux_emploi_2014
FROM region_eco re 
JOIN region r ON r.libelle = re.reg
JOIN departement d ON r.reg = d.reg
-- Jointure pour le taux emploi 2022
LEFT JOIN dep_eco e22 ON d.libelle = e22.dep AND e22.indicateur LIKE '%Taux d''emploi%' AND e22.annee LIKE '%2022%'
-- Jointure pour le taux emploi 2014 (utilisé dans votre script)
LEFT JOIN dep_eco e14 ON d.libelle = e14.dep AND e14.indicateur LIKE '%Taux d''emploi%' AND e14.annee LIKE '%2014%'
WHERE re.indicateur LIKE '%Part des diplômés%' 
  AND re.annee LIKE '%2017%' 
  AND re.valeur <> 'N/A'
  AND re.valeur::float > 78
ORDER BY re.valeur::float DESC;


-- ---------------------------------------------------------
-- QUESTION 3.3
-- ---------------------------------------------------------
\echo '<h3>Question 3.3 : Écart d''espérance de vie H/F (2019) dans la région la plus pauvre (2018)</h3>'
\echo '<pre>-- Recherche de la région la plus pauvre puis calcul des écarts par département</pre>'

WITH region_pauvre AS (
    SELECT reg FROM region_social 
    WHERE indicateur LIKE '%Taux de pauvreté%' AND annee LIKE '%2018%' AND valeur NOT IN ('nd', 'N/A')
    ORDER BY valeur::float DESC LIMIT 1
)
SELECT 
    d.libelle AS nom_dep,
    f.valeur::float AS esp_femmes,
    h.valeur::float AS esp_hommes,
    (f.valeur::float - h.valeur::float) AS ecart_ans
FROM departement d
JOIN region r ON d.reg = r.reg
JOIN dep_social f ON d.dep = f.num AND f.indicateur LIKE '%femmes%' AND f.annee = '2019'
JOIN dep_social h ON d.dep = h.num AND h.indicateur LIKE '%hommes%' AND h.annee = '2019'
WHERE r.libelle = (SELECT reg FROM region_pauvre)
ORDER BY ecart_ans DESC;


-- ---------------------------------------------------------
-- QUESTION 3.4
-- ---------------------------------------------------------
\echo '<h3>Question 3.4 : Estimation population 2023 des départements où l''espérance de vie H >= 80 ans en 2024</h3>'
\echo '<pre>SELECT d.dep, p.valeur AS pop_2023 FROM dep_social d JOIN pop_dep p ON d.dep = p.dep WHERE d.indicateur LIKE ''%Espérance de vie des hommes%'' AND d.annee LIKE ''%2024%'' AND d.valeur::float > 80 AND p.indicateur LIKE ''%2023%'';</pre>'

SELECT 
    d.dep AS code_departement, 
    d.valeur AS esp_vie_H_2024,
    p.valeur AS estimation_pop_2023
FROM dep_social d
JOIN pop_dep p ON d.dep = p.dep
WHERE d.indicateur LIKE '%Espérance de vie des hommes à la naissance%'
  AND d.valeur NOT IN ('nd', 'N/A')
  AND d.annee LIKE '%2024%'
  AND d.valeur::float > 80
  AND p.indicateur LIKE '%Estimations de population au 1er janvier 2023%';


-- ---------------------------------------------------------
-- QUESTION 3.5
-- ---------------------------------------------------------
\echo '<h3>Question 3.5 : Espérance de vie 2024 dans les régions à faible taux d''activité (2019) et forte économie sociale (2022)</h3>'
\echo '<pre>-- Sous-requêtes pour filtrer les régions cibles puis récupération de l''espérance de vie</pre>'

SELECT 
    rs.reg, 
    rs.indicateur, 
    rs.valeur AS esperance_vie_2024
FROM region_social rs
WHERE rs.annee LIKE '%2024%'
  AND rs.indicateur ILIKE '%Espérance de vie%'
  AND rs.reg IN (
    SELECT DISTINCT re.reg FROM region_eco re
    WHERE re.reg IN (
        SELECT reg FROM region_eco WHERE indicateur ILIKE '%Activité%' AND annee::text LIKE '%2019%' AND valeur::float <= 73.5
    )
    AND re.reg IN (
        SELECT reg FROM region_eco WHERE (indicateur ILIKE '%économie sociale%' OR indicateur ILIKE '%ESS%') AND annee::text LIKE '%2022%' AND valeur::float > 10
    )
  );


-- ---------------------------------------------------------
-- QUESTION 3.6
-- ---------------------------------------------------------
\echo '<h3>Question 3.6 : Régions dont TOUS les départements sont en croissance ou déclin (2020-2023)</h3>'

\echo '<h4>A. Régions en CROISSANCE totale (tous départements > 0)</h4>'
\echo '<pre>SELECT r.libelle FROM region r WHERE NOT EXISTS (...) AND EXISTS (...);</pre>'
SELECT r.libelle AS region_croissance
FROM region r
WHERE NOT EXISTS (
    SELECT 1 FROM departement d JOIN pop_dep p ON d.libelle = p.dep
    WHERE d.reg = r.reg AND p.indicateur LIKE '%Variation relative annuelle 2020-2023%'
      AND p.annee LIKE '%Totale%' AND p.valeur::float <= 0
)
AND EXISTS (SELECT 1 FROM departement d2 WHERE d2.reg = r.reg);

\echo '<h4>B. Régions en DÉCLIN total (tous départements < 0)</h4>'
SELECT r.libelle AS region_declin
FROM region r
WHERE NOT EXISTS (
    SELECT 1 FROM departement d JOIN pop_dep p ON d.libelle = p.dep
    WHERE d.reg = r.reg AND p.indicateur LIKE '%Variation relative annuelle 2020-2023%'
      AND p.annee LIKE '%Totale%' AND p.valeur::float >= 0
)
AND EXISTS (SELECT 1 FROM departement d2 WHERE d2.reg = r.reg);