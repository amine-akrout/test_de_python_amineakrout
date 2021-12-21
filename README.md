# Python et Data Engineering Test

### Environnement du projet:
- python version 3.8.9
- librairies python utilisées : pandas, PyYAML et argparse

### Data Pipeline
Le fichier `data_pipeline.py` permet d'exécuter le pipeline de données.  
Ce script prend comme paramètres un *input* (répértoire des données en input) et un *output* (répertoire où on souhaite sauvegarder notre output JSON)

**Exemple :** 
<pre>
python data_pipeline.py --input ./data/  --output ./output/

# Output:
# [INFO] joining pubmed and drugs data...
# INFO] joining clinical trial and drugs data...
# [INFO] final join...
</pre>

### Traitement ad-hoc
Le fichier `top_journal.py` permet de mettre en place l'analyse ad-hoc.  
Ce script permet, à partir de l'output du pipeline, d'extraire le nom du journal qui mentionne le plus de
médicaments différents

<pre>
python top_journal.py

# Output:
# les journaux qui mentionnent le plus de médicaments différents sont: 'Psychopharmacology' ,'The journal of maternal-fetal & neonatal medicine' 
</pre>

### Faire évoluer le code
Pour que le code puisse gérer de grosses volumétries de données, on doit migrer vers un environnement scalable, Spark par exemple.  


Spark utilise le *lazy evaluation*, c'est à dire, il ne s'exécutera que lorsque  vous aurez réellement besoin de retourner un output (une aggégation par exemple), et entre-temps, 
il construit un plan d'exécution et trouve la manière optimale d'exécuter le code.
Ceci diffère de Pandas, qui exécute chaque étape au fur et à mesure.
Spark est également moins susceptible de manquer de mémoire, car il commence à utiliser le disque lorsqu'il atteint sa limite de mémoire.


## SQL
**1. Première requête:**  
<pre>
SELECT date,
       Sum(prod_price * prod_qty) AS ventes
FROM   TRANSACTION
WHERE  date BETWEEN '01/01/19' AND '31/12/19'
GROUP  BY date; 
</pre>
`

**2. Dexième requête:**
<pre>
SELECT client_id ,
       DECO   AS ventes_deco,
       MEUBLE AS ventes_meubles,
FROM   (
                 SELECT    t.client_id,
                           t.prod_price, 
                           t.prod_qty
                           p.product_type
                 FROM      TRANSACTION t
                 LEFT JOIN PRODUCT_NOMENCLATURE p
                 ON        t.prop_id = p.product_id
                 WHERE     t.date BETWEEN '01/01/19' AND '31/12/19') 
PIVOT (sum(prod_price * prod_qty)) FOR product_type IN (DECO, MEUBLE) ) AS pivottable                                                                                                                                   meuble) ) AS pivottable
</pre>
