# c19frcheck - Suède

Programme visant à calculer le taux de mortalité en Suède en 2020, année de la pandémie Covid19, et de le comparer aux années précédentes.

Le cas de la Suède est intéressant car c'est un pays qui n'a pas pratiqué le confinement. Cela permet ainsi de constater quel est l'impact espéré du confinement sur la mortalité globale.

Le code source, ainsi que les sources des données sont accessibles dans ce répertoire.

# Résultats: Mortalités annuelles de 1980 à 2020

On peut constater que la mortalité de l'année 2020 (1ière année du Covid19) est certes plus élevée que les 5-10 années précédentes, mais reste tout à fait dans les normes des mortalités de ces 40 dernières années. Ce qui fait de la Covid19 un épisode épidémique certes plus grave que les proches années précédentes, mais ne provoque toutefois pas une mortalité exceptionnelle, et ce même en l'absence de confinement.

Cela permet aussi de relativiser l'efficacité du confinement au regard de la mortalité des pays qui l'ont pratiqué, et de se poser la question compte tenu de sont coût social, sanitaire et économique très conséquent de la juste pertinence de l'usage de tel moyens pour lutter contre la pandémie Covid19.

(Ici, la mortalité est moyennée par âge, ce qui permet de ne pas perturber les résultats comparatifs par le viellissement de la population).

![[Suède] Mortalité moyennée par âge](results/se_mortality_meaned_by_age.png)

# Comment executer

```
# need python3
pip install -r requirements.txt
se_run.py all
```