# c19frcheck

Programme visant à calculer le taux de mortalité de la Covid19 en France et de le comparer aux épidémies (de grippe) d'autres années.

Les données sont récupérées automatiquement par le programme depuis des sources officielles.

# Résultats: 1ier pic covid (mars/avril 2020) VS Grippe janvier 2017

On peut constater sur le graphe suivant que la première vague de Covid19 en France a un taux de mortalité légèrement plus faible que l'épisode de grippe de janvier 2017:

![Taux de mortalité par âge](results/taux_mortalite_par_age.png)

Pourtant le nombre total de décès est supérieur pour la Covid19:

![Décès par date](results/deces_par_date.png)

Cette apparente différence s'explique par le fait que nous connaissons actuellement en France un vieillissement rapide de la population (phénomène connu sous le nom de "Papy Boom"). Le nombre de personnes autour de 75 ans est en rapide augmentation, or cette tranche de la population est parmi celle qui est la plus vulnérable aux épidémies hivernales.

![Population par âge](results/population_par_age.png)

Ainsi, l'excés de mortalité que nous avons connu lors de la première vague de Covid19 s'explique non pas par la gravité du virus, qui au final semble n'avoir rien d'inhabituelle, mais simplement par le vieillisement rapide la population.

On peut en effet constater que les âges dans lesquels le Covid19 a tué plus que la grippe de janvier 2017, sont les mêmes âges oú la population est plus nombreuse en 2020 qu'en 2017.

![Décès par âge](results/deces_par_age.png)

# Résultats: Mortalités annuelles de 2000 à 2020

On peut constater que la mortalité de l'année 2020 (1ière année du Covid19) est certes un peu plus élevée que l'année précédente (2019), mais reste tout à fait dans les normes des mortalités de ces 20 dernières années. Ce qui suggère que le Covid19 ne constitue aucunement un épisode épidémique exceptionnel.

(Ici, la mortalité est moyennée par âge, ce qui permet de ne pas perturber les résultats comparatifs par le viellissement de la population).

![Mortalité moyennée par âge](results/mortalite_moyenne_par_age.png)

# Autres pays

* [Suède](./README_se.md)

# Infos complémentaires

* [Chaine Youtube "Décoder l'éco"](https://www.youtube.com/watch?v=8pA9pfFUSIk)

# Comment executer

```
# need python3
pip install -r requirements.txt
run.py all
```