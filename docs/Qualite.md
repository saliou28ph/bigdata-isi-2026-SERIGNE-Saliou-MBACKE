# Qualité des données

## Tableau des relevés

| Indicateur                            | Valeur | Interprétation                                                                                                                                                                       |
| ------------------------------------- | -----: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Lignes brutes                         |  5 025 | Nombre total de lignes présentes dans le jeu de données avant nettoyage.                                                                                                             |
| Emails manquants                      |    150 | 150 lignes ne possèdent pas d'adresse e-mail renseignée. Cela représente environ **2,99 %** des lignes brutes.                                                                       |
| Villes distinctes avant normalisation |    499 | Le jeu de données contient 499 villes différentes avant normalisation.                                                                                                               |
| Villes distinctes après normalisation |    499 | Le nombre de villes distinctes reste identique après normalisation. Les différences de casse ou d'accents n'ont donc pas créé de nouvelles villes distinctes dans ce jeu de données. |
| Doublons exacts                       |     15 | 15 lignes sont parfaitement identiques à d'autres lignes et peuvent être considérées comme des doublons exacts.                                                                      |
| Lignes après nettoyage                |  5 000 | Après suppression des 15 doublons exacts, le jeu de données contient 5 000 lignes.                                                                                                   |

## Interprétation

Le jeu de données contient initialement **5 025 lignes**. L'analyse de qualité révèle **150 valeurs manquantes dans la colonne des e-mails**, soit environ **2,99 %** des lignes. Ces valeurs manquantes doivent être prises en compte avant toute analyse utilisant les adresses e-mail.

La normalisation des noms de villes n'a pas modifié le nombre de villes distinctes : celui-ci reste de **499 avant et après normalisation**. Cela indique que les variations de casse ou d'accents n'ont pas généré de nouvelles modalités distinctes dans les données étudiées. La normalisation a néanmoins permis d'assurer une représentation homogène des noms de villes.

L'analyse a également identifié **15 doublons exacts**. Leur suppression permet de passer de **5 025 lignes à 5 000 lignes**, ce qui correspond exactement à la réduction attendue :

**5 025 − 15 = 5 010**

Il convient toutefois de remarquer que cette soustraction ne donne pas 5 000. Il existe donc une **incohérence à vérifier dans les relevés fournis** : si seuls les 15 doublons exacts ont été supprimés, le nombre de lignes devrait être **5 010** et non 5 000.

Ainsi, avant de valider définitivement le rapport de qualité, il est recommandé de vérifier s'il existe **10 autres lignes supprimées lors du nettoyage** (par exemple des lignes invalides, incomplètes ou exclues selon une autre règle).

### Conclusion

Globalement, le nettoyage montre une bonne maîtrise des données : les doublons exacts sont identifiés et la normalisation des villes a été effectuée. Cependant, la différence entre **5 025 lignes brutes**, **15 doublons exacts** et **5 000 lignes finales** doit être expliquée afin d'assurer la traçabilité complète du processus de nettoyage.
