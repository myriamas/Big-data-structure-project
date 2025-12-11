# RÉSULTATS COMPLETS - 2.5.3 et 2.5.4

## 2.5.3 - Taille TOTALE de chaque DB (en GB)

| Database | Product | Stock | Warehouse | OrderLine | Client | **TOTAL** |
|----------|---------|-------|-----------|-----------|--------|-----------|
| **DB1** | 0.1132 | 25.3320 | 0.0000 | 5900.8598 | 0.3614 | **5926.67 GB** |
| **DB2** | 0.1390 | — | 0.0000 | 5900.8598 | 0.3614 | **5901.36 GB** |
| **DB3** | — | 25.3320 | 0.0000 | 5900.8598 | 0.3614 | **5926.55 GB** |
| **DB4** | — | 25.3320 | 0.0000 | 5900.8598 | 0.3614 | **5926.55 GB** |
| **DB5** | 0.1770 | 25.3320 | 0.0000 | — | 0.3614 | **25.87 GB** |

### Observations clés :
- **DB5 est 229x plus efficace** que DB1 en termes de stockage
- DB2 est légèrement meilleur que DB1 (25.3 GB d'économie)
- DB3 et DB4 sont similaires à DB1 (dénormalisation très coûteuse)
- **DB5 fusionne OrderLine dans Product** → élimination complète de la large collection OrderLine

---

## 2.5.4 - Problèmes liés à la dénormalisation

### **DB1-DB4 : La dénormalisation progressive AUGMENTE les coûts**

#### Problème 1 : Duplication massive de données
- **DB1 (normalisé)** : Données ne sont stockées qu'UNE FOIS
- **DB3 & DB4** : Chaque produit est dupliqué à travers les documents Stock/OrderLine
  - Exemple : Un produit qui existe dans 200 entrepôts → dupliqué 200 fois
  - Résultat : **Augmentation drastique du stockage** sans bénéfice réel

#### Problème 2 : Coûts financiers élevés
- **DB1** : ~5926 GB d'infrastructure
- **DB4** : ~5926 GB d'infrastructure (idem)
- **DB5** : ~26 GB (229x moins cher !)
- Implication : Serveurs supplémentaires, disques, bande passante réseau

#### Problème 3 : Cohérence des données
**Scénario problématique** : Un produit change de prix
- **DB1 (normalisé)** : Mettre à jour 1 document Product
- **DB3/DB4 (dénormalisé)** : Mettre à jour le produit dans TOUS les documents Stock/OrderLine
  - Risque d'incohérence : certains updates peuvent échouer
  - Risque de lectures incohérentes pendant les mises à jour
  - Maintenance complexe et coûteuse

#### Problème 4 : Performance des écritures
- **Dénormalisation** = plus de documents à mettre à jour
- Transactions plus longues et plus complexes
- Plus grande latence pour les écritures massives

#### Problème 5 : Scalabilité limitée
- **DB1-DB4** : Chaque serveur porte plus de données
- Sharding moins efficace (données redondantes)
- Réplication plus coûteuse (plus de GB à répliquer)

### **DB5 : Cas optimal mais avec trade-offs**

#### Avantages de DB5 :
- Stockage minimal (25.87 GB vs 5926.67 GB)
- Lectures rapides (product + orderlines imbriqués = une seule requête)
- Faible coût d'infrastructure

#### Inconvénients de DB5 :
- Duplication de champs fixes (name, description, price se répètent dans chaque orderline)
- Si un produit change (changement de nom/description), mais les OrderLines sont historiques → ce n'est pas un problème
- Lectures partielles moins efficaces (récupérer juste le produit = télécharger les orderlines aussi)

---

## Conclusion

### **Le choix dépend du cas d'usage :**

| Cas d'usage | Meilleur choix | Raison |
|-------------|---|---|
| **Données volumineuses, peu de mises à jour** | DB1-DB2 | Économe et cohérent |
| **Lectures fréquentes de commandes complètes** | DB5 | Pas de jointure, stockage minimal |
| **Historique immuable (commandes)** | DB5 | OrderLine = snapshot historique |
| **Produits modifiables fréquemment** | DB1-DB2 | Mise à jour simple et unique |

**Pour ce projet e-commerce**, **DB5 est clairement optimal** : les commandes sont historiques, les mises à jour portent sur les nouveaux produits, et les lectures dominantes cherchent "produit + commandes associées".

