# SHARDING STRATEGIES - 2.6.1

## Configuration du cluster
- **Nombre de serveurs** : 1,000
- **Produits** : 100,000
- **Entrepôts** : 200
- **Stocks** : 20,000,000 (produits × entrepôts)
- **OrderLines** : 4,000,000,000 (4 milliards)
- **Clients** : 1,000,000
- **Marques distinctes** : 5,000

---

## Résultats du Sharding (2.6.1)

### Stock Collection

#### **1. St - #IDP** [EXCELLENT]
| Métrique | Valeur |
|----------|--------|
| **Avg docs per server** | 20,000 |
| **Avg distinct key values** | 100 |
| **Évaluation** | [5/5] Excellent |

**Explication** : 
- 100,000 produits ÷ 1,000 serveurs = 100 produits par serveur
- Distribution très équilibrée
- Chaque serveur gère ~20,000 documents (stocks)
- **Meilleur choix pour Stock**

---

#### **2. St - #IDW** [TRES MAUVAIS]
| Métrique | Valeur |
|----------|--------|
| **Avg docs per server** | 20,000 |
| **Avg distinct key values** | 0.2 |
| **Évaluation** | [1/5] Désastreux |

**Explication** :
- 200 entrepôts ÷ 1,000 serveurs = **0.2 entrepôts par serveur**
- Seulement 200 serveurs sont utilisés !
- Les 800 autres serveurs sont vides
- Certains serveurs auraient 100,000 documents (un entrepôt complet)
- **Ne PAS utiliser cette stratégie**

---

### OrderLine Collection

#### **3. OL - #IDC** [BON]
| Métrique | Valeur |
|----------|--------|
| **Avg docs per server** | 4,000,000 |
| **Avg distinct key values** | 1,000 |
| **Évaluation** | [4/5] Bon |

**Explication** :
- 1,000,000 clients ÷ 1,000 serveurs = 1,000 clients par serveur
- Distribution excellente des clés
- Chaque serveur gère ~4M documents (lignes de commande d'un groupe de clients)
- **Stratégie robuste**

---

#### **4. OL - #IDP** [ACCEPTABLE]
| Métrique | Valeur |
|----------|--------|
| **Avg docs per server** | 4,000,000 |
| **Avg distinct key values** | 100 |
| **Évaluation** | [3/5] Acceptable |

**Explication** :
- 100,000 produits ÷ 1,000 serveurs = 100 produits par serveur
- Distribution des clés bonne, mais...
- **Problème** : Certains produits (Apple) ont beaucoup plus de commandes que d'autres
- Si un produit populaire a 1M commandes, son serveur sera surchargé
- Les produits moins populaires laisseront des serveurs sous-utilisés
- **Hotspot potentiel**

---

### Product Collection

#### **5. Prod - #IDP** [EXCELLENT]
| Métrique | Valeur |
|----------|--------|
| **Avg docs per server** | 100 |
| **Avg distinct key values** | 100 |
| **Évaluation** | [5/5] Parfait |

**Explication** :
- Distribution 1-à-1 : chaque produit sur un serveur différent
- 100,000 produits ÷ 1,000 serveurs = 100 produits par serveur
- **Stratégie optimale pour Product**

---

#### **6. Prod - #brand** MAUVAIS
| Métrique | Valeur |
|----------|--------|
| **Avg docs per server** | 100 |
| **Avg distinct key values** | 5 |
| **Évaluation** | ⭐⭐ Mauvais |

**Explication** :
- 5,000 marques ÷ 1,000 serveurs = 5 marques par serveur
- **Résultat** : Seulement 5,000 serveurs sont vraiment utilisés
- 995,000 serveurs restent vides !
- Les marques avec beaucoup de produits (ex: Apple avec 50 produits) ont une concentration élevée
- **Distribution extrêmement déséquilibrée**
- **Ne PAS utiliser**

---

## Résumé Comparatif

| Stratégie | Cas d'usage | Qualité | Notes |
|-----------|------------|---------|-------|
| **St - #IDP** | Stock par produit | [5/5] | **MEILLEUR** pour Stock |
| **St - #IDW** | Stock par entrepôt | [1/5] | **À ÉVITER** - Trop peu d'entrepôts |
| **OL - #IDC** | OrderLine par client | [4/5] | Excellente distribution |
| **OL - #IDP** | OrderLine par produit | [3/5] | Risque de hotspots (produits populaires) |
| **Prod - #IDP** | Produit par ID | [5/5] | **MEILLEUR** pour Product |
| **Prod - #brand** | Produit par marque | [2/5] | **À ÉVITER** - Trop peu de marques |

---

## Recommandations

### À UTILISER
1. **St - #IDP** : Distribution parfaite des stocks
2. **OL - #IDC** : Distribution excellente des commandes par client
3. **Prod - #IDP** : Sharding naturel des produits

### À ÉVITER
1. **St - #IDW** : Trop peu d'entrepôts (200) pour 1,000 serveurs
2. **Prod - #brand** : Trop peu de marques (5,000) + concentration inégale
3. **OL - #IDP** : Risque de hotspots si distribution non uniforme

### Stratégie Optimale pour ce projet
- **Stock** : Sharding par #IDP
- **OrderLine** : Sharding par #IDC (ou #IDP si accès par produit fréquent)
- **Product** : Sharding par #IDP
