-- ============================================================================
-- TP3 - INIT POSTGRESQL
-- Plateforme e-commerce sénégalaise
-- ============================================================================
--
-- Architecture :
--
-- ecommerce_crm
--      └── clients
--
-- ecommerce_ventes
--      └── commandes
--
-- ecommerce_analytics
--      └── résultats Spark
--
-- PySpark fera le pont entre CRM et VENTES.
-- ============================================================================


-- ============================================================================
-- 0. REPARTIR DE ZERO
-- ============================================================================
-- Connexion initiale : base postgres
-- ============================================================================

\connect postgres

DROP DATABASE IF EXISTS ecommerce_crm;
DROP DATABASE IF EXISTS ecommerce_ventes;
DROP DATABASE IF EXISTS ecommerce_analytics;


-- ============================================================================
-- 1. CREATION DES BASES
-- ============================================================================

CREATE DATABASE ecommerce_crm;
CREATE DATABASE ecommerce_ventes;
CREATE DATABASE ecommerce_analytics;


-- ============================================================================
-- 2. CREATION DE L'UTILISATEUR PYSPARK
-- ============================================================================

DROP ROLE IF EXISTS spark_user;

CREATE ROLE spark_user
WITH LOGIN
PASSWORD 'Ucad2026!';


-- ============================================================================
-- DROIT DE CONNEXION AUX BASES
-- ============================================================================

GRANT CONNECT ON DATABASE ecommerce_crm
TO spark_user;

GRANT CONNECT ON DATABASE ecommerce_ventes
TO spark_user;

GRANT CONNECT ON DATABASE ecommerce_analytics
TO spark_user;


-- ============================================================================
-- 3. BASE ecommerce_crm
-- ============================================================================

\connect ecommerce_crm


-- ----------------------------------------------------------------------------
-- Table clients
-- ----------------------------------------------------------------------------

CREATE TABLE clients (
    client_id        VARCHAR(4)  NOT NULL,
    nom              VARCHAR(50) NOT NULL,
    ville            VARCHAR(30) NOT NULL,
    telephone        VARCHAR(20) NOT NULL,
    email            VARCHAR(60),
    date_inscription DATE        NOT NULL,

    PRIMARY KEY (client_id)
);


-- ----------------------------------------------------------------------------
-- Données clients
-- ----------------------------------------------------------------------------

INSERT INTO clients
(client_id, nom, ville, telephone, email, date_inscription)
VALUES
('C001', 'Awa Diop',
 'Dakar',
 '77 123 45 67',
 'awa.diop@gmail.com',
 '2024-09-12'),

('C002', 'Moussa Ndiaye',
 'Thies',
 '78 234 56 78',
 'moussa.ndiaye@yahoo.fr',
 '2024-10-03'),

('C003', 'Fatou Sall',
 'Dakar',
 '76 345 67 89',
 'fatou.sall@gmail.com',
 '2024-11-21'),

('C004', 'Cheikh Ba',
 'Dakar',
 '70 456 78 90',
 NULL,
 '2025-01-15'),

('C005', 'Aissatou Diallo',
 'Saint-Louis',
 '75 567 89 01',
 'aissatou.diallo@hotmail.com',
 '2025-02-08'),

('C006', 'Ibrahima Fall',
 'Kaolack',
 '77 678 90 12',
 'ibrahima.fall@gmail.com',
 '2025-03-19'),

('C007', 'Ousmane Sarr',
 'Ziguinchor',
 '78 789 01 23',
 'ousmane.sarr@gmail.com',
 '2025-04-27'),

('C008', 'Khady Gueye',
 'Dakar',
 '76 890 12 34',
 'khady.gueye@yahoo.fr',
 '2025-05-30'),

('C009', 'Mamadou Sy',
 'Touba',
 '70 901 23 45',
 NULL,
 '2025-06-14'),

('C010', 'Adama Kane',
 'Rufisque',
 '77 012 34 56',
 'adama.kane@gmail.com',
 '2025-07-22'),

('C011', 'Bineta Mbaye',
 'Mbour',
 '75 123 45 67',
 'bineta.mbaye@gmail.com',
 '2025-08-09'),

('C012', 'Serigne Diouf',
 'Louga',
 '78 210 43 65',
 'serigne.diouf@hotmail.com',
 '2025-09-05');


-- ----------------------------------------------------------------------------
-- Droits Spark
-- ----------------------------------------------------------------------------

GRANT USAGE ON SCHEMA public
TO spark_user;

GRANT SELECT ON ALL TABLES IN SCHEMA public
TO spark_user;

ALTER DEFAULT PRIVILEGES
IN SCHEMA public
GRANT SELECT ON TABLES
TO spark_user;


-- ----------------------------------------------------------------------------
-- Vérification CRM
-- ----------------------------------------------------------------------------

SELECT COUNT(*) AS nb_clients
FROM clients;


-- ============================================================================
-- 4. BASE ecommerce_ventes
-- ============================================================================

\connect ecommerce_ventes


-- ----------------------------------------------------------------------------
-- Table commandes
-- ----------------------------------------------------------------------------

CREATE TABLE commandes (
    commande_id    VARCHAR(5)  NOT NULL,
    client_id      VARCHAR(4)  NOT NULL,
    produit_id     VARCHAR(4)  NOT NULL,
    montant_fcfa   INTEGER     NOT NULL,
    moyen_paiement VARCHAR(20) NOT NULL,
    statut         VARCHAR(15) NOT NULL,
    date_commande  DATE        NOT NULL,

    PRIMARY KEY (commande_id)
);


-- ----------------------------------------------------------------------------
-- Données commandes
-- ----------------------------------------------------------------------------

INSERT INTO commandes
(commande_id, client_id, produit_id, montant_fcfa,
 moyen_paiement, statut, date_commande)
VALUES
('CMD01', 'C001', 'P001', 145000, 'Orange Money', 'livree',   '2025-10-02'),
('CMD02', 'C003', 'P002', 25000,  'Wave',         'livree',   '2025-10-05'),
('CMD03', 'C002', 'P010', 8500,   'especes',      'livree',   '2025-10-07'),
('CMD04', 'C001', 'P003', 32000,  'Orange Money', 'livree',   '2025-10-12'),
('CMD05', 'C008', 'P005', 57500,  'carte',        'livree',   '2025-10-15'),
('CMD06', 'C005', 'P007', 12000,  'Wave',         'en_cours', '2025-10-18'),
('CMD07', 'C004', 'P008', 74500,  'Orange Money', 'livree',   '2025-10-21'),
('CMD08', 'C010', 'P009', 15500,  'Wave',         'livree',   '2025-10-24'),
('CMD09', 'C006', 'P006', 39000,  'especes',      'annulee',  '2025-10-27'),
('CMD10', 'C003', 'P004', 28500,  'Orange Money', 'livree',   '2025-11-01'),
('CMD11', 'C009', 'P010', 8500,   'especes',      'livree',   '2025-11-04'),
('CMD12', 'C012', 'P008', 74500,  'carte',        'livree',   '2025-11-08'),
('CMD13', 'C001', 'P002', 25000,  'Wave',         'livree',   '2025-11-11'),
('CMD14', 'C008', 'P006', 39000,  'Orange Money', 'en_cours', '2025-11-14'),

-- C999 n'existe volontairement pas dans la table clients
('CMD15', 'C999', 'P004', 28500, 'Wave', 'livree', '2025-11-17'),

('CMD16', 'C002', 'P001', 145000, 'Orange Money', 'livree',   '2025-11-20'),
('CMD17', 'C005', 'P007', 12000,  'especes',      'annulee',  '2025-11-23'),
('CMD18', 'C004', 'P003', 32000,  'Wave',         'livree',   '2025-11-26'),
('CMD19', 'C010', 'P001', 145000, 'carte',        'livree',   '2025-11-29'),
('CMD20', 'C006', 'P010', 8500,   'Orange Money', 'livree',   '2025-12-02'),
('CMD21', 'C003', 'P002', 25000,  'Wave',         'en_cours', '2025-12-05'),
('CMD22', 'C009', 'P005', 57500,  'Orange Money', 'livree',   '2025-12-08'),
('CMD23', 'C012', 'P007', 12000,  'especes',      'livree',   '2025-12-11'),
('CMD24', 'C001', 'P008', 74500,  'carte',        'livree',   '2025-12-14'),
('CMD25', 'C008', 'P009', 15500,  'Wave',         'annulee',  '2025-12-17');


-- ----------------------------------------------------------------------------
-- Droits Spark
-- ----------------------------------------------------------------------------

GRANT USAGE ON SCHEMA public
TO spark_user;

GRANT SELECT ON ALL TABLES IN SCHEMA public
TO spark_user;

ALTER DEFAULT PRIVILEGES
IN SCHEMA public
GRANT SELECT ON TABLES
TO spark_user;


-- ----------------------------------------------------------------------------
-- Vérification VENTES
-- ----------------------------------------------------------------------------

SELECT COUNT(*) AS nb_commandes
FROM commandes;


-- Vérification de la commande orpheline
SELECT *
FROM commandes
WHERE client_id = 'C999';


-- ============================================================================
-- 5. BASE ecommerce_analytics
-- ============================================================================

\connect ecommerce_analytics


-- ----------------------------------------------------------------------------
-- Spark pourra écrire ses résultats ici
-- ----------------------------------------------------------------------------

GRANT USAGE, CREATE
ON SCHEMA public
TO spark_user;

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA public
TO spark_user;

ALTER DEFAULT PRIVILEGES
IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLES
TO spark_user;


-- ============================================================================
-- 6. VERIFICATIONS FINALES
-- ============================================================================

\connect postgres


-- Vérification utilisateur
SELECT rolname AS utilisateur
FROM pg_roles
WHERE rolname = 'spark_user';


-- Vérification des 3 bases
SELECT
    COUNT(*) AS nb_bases
FROM pg_database
WHERE datname LIKE 'ecommerce%';


SELECT
    datname AS base
FROM pg_database
WHERE datname LIKE 'ecommerce%'
ORDER BY datname;


-- ============================================================================
-- FIN DU SCRIPT
-- ============================================================================