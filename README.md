**********************************************************************************************************************************
# Chargement massif de données dans Apache Cassandra :
**********************************************************************************************************************************
## L'utilitaire DSBulk permet de charger des données à partir de fichiers CSV dans les tables de la base de données Cassandra NoSQL
**********************************************************************************************************************************
## Objectifs du TP : ##
* Comprendre les cas d'utilisation de DSBulk
* Utiliser les commandes DSBulk `load`, `unload` et `count`
* Découvrir les options DSBulk `-url`, `-k`, `-t`, `-m` et plus
* Explorer plusieurs exemples d'utilisation de DSBulk

Ce TP est également disponible en anglais sur le site [https://www.datastax.com/learn/cassandra-fundamentals](https://www.datastax.com/learn/cassandra-fundamentals)

#### Rappel pour retrouver les environnements éventuellement précédemment instanciés dans Gitpod : https://gitpod.io/workspaces
[# Pour afficher les workspaces déjà instanciés si besoin de faire du ménage :](https://gitpod.io/workspaces)

## C'est parti : lançons l'environnement du TP ici :

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/crystalloide/Cassandra-fundamentals-bulk-loading
)


**********************************************************************************************************************************
## Cliquer sur new workspace et patienter
**********************************************************************************************************************************

DataStax Bulk Loader (DSBulk) est un utilitaire de ligne de commande efficace, flexible, facile à utiliser et gratuit pour Apache Cassandra qui excelle dans le chargement, le déchargement et le comptage des données.
 
DSBulk permet de :

- Charger les données des fichiers CSV ou JSON dans la base de données
  
- Décharger les données stockées dans la base de données dans des fichiers CSV ou JSON
  
- Compter rapidement le nombre de lignes dans une table donnée

DSBulk est un bon choix pour les petits, moyens et grands ensembles de données. 

Il importe et exporte les données depuis et dans C* beaucoup plus rapidement que via des INSERT individuels, la commande COPY ou d'autres outils communautaires. 

Pour un très grand volume de données distribuées, une alternative potentiellement plus rapide à DSBulk pourrait être le chargement des données avec Apache Spark.


    dsbulk [options]

    DSBulk prend en charge les trois commandes suivantes :

	load 		Charge les données à partir de fichiers externes dans Apache Cassandra
	unload 		Décharge des données d'Apache Cassandra dans des fichiers
	count 		Calcule des statistiques sur une table Cassandra (nombre total de lignes, etc)
	
	

Exemple de paramètres : 

	url 		URL ou chemin de la ou des ressources à lire ou à écrire
	k 			Keyspace utilisé pour le chargement/déchargement/comptage
	t 			Table de chargement/déchargement/comptage
	m 			Mappage des champs de fichier aux colonnes de table
	cl 			Niveau de cohérence à utiliser pour la lecture ou l'écriture (LOCAL_ONE est la valeur par défaut)
	header 		Activez ou désactivez si les fichiers à lire ou à écrire commencent par une ligne d'en-tête
	logDir 		Répertoire pour les journaux (le répertoire actuel est la valeur par défaut)
	query 		requête CQL à utiliser pour le chargement/déchargement/comptage
	skipRecords Nombre de lignes (autres que d'en-tête) à ignorer lors de la lecture d'un fichier

✅ Installation de dsbulk : 
      
    cd /workspace/Cassandra-utilitaires-dsbulk-loader
      
    wget -q https://downloads.datastax.com/dsbulk/dsbulk.tar.gz
      
    tar -xzf dsbulk.tar.gz
      
    rm dsbulk.tar.gz

    mv dsbulk* "$GITPOD_REPO_ROOT/dsbulk"
    
    echo $PATH
    export PATH="$GITPOD_REPO_ROOT/dsbulk/bin:$PATH"
    echo $PATH 

## Pour en savoir - beaucoup - plus : 
    dsbulk help 



**********************************************************************************************************************************

✅ Installation de la version 8 de Java pour cassandra 3.11.x : 

    sdk install java 8.0.345.fx-zulu

Remarque : faire "Y" pour que cette version devienne celle par défaut.


✅ Vérification de la version de Java : 

    java -version
    

✅ Installation de la version 8 de Java pour cassandra 3.11.x : 

    which python2 
    
    sudo apt-get install python2

    python2 -V
    

✅ Installation de cassandra 3.11.x : 

Téléchargement du tarball avec les binaires sur l'un des sites miroirs de la fondation Apache : 

    curl -OL https://downloads.apache.org/cassandra/3.11.19/apache-cassandra-3.11.19-bin.tar.gz

    
Vérification de l'intégrité du tarball ainsi téléchargé avec le hash en utilisant GPG : 

    gpg --print-md SHA256 apache-cassandra-3.11.19-bin.tar.gz
    
Comparaison de la signature du fichier Tarball avec le contenu du fichier SHA256 récupéré en ligne :

    curl -L https://downloads.apache.org/cassandra/3.11.19/apache-cassandra-3.11.19-bin.tar.gz.sha256


Décompression du tarball:

    tar xzvf apache-cassandra-3.11.19-bin.tar.gz
    
Les fichiers sont extraits dans le répertoire apache-cassandra-3.11.19/

Cela correspond donc au "tarball installation location".


    rm apache-cassandra-3.11.19-bin.tar.gz

    mv apache-cassandra-3.11.19 cassandra3
    

✅ On installe CQLSH :

    pip install -U cqlsh


✅ Mise à jour du Path : 
   
    echo $PATH
    export PATH="$GITPOD_REPO_ROOT/cassandra3/bin:$PATH"
    export PATH="$GITPOD_REPO_ROOT/cassandra3/tools/bin:$PATH"
    echo $PATH 



✅ Lancement de Cassandra : 

On aurait pu lancer via une image docker :
 	docker pull cassandra:4.0
        docker run -p 9042:9042 -d \
                 --name Cassandra \
                 -v ${PWD}/config/cassandra.yaml:/etc/cassandra/cassandra.yaml \
                 -v ${PWD}/config/cassandra-rackdc.properties:/etc/cassandra/cassandra-rackdc.properties cassandra:4.0

Mais on va lancer le cassandra que l'on a installé précédemment en local : 

    cd $GITPOD_REPO_ROOT/cassandra3
    
    bin/cassandra
    
Cela lancera Cassandra comme user Linux authentifié.

✅ Suivi du lancement de Cassandra (dans un autre temrinal "bash") :

    cd $GITPOD_REPO_ROOT/cassandra3
    
    tail -f /workspace/Cassandra-utilitaires-dsbulk-loader/cassandra3/logs/system.log
    

✅ Vérification que le noeud Cassandra 3.x est bien lancé :

    nodetool status


✅ Vérification de la version de Cassandra (3.11.x attendu):

    nodetool version


✅ Affichage en retour : 

    ReleaseVersion: 3.11.19


✅ Vérification que le noeud Cassandra 3.11.x est bien opérationnel : 

    nodetool status

✅ Affichage en retour : 

    Datacenter: datacenter1
    =======================
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address    Load       Tokens       Owns (effective)  Host ID                               Rack
    UN  127.0.0.1  70.87 KiB  256          100.0%            b10d4523-769e-41d4-aaaf-721ed30f4a22  rack1
    

### Création d'un keyspace et d'une table, puis insertion de données :


✅ Exécution de commandes CQL via CQLsh :

    cd $GITPOD_REPO_ROOT/cassandra3
    
   Création du keyspace et des 4 tables en utilisant le shell CQL (SQLSH) :
   
    bin/cqlsh -e "
    CREATE KEYSPACE IF NOT EXISTS ks_bulk_loading
    WITH replication = {
      'class': 'NetworkTopologyStrategy', 
      'datacenter1': 1 };

    USE ks_bulk_loading;

    CREATE TABLE IF NOT EXISTS users (
      id TEXT,
      gender TEXT,
      age INT,
      PRIMARY KEY ((id))
    );

    CREATE TABLE IF NOT EXISTS movies (
      id TEXT,
      title TEXT,
      year INT,
      duration INT,
      country TEXT,
      PRIMARY KEY ((id))
    );

    CREATE TABLE IF NOT EXISTS ratings_by_user (
      user_id TEXT,
      movie_id TEXT,
      rating INT,
      PRIMARY KEY ((user_id), movie_id)
    );

    CREATE TABLE IF NOT EXISTS ratings_by_movie (
      movie_id TEXT,
      user_id TEXT,
      rating INT,
      PRIMARY KEY ((movie_id), user_id)
    );"


✅ Vérification que les 4 tables ont bient été créées :

    bin/cqlsh -k ks_bulk_loading -e "DESCRIBE TABLES;"

✅ Affichage en retour :
    
    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ bin/cqlsh -k ks_bulk_loading -e "DESCRIBE TABLES;"

    movies  ratings_by_movie  ratings_by_user  users


✅ Chargement des utilisateurs dans la table :

Notre première tâche consiste à charger les données utilisateur à partir du fichier users.csv avec les champs d'en-tête user_id, sexe et âge

dans les utilisateurs de la table avec les colonnes identifiant, sexe et âge.

✅ Affichage des 5 premères lignes du fichier :

    head -n 5 $GITPOD_REPO_ROOT/assets/users.csv

✅ Affichage en retour :

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ head -n 5 $GITPOD_REPO_ROOT/assets/users.csv
    user_id,gender,age
    u1,M,17
    u2,M,12
    u3,M,14
    u4,F,16


✅ Vérification que le table est bien vide initialement :

    bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM users LIMIT 5;"
    
✅ Affichage en retour :

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM users LIMIT 5;"
    
     id | age | gender
    ----+-----+--------
    

Étant donné que les noms des champs du fichier et les noms des colonnes du tableau ne correspondent pas exactement, nous devons mapper explicitement les champs aux colonnes.

Il existe de nombreuses façons de procéder, comme nous allons le voir dans les exemples suivants.

✅ Chargement des données via le principe "name-to-name mapping" :

    dsbulk load -url $GITPOD_REPO_ROOT/assets/users.csv \
            -k ks_bulk_loading    \
            -t users              \
            -header true          \
            -m "user_id=id,       \
                gender=gender,    \
                age=age"          \
            -logDir /tmp/logs


✅ Affichage en retour :

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk load -url $GITPOD_REPO_ROOT/assets/users.csv \
    >             -k ks_bulk_loading    \
    >             -t users              \
    >             -header true          \
    >             -m "user_id=id,       \
    >                 gender=gender,    \
    >                 age=age"          \
    >             -logDir /tmp/logs

    Operation directory: /tmp/logs/LOAD_20230430-101132-320893
    total | failed | rows/s |  p50ms |  p99ms | p999ms | batches
    1,100 |      0 |    316 | 161.61 | 402.65 | 497.03 |    1.00
    Operation LOAD_20230430-101132-320893 completed successfully in 2 seconds.

Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/LOAD_20230430-101132-320893/checkpoint.csv




✅ Chargement des données via le principe "position-to-name mapping" :

    dsbulk load -url $GITPOD_REPO_ROOT/assets/users.csv \
            -k ks_bulk_loading    \
            -t users              \
            -header true          \
            -m "0=id,             \
                1=gender,         \
                2=age"            \
            -logDir /tmp/logs
			
			

✅ Affichage en retour :		

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk load -url $GITPOD_REPO_ROOT/assets/users.csv \
    >             -k ks_bulk_loading    \
    >             -t users              \
    >             -header true          \
    >             -m "0=id,             \
    >                 1=gender,         \
    >                 2=age"            \
    >             -logDir /tmp/logs
    
    Operation directory: /tmp/logs/LOAD_20230430-101218-019530
    total | failed | rows/s | p50ms |  p99ms | p999ms | batches
    1,100 |      0 |    384 | 69.11 | 193.99 | 196.08 |    1.00
    Operation LOAD_20230430-101218-019530 completed successfully in 2 seconds.

Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/LOAD_20230430-101218-019530/checkpoint.csv
			
			

✅ Chargement des données en supprimant le header du fichier et en spécifiant le nom de chaque colonne :

    dsbulk load -url $GITPOD_REPO_ROOT/assets/users.csv \
            -k ks_bulk_loading    \
            -t users              \
            -header false         \
            -skipRecords 1        \
            -m "id, gender, age"  \
            -logDir /tmp/logs



✅ Affichage en retour :		

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk load -url $GITPOD_REPO_ROOT/assets/users.csv \
    >             -k ks_bulk_loading    \
    >             -t users              \
    >             -header false         \
    >             -skipRecords 1        \
    >             -m "id, gender, age"  \
    >             -logDir /tmp/logs
        
    Operation directory: /tmp/logs/LOAD_20230430-101255-984151
    total | failed | rows/s | p50ms |  p99ms | p999ms | batches
    1,100 |      0 |    401 | 65.51 | 188.74 | 200.28 |    1.00
    Operation LOAD_20230430-101255-984151 completed successfully in 2 seconds.

Checkpoints for the current operation were written to checkpoint.csv

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/LOAD_20230430-101255-984151/checkpoint.csv




✅ Affichage de 5 lignes de la table :

    bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM users LIMIT 5;"


✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ cqlsh -k ks_bulk_loading -e "SELECT * FROM users LIMIT 5;"
    
     id    | age | gender
    -------+-----+--------
       u37 |  17 |      M
     u1011 |  34 |      F
      u137 |  17 |      M
      u228 |  45 |      F
      u193 |  12 |      M
    
    (5 rows)
    


### Chargement de la table "films" :

Ensuite, on alimente les données sur les films à partir du fichier movies.csv dans la table movies.

✅ Affichage des 5 premières lignes du fichier en entrée :

    head -n 5 $GITPOD_REPO_ROOT/assets/movies.csv


✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ head -n 5 $GITPOD_REPO_ROOT/assets/movies.csv
        
    movie_id,title,year,duration,country
    m1,Once Upon a Time in the West,1968,165,Italy
    m2,"10,000 B.C.",2008,109,United States
    m3,It's Always Fair Weather,1955,102,United States
    m4,Tarzan,1999,84,United States


✅ Vérification que le table movies est bien vide initialement :

    bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM movies LIMIT 5;"

✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ cqlsh -k ks_bulk_loading -e "SELECT * FROM movies LIMIT 5;"
    
     id | country | duration | title | year
    ----+---------+----------+-------+------
    
    (0 rows)


✅ Chargement des données :

    dsbulk load -url $GITPOD_REPO_ROOT/assets/movies.csv \
            -k ks_bulk_loading     \
            -t movies              \
            -header true           \
            -m "movie_id=id,       \
                title=title,       \
                year=year,         \
                duration=duration, \
                country=country"   \
            -logDir /tmp/logs


✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk load -url $GITPOD_REPO_ROOT/assets/movies.csv \
    >             -k ks_bulk_loading     \
    >             -t movies              \
    >             -header true           \
    >             -m "movie_id=id,       \
    >                 title=title,       \
    >                 year=year,         \
    >                 duration=duration, \
    >                 country=country"   \
    >             -logDir /tmp/logs
    Operation directory: /tmp/logs/LOAD_20230430-101651-214499
    total | failed | rows/s | p50ms |  p99ms | p999ms | batches
      920 |      0 |    625 | 31.59 | 106.43 | 123.21 |    1.00
    Operation LOAD_20230430-101651-214499 completed successfully in 1 second.
    
Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/LOAD_20230430-101651-214499/checkpoint.csv



✅ Affichage de 5 lignes de la table :

    bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM movies LIMIT 5;"


✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ cqlsh -k ks_bulk_loading -e "SELECT * FROM movies LIMIT 5;"
    
     id   | country       | duration | title               | year
    ------+---------------+----------+---------------------+------
     m822 | United States |      114 |            Superbad | 2007
     m258 | United States |      112 |      The Band Wagon | 1953
     m618 |       Germany |      110 |            The Wave | 2008
     m512 | United States |      130 |                Fame | 1980
     m763 | United States |      110 | From Dusk Till Dawn | 1995
    
    (5 rows)



### Chargement des notes sur les films (ratings) :

Chargeons les classements de films à partir du fichier ratings.csv dans les tables ratings_by_user et ratings_by_movie :

✅ Affichage des 5 premières lignes du fichier ratings.csv :

    head -n 5 $GITPOD_REPO_ROOT/assets/ratings.csv


✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ head -n 5 $GITPOD_REPO_ROOT/assets/ratings.csv
    user_id,movie_id,rating
    u289,m1,8
    u957,m1,5
    u248,m1,8
    u438,m1,6
    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ 



✅ Vérification que les tables sont bien vides initialement :

    bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM ratings_by_user LIMIT 5;"

    bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM ratings_by_movie LIMIT 5;"

✅ Affichage en retour :	


    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ cqlsh -k ks_bulk_loading -e "SELECT * FROM ratings_by_user LIMIT 5;"

    SELECT * FROM ratings_by_movie LIMIT 5;"
    
     user_id | movie_id | rating
    ---------+----------+--------
    
    (0 rows)

    
    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ cqlsh -k ks_bulk_loading -e "SELECT * FROM ratings_by_movie LIMIT 5;"

     movie_id | user_id | rating
    ----------+---------+--------
    
    (0 rows)


✅ Chargement des données dans la table ratings_by_user : 

Remarque : Les noms des champs du fichier et les noms des colonnes du tableau correspondent. 

Il n'y a donc pas besoin de fournir un mapping explicite cette fois.

    dsbulk load -url $GITPOD_REPO_ROOT/assets/ratings.csv \
            -k ks_bulk_loading      \
            -t ratings_by_user      \
            -header true            \
            -logDir /tmp/logs


✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk load -url $GITPOD_REPO_ROOT/assets/ratings.csv \
    >             -k ks_bulk_loading      \
    >             -t ratings_by_user      \
    >             -header true            \
    >             -logDir /tmp/logs
    Operation directory: /tmp/logs/LOAD_20230430-102108-195154
     total | failed | rows/s | p50ms |  p99ms | p999ms | batches
    48,094 |      0 |  3,350 | 50.12 | 216.01 | 788.53 |    1.03
    Operation LOAD_20230430-102108-195154 completed successfully in 14 seconds.
    
Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/LOAD_20230430-102108-195154/checkpoint.csv



✅ Chargement des données dans la table ratings_by_movie :

    dsbulk load -url $GITPOD_REPO_ROOT/assets/ratings.csv \
            -k ks_bulk_loading      \
            -t ratings_by_movie     \
            -header true            \
            -logDir /tmp/logs

✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk load -url $GITPOD_REPO_ROOT/assets/ratings.csv \
    >             -k ks_bulk_loading      \
    >             -t ratings_by_movie     \
    >             -header true            \
    >             -logDir /tmp/logs
    Operation directory: /tmp/logs/LOAD_20230430-102212-984975
     total | failed | rows/s |  p50ms |  p99ms | p999ms | batches
    48,094 |      0 |  7,470 | 113.23 | 486.54 | 503.32 |   22.14 
    Operation LOAD_20230430-102212-984975 completed successfully in 5 seconds.
    
Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/LOAD_20230430-102212-984975/checkpoint.csv



✅ Affichage de 5 lignes pour chacune des 2 tables :

    bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM ratings_by_user LIMIT 5;"
    
    bin/cqlsh -k ks_bulk_loading -e "SELECT * FROM ratings_by_movie LIMIT 5;"


✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ cqlsh -k ks_bulk_loading -e "SELECT * FROM ratings_by_user LIMIT 5;"

    SELECT * FROM ratings_by_movie LIMIT 5;"
    
     user_id | movie_id | rating
    ---------+----------+--------
     u37 |     m119 |      5
     u37 |     m160 |      5
     u37 |     m190 |      4
     u37 |     m197 |      9
     u37 |     m223 |      7

    (5 rows)
    
    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ cqlsh -k ks_bulk_loading -e "SELECT * FROM ratings_by_movie LIMIT 5;"

     movie_id | user_id | rating
    ----------+---------+--------
     m822 |   u1087 |      4
     m822 |    u134 |      4
     m822 |    u157 |      8
     m822 |    u167 |      5
     m822 |    u208 |      6

    (5 rows)


### Déchargement et comptage :

Les commandes de déchargement et de comptage peuvent être utilisées pour exporter des données depuis Cassandra et faire des décomptes simples de lignes.


✅ Déchargement complet des lignes de la table ratings_by_movie :

    dsbulk unload -url all_ratings    \
              -k ks_bulk_loading  \
              -t ratings_by_movie \
              -header true        \
              -logDir /tmp/logs 
              
✅ Affichage en retour :	

    Gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk unload -url all_ratings    \
    >               -k ks_bulk_loading  \
    >               -t ratings_by_movie \
    >               -header true        \
    >               -logDir /tmp/logs 
    Operation directory: /tmp/logs/UNLOAD_20230430-102627-201808
     total | failed | rows/s | p50ms |  p99ms | p999ms
    48,094 |      0 | 15,153 | 45.17 | 190.84 | 278.92
    Operation UNLOAD_20230430-102627-201808 completed successfully in 2 seconds.
    
Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/UNLOAD_20230430-102627-201808/checkpoint.csv



✅ Déchargement complet des lignes de la table ratings_by_movie à l'aide d'une requête :

    dsbulk unload -url m267_ratings   \
              -k ks_bulk_loading  \
              -query "            \
    SELECT *                      \
    FROM ratings_by_movie         \
    WHERE movie_id = 'm267'"      \
              -header true        \
              -logDir /tmp/logs 

✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk unload -url m267_ratings   \
    >               -k ks_bulk_loading  \
    >               -query "            \
    > SELECT *                          \
    > FROM ratings_by_movie             \
    > WHERE movie_id = 'm267'"          \
    >               -header true        \
    >               -logDir /tmp/logs 
    Operation directory: /tmp/logs/UNLOAD_20230430-102700-706420
    total | failed | rows/s | p50ms | p99ms | p999ms
       70 |      0 |    108 | 65.67 | 65.80 |  65.80
    Operation UNLOAD_20230430-102700-706420 completed successfully in less than one second.
    
Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/UNLOAD_20230430-102700-706420/checkpoint.csv



✅ Affichage de 5 lignes de chaque fichier csv résultant :

    head -n 5 all_ratings/*
    
    head -n 5 m267_ratings/*


✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ head -n 5 m267_ratings/*
    ==> m267_ratings/output-000001.csv <==
    movie_id,user_id,rating
    m267,u1013,6
    m267,u185,5
    m267,u436,3
    m267,u678,5
    
    ==> m267_ratings/output-000002.csv <==
    movie_id,user_id,rating
    m267,u1085,7
    m267,u20,5
    m267,u476,4
    m267,u689,8
    
    ==> m267_ratings/output-000003.csv <==
    movie_id,user_id,rating
    m267,u1100,4
    m267,u24,6
    m267,u486,5
    m267,u694,8
    
    ==> m267_ratings/output-000004.csv <==
    movie_id,user_id,rating
    m267,u117,7
    m267,u248,5
    m267,u514,4
    m267,u707,6
    
    ==> m267_ratings/output-000005.csv <==
    movie_id,user_id,rating
    m267,u118,5
    m267,u263,5
    m267,u517,7
    m267,u721,8
    
    ==> m267_ratings/output-000006.csv <==
    movie_id,user_id,rating
    m267,u123,5
    m267,u265,8
    m267,u592,7
    m267,u729,5
    
    ==> m267_ratings/output-000007.csv <==
    movie_id,user_id,rating
    m267,u176,7
    m267,u38,7
    m267,u62,4
    m267,u731,7
    
    ==> m267_ratings/output-000008.csv <==
    movie_id,user_id,rating
    m267,u183,5
    m267,u420,3
    m267,u627,7
    m267,u732,8
    



✅ Comptage de toutes les lignes de la table ratings_by_movie :

    dsbulk count  -k ks_bulk_loading  \
              -t ratings_by_movie \
              -logDir /tmp/logs 

✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk count  -k ks_bulk_loading  \
    >               -t ratings_by_movie \
    >               -logDir /tmp/logs 
    Operation directory: /tmp/logs/COUNT_20230430-102828-031314
     total | failed | rows/s | p50ms |  p99ms | p999ms
    48,094 |      0 | 40,529 | 39.80 | 159.38 | 159.38
    Operation COUNT_20230430-102828-031314 completed successfully in less than one second.

Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/COUNT_20230430-102828-031314/checkpoint.csv




✅ Comptage de toutes les lignes de la table "ratings_by_movie" à l'aide d'une requête :

    dsbulk count  -k ks_bulk_loading  \
              -query "                \
    SELECT *                          \
    FROM ratings_by_movie             \
    WHERE movie_id = 'm267'"          \
              -logDir /tmp/logs 

✅ Affichage en retour :	

    gitpod /workspace/cassandra-fundamentals-bulk-loading (main) $ dsbulk count  -k ks_bulk_loading  \
    >               -query "            \
    > SELECT *                          \
    > FROM ratings_by_movie             \
    > WHERE movie_id = 'm267'"          \
    >               -logDir /tmp/logs 
    Operation directory: /tmp/logs/COUNT_20230430-102909-103232
    total | failed | rows/s | p50ms | p99ms | p999ms
       70 |      0 |    243 |  6.67 |  6.68 |   6.68
    Operation COUNT_20230430-102909-103232 completed successfully in less than one second.
    
Checkpoints for the current operation were written to checkpoint.csv.

To resume the current operation, re-run it with the same settings, and add the following command line flag:

--dsbulk.log.checkpoint.file=/tmp/logs/COUNT_20230430-102909-103232/checkpoint.csv

**********************************************************************************************************************************
## Fin du TPxx - Chargement de données en masse avec dsbulk dans Apache Cassandra
**********************************************************************************************************************************

