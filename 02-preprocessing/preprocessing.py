#!/usr/bin/env python
# coding: utf-8


#Tout d'abord il faut importer les librairies dont j'ai besoin pour prepocessing mes données
#!pip install langdetect
import pandas as pd
import numpy as np
#Pour tokenisation et normalisation les corpus
import nltk
from nltk import *
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.tag.stanford import StanfordPOSTagger
from nltk import StanfordTagger
#Detecteur lanquage du text
from langdetect import detect
import time
import re
import string
import spacy
from spacy.lang.fr.stop_words import STOP_WORDS as fr_stop
from spacy.lang.en.stop_words import STOP_WORDS as en_stop
#Pour clustering les annonces
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans



#Création les fonnoctionnes 

#1# "normalisationCorpus" est une fonctionne qui va ajouter la colonne 'phrase_token' qui contiendra le nom
#de metier et sa description en miniscule
#et trois colonnes vides 'phrases_cible', 'resume' et 'category' afin d'être utiliser dans le preprocessing

def normalisationCorpus(df):
    donnees = df
    corpus = ""
    donnees['phrase_token']=""
    for index, row in donnees.iterrows():
        donnees.loc[index,'entreprise'] = str(donnees.loc[index,'entreprise'])
        corpus = "titre " +donnees.loc[index,'metier'].lower() +". "+ "sommaire "+donnees.loc[index,'sommaire'].lower()+". "+ donnees.loc[index,'description'].lower() +"."
        donnees.loc[index,'phrase_token'] = sent_tokenize(corpus)
    donnees['phrases_cible']=""
    donnees['resume']=""
    donnees['category'] = ""
    return donnees

#2# langueDetecte est une fonctionne qui va detecter les annonces anglais et puis il les supprimes

def langueDetecte(df):
    #count = 0
    donnees = df
    for index, row in donnees.iterrows():
        lang = detect(donnees.loc[index,'description'])
        if lang == "en" :
            #count+=1
            donnees = donnees.drop(index)
    #print(count)
    return donnees

#3# "nettoayage" est une fonctionne afin de chercher quelques mots ciblés qui peuvent contenir les compétences

def nettoayage(df):
    donnees = df
    collect=""
    corpus = ""
    mots_cible=["expérience","profil","compétence","mission","mont","capacité","dispos","maitris","titre","sommaire"]
    for index, row in donnees.iterrows():
        corpus = donnees.loc[index,"phrase_token"]
        for phrase in corpus:
            if any(mot in phrase for mot in mots_cible):
                collect = collect +" " + phrase + " \n"
        #Ici pour je supprime de mon corpus tous les mots cible dans la variable mots_cible
        mots = collect.split()

        motsResultat  = [mot for mot in mots if mot.lower() not in mots_cible]
        resultat = ' '.join(motsResultat)
        donnees.loc[index,"phrases_cible"] = resultat
        collect = ""
        
    return donnees

#4# "stopMots" est une fonctionne qui va supprimmer les mots frequants comme (de,le,la,je,avec,...)
#dans le corpus pour qu'il soit plus propre pour l'étape de machine learning

def stopMots(df):
    corpus = ""
    sans=""
    monList = ['de','et','vous','des','à','la','en','les','sur','vos','le','leur','',"l'",'pour','avec','êtes','votre','chaque','est','sont','du','ils','leurs','que','par','un','une','revenus','première',]
    stopWords = list(stopwords.words('french'))+list(fr_stop) + list(en_stop)+monList
    donnees = df
    transl_table = dict( [ (ord(x), ord(y)) for x,y in zip( u"‘'’´“”–-",  u"''''\"\"--") ] )
    for index, row in donnees.iterrows():
        corpus = donnees.loc[index,"phrases_cible"].replace("d'"," ").replace("l'"," ")
        corpus = corpus.translate(transl_table)
        corpus = corpus.translate(str.maketrans('','',string.punctuation)).replace("’"," ").replace("'"," ")
        tokens = word_tokenize(corpus, language='french')
        for token in tokens:
            if token not in stopWords :
                sans = sans + " " + token+ " "
        donnees.loc[index,"resume"] = sans
        sans = ""
    return donnees




#IMPORTATION LES DONNEES

mainDataFrame = pd.read_csv("annonces.csv")

#PREMIERE VUE DE MES DONNEES

mainDataFrame.head(5)
mainDataFrame.tail(5)
mainDataFrame.shape


#Mon dataset contien 1888 LIGNES ET 9 COLONNES

(mainDataFrame.loc[:,"description"].isnull() == False).count()
#Et il n'y a pas de champs vide dans la colonne description


#NETTOYAGE : 

#dans mon data set Il y avait 71 annonces en anglais qu'il faut être supprimé
#Supprimer les annonce en Anglais
mainDataFrame = langueDetecte(mainDataFrame)

#je veux mettre id comme l'index de mon dataframe
mainDataFrame = mainDataFrame.set_index('id')

#Supprimer les dedoublons
mainDataFrame = mainDataFrame.drop_duplicates(subset='lien', keep="first")

#Normalisation à l'aide de la fonction "normalisationCorpus"
mainDataFrame = normalisationCorpus(mainDataFrame)

#Nettoayage les corpus à l'aide de la fonction "nettoayage"
mainDataFrame = nettoayage(mainDataFrame)

#Supprimer les mots frequants à l'aide de la fonction "stopMots"
mainDataFrame = stopMots(mainDataFrame)

# Resultat final :
mainDataFrame.head(5)

#TESTING

#TF-IDF
#F-IDF signifie "Term Frequency - Inverse Document Frequency"
#C'est une technique pour quantifier un mot dans les documents,
#nous calculons généralement un poids à chaque mot qui signifie
#l'importance du mot dans le document et le corpus.            
#Cette méthode est une technique largement utilisée dans la    
#recherche d'informations et l'exploration de textes.


# CRÉER UN VARIABLE TTfidfVectorizer 
tfv = TfidfVectorizer(ngram_range = (1,1))

#TRANSFORMEZ LA COLONNE RESUME AU FORMAT VECTEUR DE POIDS
vec_text = tfv.fit_transform(mainDataFrame.loc[:,"resume"])

#Retourner une liste des mots fréquants.
words = tfv.get_feature_names()

#Configurer kmeans clustering
kmeans = KMeans(n_clusters = 20, n_init = 17, n_jobs = -1, tol = 0.01, max_iter = 200)
#fit les données 
kmeans.fit(vec_text)
# Transformer les nombres aux mots correspondants
#common_words = kmeans.cluster_centers_.argsort()[:,-1:-11:-1]
#for num, centroid in enumerate(common_words):
#    print(str(num) + ' : ' + ', '.join(words[word] for word in centroid))


    
#Associer chaque annonce au son cluster correspondant
mainDataFrame['category'] = kmeans.labels_

#Voir le resultat
mainDataFrame.head(5)

#Exporter les données traitées finales
mainDataFrame.to_csv('final.csv',header=True)


#try:
#    connection = mysql.connector.connect(host='172.28.100.229',database='projet',user='azad',password='886622')

#except Error as e:
#    print("err:", e)
    
#cursor = connection.cursor()
#cursor.execute('''DROP TABLE IF EXISTS final''')
#cursor.execute('''CREATE TABLE final (id int, motcle VARCHAR(255),metier VARCHAR(255), entreprise VARCHAR(255),location VARCHAR(255),datedannoce VARCHAR(255),lien LONGTEXT, sommaire LONGTEXT, description LONGTEXT,phrase_token LONGTEXT, phrases_cible LONGTEXT,resume LONGTEXT,category int,PRIMARY KEY(id));''')

#connection.commit()
#for index, row in mainDataframe.iterrows():
#    ids = index.astype(int)
#    motcle = mainDataframe.loc[index,"motcle"]
#    metier = mainDataframe.loc[index,"metier"]
#    entreprise = str(mainDataframe.loc[index,"entreprise"])
#    location = mainDataframe.loc[index,"location"]
#    datedannonce = mainDataframe.loc[index,"datedannoce"]
#    lien = mainDataframe.loc[index,"lien"]
#    sommaire = mainDataframe.loc[index,"sommaire"]
#    description = mainDataframe.loc[index,"description"]
#    phrase_token =( mainDataframe.loc[index,"phrase_token"])
#    phrases_cible = mainDataframe.loc[index,"phrases_cible"]
#    resume = mainDataframe.loc[index,"resume"]
#    category = (mainDataframe.loc[index,"category"]).astype(int)
#    cursor2 = connection.cursor()
#    sql = '''INSERT INTO `final`(`id`, `motcle`, `metier`, `entreprise`, `location`, `datedannoce`, `lien`, `sommaire`, `description`, `phrase_token`, `phrases_cible`, `resume`, `category`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
#    cursor2.execute(sql,(ids,motcle, metier, entreprise, location , datedannonce, lien, sommaire,description,phrase_token,phrases_cible,resume,category,))
#    cursor2.close()
#    connection.commit()


