#!/usr/bin/env python
# coding: utf-8




# IMPORTATION LES LIBRAIRIES NECESSAIRE #


import pandas as pd
import numpy as np
import mysql
import mysql.connector
from mysql.connector import Error
import ssl
from flask import Flask
from flask import request
from flask_cors import CORS
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

# POUR ENVOYER LES RESULTATS DE REQUETE À LA VERSION JSON A NOTRE PAGE WEB #

import sys
import json




#CREATION UNE FONCTIONNE AFIN DE COMMENCER LE PROCESS DE PREDICTION
def cluster(mots):
    text_user = mots
    # TELECHARGHER LA BASE DE DONNÉES DANS UN VARIABLE DATAFRAME #
    mainDataFrame = pd.read_csv("final.csv",encoding='utf8')


# TF-IDF signifie "Term Frequency - Inverse Document Frequency". #
# C'est une technique pour quantifier un mot dans les documents, #
# nous calculons généralement un poids à chaque mot qui signifie #
# l'importance du mot dans le document et le corpus.             #
# Cette méthode est une technique largement utilisée dans la     #
# recherche d'informations et l'exploration de textes            #



# CRÉER UN VARIABLE TTfidfVectorizer #


    tfv = TfidfVectorizer(ngram_range = (1,1))


# TRANSFORMEZ LA COLONNE RESUME AU FORMAT VECTEUR DE POIDS #


    vec_text = tfv.fit_transform(mainDataFrame.loc[:,"resume"])
#Retourner une liste des mots fréquants.
#words = tfv.get_feature_names()
#df.to_sql('temp_table', engine, if_exists='replace')


# CONFIGURATION LE CLUSTERING AVEC KMEAN #


    kmeans = KMeans(n_clusters = 20, n_init = 17, n_jobs = -1, tol = 0.01, max_iter = 200)


# FIT LES DONNÉES AVEC LA VECTEUR vec_text #


    kmeans.fit(vec_text)

# Transformer les nombres aux mots correspondants #
#common_words = kmeans.cluster_centers_.argsort()[:,-1:-11:-1]             #
#for num, centroid in enumerate(common_words):                             #
 #   print(str(num) + ' : ' + ', '.join(words[word] for word in centroid)) #

    #Associer chaque annonce au son cluster correspondant
    mainDataFrame['category'] = kmeans.labels_
    #TRIER LES RESULTAT PAR LA COLONNE CATEGORY
    mainDataFrame = mainDataFrame.sort_values(by=['category'])

    # TRANSFORMEZ LE TEXTE SAISI PAR UTILISATEUR AU FORMAT VECTEUR DE POIDS #
    vec_user_text = tfv.transform([text_user])
    # PREDICTION LE CLUSTER#
    prediction = kmeans.predict(vec_user_text)
#print(prediction)
    #RECUPERER LE NUMERO DE CLUSTER ET COPIER 15 PREMIERS RESULTATS
    dfnew=mainDataFrame.loc[mainDataFrame['category'] == prediction[0]]
    resultat= dfnew.head(15)
    arr=[]
    #CREER UN ARRAY AFIN DE VERSER LES ANNONCES AU FORMAT JSON POUR LES ENVOYER A LA PAGE index.html
    for index, row in resultat.iterrows():
        arr.append(eval(resultat.loc[index,:].to_json()))
    row_json = json.dumps(arr)

    return row_json
#print(row_json)    

#UTILISER FLASK POUR RECUPERER ET ENVOYER LES DONNNEES ENTRE PAGE INDEX.HTML ET CE FICHIER PYTHON
app = Flask(__name__)
CORS(app)

@app.route('/')
def hello():
        #Récuperer le texte depuis la page index.html#
        mots = request.args.get('mots')
        row = cluster(mots)
        return row

if __name__ == '__main__':
        app.run(host='0.0.0.0',port=5001)

