#!/usr/bin/env python
# coding: utf-8

#Time pour ralentir le proccess de scraping
import time
#Request pour envoyer et recevoir les requette de serveur
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
#Beautifulsoup pour récuperer les balise html et extraire les données
from bs4 import BeautifulSoup
import mysql
import mysql.connector
from mysql.connector import Error
import ssl



#Ici j'ai essayé de connecter 
try:
    connection = mysql.connector.connect(host='172.28.100.229',database='projet',user='azad',password='886622')

except Error as e:
    print("err:", e)

#cursor = connection.cursor()
#cursor.execute('''DROP TABLE IF EXISTS annonces''')
#cursor.execute('''CREATE TABLE annonces (id int AUTO_INCREMENT, motcle VARCHAR(255),metier VARCHAR(255), entreprise VARCHAR(255),location VARCHAR(255),datedannoce VARCHAR(255),lien LONGTEXT,description LONGTEXT, PRIMARY KEY(id));''')

#connection.commit()




#https://fr.indeed.com/emplois?q=data%20analyst&l=Lille
#je vais remplacer nom d'emploi et la ville avec {} 
#pour que je puisse chercher autant ville ou metier que je desire
#url_model='https://fr.indeed.com/emplois?q={}&l={}'


#Ici j'ai créé une fonctionne afin de retourner le vrai lien en lui applenat avec nom de metier et un lieu
def recupere_url(n_metier, lieu):
    url_model='https://fr.indeed.com/emplois?q={}&l={}'
    vrai_url=url_model.format(n_metier, lieu)
    return vrai_url






# Une fonctionne afin d'ouvrire chaque annonce et scraper sa description

def recupere_decription(lien):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
    time.sleep(20)
    requet = requests.get(lien)
    soup_html= BeautifulSoup(requet.text, 'html.parser')
    description_job=soup_html.find('div', 'jobsearch-jobDescriptionText')
    reponse = description_job.get_text(separator=u' ')
    return reponse

# Une fonctionne pour récuperer les informations de chaque bloque comme nom de job, nom d'entreprise, 
# lien de job et son lieu

def recupere_donnees(metier):

    #Je commence par création d'un dictionnaire afin de regrouper tous infos dont j'ai besoin de chaque métier
    donnees = {}
    
    # Je vais stocker les codes HTML bruts dans une variable que j'ai nommé 'job'
    
    job = metier
    
    #Nom de métier est dans un balise <a> qui est dans un autre balise <h2>
    #Donc j'utilise le methode get avec atrribut "title" pour récuperer le nom de metier
    #Et en fin j'ai fait un strip() pour supprimer les spaces et nouveau ligne qui peuvent être avant ou après
    #le nom de metier
    
    #donnees['motcle'] = motcle
    donnees['metier'] = job.h2.a.get("title").strip()
    

    #Nom d'entreprise
    try:
    
        entreprise_n = job.find('span',{'class':'company'})
        if(entreprise_n) :
            donnees['entreprise'] = str(entreprise_n.get_text()).strip()
        
    except AttributeError:
        donnees['entreprise'] = ''
    
    
    #Location de job
    try:
        
        location_main = job.find('div', 'recJobLoc').get('data-rc-loc')
        location_div = job.find('div','location')
        location_span = job.find('span',{'class':'location'})
    
        if(location_main) :
        
            donnees['location'] = location_main
        
        elif(location_span) :
        
            donnees['location'] = location_span.get_text()
        
        elif(location_div) :
        
            donnees['location'] = location_div2.get_text()
    except AttributeError:
        
        donnees['location'] = ''
    
    #La date d'annonce
    try:
        donnees['datedannoce'] = job.find('span',{'class':'date'}).get_text()
    except AttributeError:
        donnees['datedannoce'] = ''
        
    
    sub_lien = job.h2.a.get('href')
    lienDeAnnonce = 'https://fr.indeed.com' + sub_lien
    donnees['lien'] = lienDeAnnonce
    
    try:
        donnees['sommaire'] = job.find('div',{'class':'summary'}).text.strip()
    except AttributeError:
        donnees['sommaire']= ''
    #Appeler la fonctionne recupere_description pour obtenier la description d'annonce 
    #Et le verser dans la dictionnaire
    try:
        donnees['description'] = recupere_decription(lienDeAnnonce)
    except AttributeError:
        donnees['description'] = ''
    #infos = (nomDeMetier, nomDeEntreprise,location, dateDeAnnonce, salaire, lienDeAnnonce, sommaire, descDeMetier)
    #Et à la fin retourner les donnes
    
    return donnees





#Ici j'ai créé une fonctionne pour scrapper la page de resultat de metiers et recuperer les bloques des annonces
#n_metier, ville
def scrape_lien(n_metier,lieu):

    motCle = n_metier

    url = recupere_url(n_metier, lieu)
    print(url)

    #pour éviter d'être bloqué par le site il faut changer le header
    while True : 
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
        
        #'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
        #ce code va envoyer un requet à site indeed et puis va retourner sa réponse dans la variable reponse que j'ai crée
        #Si le process est bien fait la variable reponse va nous retourner 200 qui represent la succes

        time.sleep(20)
        reponse=requests.get(url,headers=headers,verify=False)
        print(reponse)

        # maintenant que la reponse est ok je peux récuperer les balises HTML avec "text" attribut
        #je vais appeler BeautifulSoup pour créer un objet à fin de naviger dans les balises

        soup = BeautifulSoup(reponse.text, 'html.parser')
        print(soup)

        #chaque annoce est dans un balise(bloque_job) div avec le nom de class "jobsearch-SerpJobCard"
        #qui est en commun avec tous les job annonces
        #je vais recuperer identifier et trouver tous les balise avec le nom de class "jobsearch-SerpJobCard"
        #en utilisant le methode find_all dans beautiful soup

        bloques_job = soup.find_all('div', 'jobsearch-SerpJobCard')
        print(len(bloques_job))
        
        for bloque in bloques_job:
            donnees = recupere_donnees(bloque)

            #print("metier : "+donnees['metier']+" Nom d'entreprise : "+donnees['entreprise']+" location : " +donnees['location']+"\n")
            
            cursor2 = connection.cursor()
            sql = '''INSERT INTO `annonces` (`motcle`,`metier`, `entreprise`, `location`, `datedannoce`,`lien`,`sommaire`,`description`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
            cursor2.execute(sql,(motCle, donnees['metier'], donnees['entreprise'],donnees['location'], donnees['datedannoce'], donnees['lien'], donnees['sommaire'],donnees['description'],))
            cursor2.close()
            connection.commit()
    
        # Après première page c'est le tour des pages Suivantes
        # Sur la premiere page que j'ai escrappé, j'ai remarqué que la deuxieme et les pages suivants sont dans un
        # balise <a> avec l'attribut aria-lable et Suivant, 
        # donc je vais essayer de trouver ce balise dans ma première page
        try :
            url = 'https://fr.indeed.com' + soup.find('a',{'aria-label':'Suivant'}).get("href")
        except AttributeError:

            break

            
#COMMENCER À SCRAOER LES DONNÉES
liste_metiers=['data analyste','informatique','seo','marketing','python','js','cloud','web']
for item in liste_metiers:
    scrape_lien(item,'lille')

