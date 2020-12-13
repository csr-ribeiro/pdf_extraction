# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 14:49:33 2020

@author: cristiano.ribeiro
"""

from tabula import read_pdf
import tabula
from pandas import DataFrame
import pandas as pd
import numpy as np
import psycopg2
import os
import re
import string


def buscandopdf(caminhopdf):
    
    pasta = caminhopdf
    caminho = []
    for l in os.listdir(pasta):
        if l.endswith('.pdf'):
            caminho = caminho + [l]
    caminho = [os.path.join(pasta, name) for name in caminho]
    return(caminho)


def lerPDF(arquivo):
    lerdados = read_pdf(arquivo,pages='all',encoding='utf-8', stream=True)             #carrega os pdf
    return(lerdados)



def limpadados(dados):
    
    dados = dados.dropna(axis=0, thresh=2)                                                  # remove as linhas onde só existe um dado
    dados = dados.dropna(axis=1, how='all')                                                 # remove as colunas que não tem nenhum dado
    try:
        for col in dados:                                                                   # laço de repetição para percorrer o cabeçalho
            t = len(col)                                                                    # variavel recebe o tamanho de cada cabeçalho
            if t > 15:                                                                      # se cabeçalho maior que 15 strings
                n = dados[col].str.split(" ", n=1, expand=True)                             # divide a coluna pelo delimitador "espaço"
                name1 = col[:-10]                                                           # pega os dados para formar o cabeçalho da primeira coluna
                name2 = col[-10:]                                                           # pega os dados para formar o cabeçalho da segunda coluna
                dados[name1] = n[0]                                                         # faz inserção dos dados da primeira coluna no name1
                dados[name2] = n[1]                                                         # faz inserção dos dados da segunda coluna no name2
                dados.drop(columns=[name1 + name2], inplace=True)                           # exclui a coluna que estava maior do que 15
                dados = dados[['Regiões do IMEA ', 'Centro-Sul', 'Médio-Norte',
                               'Nordeste', 'Noroeste', 'Norte', 'Oeste', 'Sudeste',
                               'Mato Grosso']]                                              # ordena as colunas
        list = np.array(dados)                                                              # transforma dataframe em array
        list = np.delete(list, np.s_[-4:], axis=0)                                          # remove as ultimas linhas de sujeira
        linha = len(list)                                                                   # variavel recebe a quantidade de linhas
        for i in range(linha):                                                              # laço de repetição para percorrer as linhas
            for j in range(9):                                                              # laço de repetição para percorrer as colunas
                if j > 0:                                                                   # começar o tratamento da segunda coluna
                   list[i][j] = list[i][j].translate(
                       {ord(k): None for k in 'jan%/fev'})                                  # deixa apenas os numeros e a virgula
                   list[i][j] = list[i][j][-6:]                                             # deixa apenas os ultimos 6 caracteres
                   list[i][j] = list[i][j].strip()
                   list[i][j] = list[i][j].split(" ")[-1]
                   list[i][j] = float(list[i][j].replace(',', '.'))                         # transforma a porcentagem em float
                   if list[i][j] > 100:                                                     # se a porcetagem maior que 100
                     list[i][j] = str(list[i][j])                                           # transforma a porcentagem novamente em string
                     list[i][j] = list[i][j][-5:]                                           # deixa apenas os ultimos 5 caracteres
                     list[i][j] = float(list[i][j])                                         # transforma novamente em float

                if j == 0:                                                                  # faz a seleção da primeira coluna
                  list[i][j] = list[i][j][-9:]                                              # deixa apenas as 9 ultimas strings
                  if len(list[i][j]) < 9:                                                   # se a data tiver apenas 9 string
                      list[i][j] = list[i][j].zfill(9)                                      # faz a inclusão do 0 à esquerda

        return(list)
    
    except:
        print("Erro no tratamento")


def enviadados(dados):
    con = psycopg2.connect(host='localhost', database ='teste',
                           user='postgres',password='root')
    cur = con.cursor()    
    sql = 'SELECT "Data" FROM dados'
    try:
        cur.execute(sql)
        recupera = cur.fetchall()
        linha = len(dados)
        data = []
        for n in recupera:
            for j in n:
                j = j.strftime("%d-%b-%y")
                j = j.lower()
                data = data + [j] 
       
        for i in range(linha):                                                                  #laço de repetição para percorrer as linhas
            if dados[i][0] not in data:
        

                sql = ("INSERT INTO dados VALUES ('%s','%f','%f','%f','%f','%f','%f','%f','%f');"
                       %(dados[i][0],dados[i][1],dados[i][2],dados[i][3],dados[i][4],dados[i][5],
                         dados[i][6],dados[i][7],dados[i][8]))
                cur.execute(sql)
                con.commit ()
                print(cur.rowcount, "registro inserido.")
                
    except:
        print("Erro ao inserir")


caminho = ("C:\\download_pdf")
buscando = buscandopdf(caminho)
qtd = len(buscando)
for i in range(qtd):
    lendo = lerPDF(buscando[i])   
    dadoslimpos = limpadados(lendo)
    enviar = enviadados(dadoslimpos)


