# -*- coding: utf-8 -*-

from tabula import read_pdf
import numpy as np
import psycopg2
import os

pasta = "pdf"
caminho = []
for l in os.listdir(pasta):
    if l.endswith('.pdf'):
        caminho = caminho + [l]
        
for c in range(len(caminho)):
    pdf_path = ("./pdf/%s")%caminho[c]
    df = read_pdf(pdf_path,pages='all', encoding='ANSI', stream=True)                   #carrega os pdf
    df = df.dropna(axis=0, thresh=2)                                                    #remove as linhas onde só existe um dado
    df = df.dropna(axis=1, how='all')                                                   #remove as colunas que não tem nenhum dado
    for col in df:                                                                      #laço de repetição para percorrer o cabeçalho 
        t = len(col)                                                                    #variavel recebe o tamanho de cada cabeçalho
        if t > 15:                                                                      #se cabeçalho maior que 15 strings
            n = df[col].str.split(" ", n = 1, expand = True)                            #divide a coluna pelo delimitador "espaço"
            name1 = col[:-10]                                                           #pega os dados para formar o cabeçalho da primeira coluna
            name2 = col[-10:]                                                           #pega os dados para formar o cabeçalho da segunda coluna
            df[name1] = n[0]                                                            #faz inserção dos dados da primeira coluna no name1
            df[name2] = n[1]                                                            #faz inserção dos dados da segunda coluna no name2
            df.drop(columns =[name1 + name2], inplace = True)                           #exclui a coluna que estava maior do que 15
            df = df[['Regiões do IMEA ','Centro-Sul','Médio-Norte',
                     'Nordeste','Noroeste','Norte','Oeste','Sudeste','Mato Grosso']]    #ordena as colunas
    list = np.array(df)                                                                 #transforma dataframe em array
    list = np.delete(list, np.s_[-4:], axis=0)                                          #remove as ultimas linhas de sujeira
    linha = len(list)                                                                    #variavel recebe a quantidade de linhas
    for i in range(linha):                                                              #laço de repetição para percorrer as linhas
        for j in range(9):                                                              #laço de repetição para percorrer as colunas
            if j > 0 :                                                                  #começar o tratamento da segunda coluna
                list[i][j] = list[i][j].translate({ord(k):None for k in 'jan%/fev'})    #deixa apenas os numeros e a virgula
                list[i][j] = list[i][j][-6:]                                            #deixa apenas os ultimos 6 caracteres
                list[i][j] = float(list[i][j].replace(',', '.'))
                if list[i][j] > 100 :
                    list[i][j] = str(list[i][j])
                    list[i][j] = list[i][j][-5:]
                    list[i][j] = float(list[i][j])
                
            if j == 0 :                                                                 #faz a seleção da primeira coluna
                list[i][j] = list[i][j][-9:]                                            #deixa apenas as 9 ultimas strings
                if len(list[i][j]) < 9:
                    list[i][j] = list[i][j].zfill(9)
    con = psycopg2.connect(host='localhost', database ='teste',
                       user='postgres',password='root')
    cur = con.cursor()
    sql = 'SELECT "Data" FROM dados'
    cur.execute(sql)
    recupera = cur.fetchall()
    teste = []
    for n in recupera:
        for j in n:
            j = j.strftime("%d-%b-%y")
            j = j.lower()
            teste = teste + [j] 
    for i in range(linha):                                                              #laço de repetição para percorrer as linhas
        if list[i][0] not in teste:
            sql = ("INSERT INTO dados VALUES ('%s','%f','%f','%f','%f','%f','%f','%f','%f');"
                   %(list[i][0],list[i][1],list[i][2],list[i][3],list[i][4],list[i][5],list[i][6],
                     list[i][7],list[i][8]))
            cur.execute(sql)
            con.commit ()
            print(cur.rowcount, "registro inserido.")
