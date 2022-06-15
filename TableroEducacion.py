# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 16:57:18 2022

@author: Lisa
"""

import os
import pandas as pd
import numpy as np


# Selección de directorio
os.chdir("C:/Users/Lisa/Documents/Bases de Python/Bases Actualizables/Ciclo3/")

df_5w  = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Bases Actualizables/Ciclo3/5W_Colombia_-_RMRP_2022_Consolidado tres_25052022 (1).xlsx")

#agrego una nueva columna concatenando departamento y municipio
df_5w["Full Name"] = df_5w["Admin Departamento"] + df_5w["Admin Municipio"]

df_5w['Mes de atención'].unique()

df_5w['Sector'].unique()

mes = ['04_Abril']
sector = ['Educación']

df_5w.columns 
#con la que trabajaré apartir de ahora 
df_5w_sector_mes = df_5w[(df_5w['Sector'].isin(sector))&(df_5w['Mes de atención'].isin(mes))]

#Otro Formato
#df_5w_sector_mes2 = df_5w[(df_5w['_ Sector']=='Educación')&(df_5w['Mes de atención'] == '03_Marzo')]

#Cargo api
df_api_ind_mpio  = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Bases Actualizables/Ciclo3/API_Consolidado_ciclo_TRES_ GENERAL_27052022 (2).xlsx", sheet_name= "Indicador y Municipio")
df_api_sect_nac  = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Bases Actualizables/Ciclo3/API_Consolidado_ciclo_TRES_ GENERAL_27052022 (2).xlsx", sheet_name= "Sector Nacional")
df_api_ind_mpio["Full Name"] = df_api_ind_mpio["Departamento"] + df_api_ind_mpio["Municipio"]

df_api_sector_mes = df_api_ind_mpio[(df_api_ind_mpio['Sector'].isin(sector))&(df_api_ind_mpio['Mesdeatención'].isin(mes))]
df_api_sect_nac = df_api_sect_nac[(df_api_sect_nac['Sector'].isin(sector))&(df_api_sect_nac['Mesdeatención'].isin(mes))]
#df_api_sector_mes2 = df_api_ind_mpio[(df_api_ind_mpio['Sector']=='Educación')&(df_api_ind_mpio['Mesdeatención'] == '03_Marzo')]


#divipola

divipola = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Bases Actualizables/divipolita.xlsx")

#agrego una nueva columna concatenando departamento y municipio
divipola["Full Name"] = divipola["Departamento"] + divipola["Municipio"]
divipola["Full Name"] = divipola["Full Name"].drop_duplicates()


def standardize_territories(column):
    column = column.str.replace("_"," ", regex=True)
    column = column.map(lambda x: x.lower())
    column = column.map(lambda x: x.strip())
    column = column.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    column = column.str.replace(r'[^\w\s]+', '', regex=True)
    column = column.str.replace("narinotumaco","narinosan andres de tumaco", regex=True)
    column = column.str.replace("antioquiael carmen de viboral","antioquiacarmen de viboral", regex=True)
    column = column.str.replace("caldasvillamaria","caldasvilla maria", regex=True)
    return column


# Estandarizando nombres de departamentos ...
df_5w_sector_mes['Full Name'] = standardize_territories(df_5w_sector_mes['Full Name'])
df_api_sector_mes['Full Name'] = standardize_territories(df_api_sector_mes['Full Name'])
divipola['Full Name'] = standardize_territories(divipola['Full Name'])



#prueba = df_5w_sector_mes['Full Name'].sort_values()

#prueba1 = divipola['Full Name'].sort_values()

# Adicionar el divipola más adecuado
df_5w_sector_mes = pd.merge(df_5w_sector_mes, divipola, how= 'left', left_on = 'Full Name',
                 right_on = 'Full Name')

if df_5w_sector_mes['Full Name'].isna().sum() > 1:
    print('Ajustar full name')
    
if df_5w_sector_mes['dpto'].isna().sum() > 1:
    print('Ajustar Divipola dpto')
    
if df_5w_sector_mes['mpio'].isna().sum() > 1:
    print('Ajustar Divipola mpio')

## revisar cuales son los mpios con nan
nan_values = df_5w_sector_mes[df_5w_sector_mes['mpio'].isna()]

val = divipola[divipola['Municipio'].str.contains("villa")]

# Adicionar el divipola más adecuado
df_api_sector_mes = pd.merge(df_api_sector_mes, divipola, how= 'left', left_on = 'Full Name',
                 right_on = 'Full Name')


if df_api_sector_mes['Full Name'].isna().sum() > 1:
    print('Ajustar full name')
    
if df_api_sector_mes['dpto'].isna().sum() > 1:
    print('Ajustar Divipola dpto')
    
if df_api_sector_mes['mpio'].isna().sum() > 1:
    print('Ajustar Divipola mpio')
    
### tablas dinamicas

#Cifras claves 

Pobla_meta_rmrp = "427361"
reque_finan = "50.1M USD"


#obtengo el dato Beneficiarios recibieron una o más asistencias por parte del SECTOR 
benef_mes = round(df_api_sect_nac['bene_mensuales'].sum())

df_5w_sector_mes.columns

#definir el nro de Departamentos 5W
no_dpto = df_5w_sector_mes.groupby(['Departamento', "dpto"], as_index=False, sort=False).agg({'Total beneficiarios nuevos durante el mes':'sum'}).round(0)
no_dpto = no_dpto[no_dpto['Total beneficiarios nuevos durante el mes'] != 0]
dpto_Alcanzado = no_dpto
dpto_Alcanzado = dpto_Alcanzado.reset_index()
dpto_Alcanzado.index = dpto_Alcanzado.index + 1
#dpto = no_dpto[['Departamento',"dpto"]]
no_dpto = no_dpto["dpto"].nunique()

#definir el nro de Departamentos API
#no_dpto = df_api_sector_mes.groupby('Departamento_x', as_index=False, sort=False).agg({'bene_nuevos':'sum'}).round(0)
#no_dpto= no_dpto[no_dpto['bene_nuevos'] != 0]


#definir nro de Municipios alcanzados 5W

no_mpio = df_5w_sector_mes.groupby(['Municipio','mpio'], as_index=False, sort=False).agg({'Total beneficiarios nuevos durante el mes':'sum'}).round(0)
no_mpio = no_mpio[no_mpio['Total beneficiarios nuevos durante el mes'] != 0]
muni_Alcanzado = no_mpio
muni_Alcanzado = muni_Alcanzado.reset_index()
muni_Alcanzado.index = muni_Alcanzado.index + 1
no_mpio = no_mpio['mpio'].nunique()



#Definir el nro de Organizaciones participantes 5w Socio princiapl

no_socios = df_5w_sector_mes.groupby('Socio Principal Nombre', as_index=False, sort=False).agg({'Total beneficiarios nuevos durante el mes':'sum'}).round(0)
no_socios = no_socios[no_socios['Total beneficiarios nuevos durante el mes'] != 0]
socios = no_socios[['Socio Principal Nombre']]
no_socios = no_socios['Socio Principal Nombre'].nunique()

#Definir tabla pivote con los socios 

right2_join = df_5w_sector_mes
right2_join.columns
right2_join["Socio Implementador Nombre"] = right2_join["Socio Implementador Nombre"].fillna("ninguno")
right2_join = right2_join[['Full Name','Socio Principal Nombre','Socio Implementador Nombre','Total beneficiarios nuevos durante el mes']]
sociospi = pd.pivot_table(right2_join, index =['Socio Principal Nombre','Socio Implementador Nombre'], values ='Total beneficiarios nuevos durante el mes',aggfunc= sum).reset_index()
sociospi = sociospi[sociospi['Total beneficiarios nuevos durante el mes'] != 0]
nsociospi = sociospi['Socio Principal Nombre'].nunique()


if (nsociospi != no_socios) :
    print("el numero de socios no es igual, ERROR")
else: 
    print("son iguales")
    
#crear un nuevo dataframe con las variables calculadas, para introducir luego otra hoja de excel con ellas. 
cifras_clave= pd.DataFrame([{'Cifras':benef_mes, 'Indicador': 'Número de Beneficiarios recibieron una o más asistencias por parte del SECTOR educacion-BENEFICIARIOS Mensual'},
                            {'Cifras':Pobla_meta_rmrp, 'Indicador':'poblacion respuesta del rmrp'}, 
                            {'Cifras':reque_finan, 'Indicador':'Requerimientos financieros para el sector educacion en Colombia en 2022'}, 
                            {'Cifras':no_dpto, 'Indicador':'nro de Departamentos'},
                            {'Cifras':no_mpio, 'Indicador':'nro de Municipios alcanzados'},
                            {'Cifras':no_socios, 'Indicador':'nro de organizaciones que reportaron'},  
                           ],
                            columns=['Cifras', 'Indicador'])

#cifras_clave.head(20)

Objetivos = ["Estrategias programáticas para asegurar el derecho a la educación de niñas,niños, adolescentes y jóvenes.",
          "Fortalecimiento de los procesos asociados a la educación formal, no formal e informal",
          "Desarrollo de los procesos de gestión de matrícula",
          "Formación y acompañamiento a familias y comunidades",
          "Implementación de programas de acompañamiento psicosocial",
          "Socialización/Orientación de herramientas, circulares, rutas, instrumentos de política",
          "Desarrollo de campañas de comunicación ",
          "Procesos de apoyo a la protección de las trayectorias educativas de NNA refugiados y migrantes y de comunidades de acogida",
          "Estrategias de permanencia",
          "Entrega de insumos educativos a estudiantes, IE, centros de educación inicial",
          "Alimentación escolar",
          "Búsqueda activa", 
          "Metodologías de inclusión para NNA con diversidad funcional"]

Objetivos = pd.DataFrame(Objetivos, columns= ["Objetivos sectoriales para la respuesta 2022"])

Objetivos = Objetivos.set_index('Objetivos sectoriales para la respuesta 2022')


df_5w_sector_mes['Indicador'].unique()
ind0= ""
ind1=df_5w_sector_mes.loc[df_5w_sector_mes['Indicador'] == '# de maestros, funcionarios o socios capacitados / empoderados para mejorar el acceso y la calidad de la educación',  'Total beneficiarios nuevos durante el mes'].sum()
ind2=df_5w_sector_mes.loc[df_5w_sector_mes['Indicador'] == '# de refugiados y migrantes inscritos en instituciones educativas formales o inscritos en actividades / programas de educación alternativa o no formal','Total beneficiarios nuevos durante el mes'].sum()
ind3=df_5w_sector_mes.loc[df_5w_sector_mes['Indicador'] == '# de refugiados y migrantes que son niños, adolescentes o jóvenes que reciben apoyo con suministros o servicios.', 'Total beneficiarios nuevos durante el mes'].sum()
ind4=df_5w_sector_mes.loc[df_5w_sector_mes['Indicador'] == '# de actividades de información, sensibilización, promoción y difusión sobre la importancia del derecho a la educación, la disponibilidad y la calidad de la educación',  'Total beneficiarios nuevos durante el mes'].sum()
ind5=df_5w_sector_mes.loc[df_5w_sector_mes['Indicador'] == '# de refugiados y migrantes matriculados en instituciones educativas formales o alternativas/programas de educación no formal que completan el ciclo escolar correspondiente', 'Total beneficiarios nuevos durante el mes'].sum()

datos = [
  {'Cifras':ind0, 'Indicador':'Principales actividades y benef'},
  {'Cifras':ind1, 'Indicador':'# de maestros, funcionarios o socios capacitados / empoderados para mejorar el acceso y la calidad de la educación (5w)'},
  {'Cifras':ind2, 'Indicador':'# de refugiados y migrantes inscritos en instituciones educativas formales o inscritos en actividades / programas de educación alternativa o no formal(5w)'},
  {'Cifras':ind3, 'Indicador':'# de refugiados y migrantes que son niños, adolescentes o jóvenes que reciben apoyo con suministros o servicios.(5w)'},
  {'Cifras':ind4, 'Indicador':'# de actividades de información, sensibilización, promoción y difusión sobre la importancia del derecho a la educación, la disponibilidad y la calidad de la educación(5w)'}, 
  {'Cifras':ind5, 'Indicador':'# de refugiados y migrantes matriculados en instituciones educativas formales o alternativas/programas de educación no formal que completan el ciclo escolar correspondiente(5w)'}, 
  ]
titulo = datos.pop(0)

indicadores_5w= pd.DataFrame(
    data=datos,
    columns=['Cifras', 'Indicador']
)

indicadores_5w.index.name = titulo['Indicador']


sociosxmun = pd.pivot_table(df_5w_sector_mes,index=['Admin Municipio','Socio Principal Nombre'], columns = 'Indicador', values='Total beneficiarios nuevos durante el mes').round(0).reset_index()
sociosxmun= sociosxmun.replace(np.nan,0)
sociosxmun.dtypes

sociosxmun['# de actividades de información, sensibilización, promoción y difusión sobre la importancia del derecho a la educación, la disponibilidad y la calidad de la educación'] = sociosxmun['# de actividades de información, sensibilización, promoción y difusión sobre la importancia del derecho a la educación, la disponibilidad y la calidad de la educación'].astype(int)
sociosxmun["# de maestros, funcionarios o socios capacitados / empoderados para mejorar el acceso y la calidad de la educación"] = sociosxmun["# de maestros, funcionarios o socios capacitados / empoderados para mejorar el acceso y la calidad de la educación"].astype(int)
sociosxmun['# de refugiados y migrantes inscritos en instituciones educativas formales o inscritos en actividades / programas de educación alternativa o no formal'] = sociosxmun['# de refugiados y migrantes inscritos en instituciones educativas formales o inscritos en actividades / programas de educación alternativa o no formal'].astype(int)
sociosxmun['# de refugiados y migrantes que son niños, adolescentes o jóvenes que reciben apoyo con suministros o servicios.'] = sociosxmun['# de refugiados y migrantes que son niños, adolescentes o jóvenes que reciben apoyo con suministros o servicios.'].astype(int)
sociosxmun['# de refugiados y migrantes matriculados en instituciones educativas formales o alternativas/programas de educación no formal que completan el ciclo escolar correspondiente'] = sociosxmun['# de refugiados y migrantes matriculados en instituciones educativas formales o alternativas/programas de educación no formal que completan el ciclo escolar correspondiente'].astype(int)

sociosxmun.dtypes
# Applying the condition
sociosxmun['# de actividades de información, sensibilización, promoción y difusión sobre la importancia del derecho a la educación, la disponibilidad y la calidad de la educación'] = np.where(sociosxmun['# de actividades de información, sensibilización, promoción y difusión sobre la importancia del derecho a la educación, la disponibilidad y la calidad de la educación']>0, 1, 0)
sociosxmun["# de maestros, funcionarios o socios capacitados / empoderados para mejorar el acceso y la calidad de la educación"] = np.where(sociosxmun["# de maestros, funcionarios o socios capacitados / empoderados para mejorar el acceso y la calidad de la educación"]>0, 1, 0)
sociosxmun['# de refugiados y migrantes inscritos en instituciones educativas formales o inscritos en actividades / programas de educación alternativa o no formal'] = np.where(sociosxmun['# de refugiados y migrantes inscritos en instituciones educativas formales o inscritos en actividades / programas de educación alternativa o no formal']>0, 1, 0)
sociosxmun['# de refugiados y migrantes que son niños, adolescentes o jóvenes que reciben apoyo con suministros o servicios.'] = np.where(sociosxmun['# de refugiados y migrantes que son niños, adolescentes o jóvenes que reciben apoyo con suministros o servicios.']>0, 1, 0)
sociosxmun['# de refugiados y migrantes matriculados en instituciones educativas formales o alternativas/programas de educación no formal que completan el ciclo escolar correspondiente']  = np.where(sociosxmun['# de refugiados y migrantes matriculados en instituciones educativas formales o alternativas/programas de educación no formal que completan el ciclo escolar correspondiente'] >0, 1, 0)

# Select 1 to 3 columns to sum
sociosxmun['Sum']=sociosxmun.iloc[:,3:7].sum(axis=1)
sociosxmun = sociosxmun[sociosxmun.Sum != 0]
sociosxmun.pop("Sum")
sociosxmun = sociosxmun.replace({1: "X", 0: ""})

#agregar los valores 
sociosxmun.rename(columns = {'Admin Municipio':'muni'}, inplace = True)
sociosxmun["Numero"] = sociosxmun.muni.astype('category').cat.codes.add(1)
sociosxmun = sociosxmun.set_index('Numero')


Writer= pd.ExcelWriter("C:/Users/Lisa/Documents/Bases de Python/Bases Actualizables/tablero_Educacion_04_2022.xlsx")

cifras_clave.to_excel(Writer, sheet_name='Cifras Clave.xlsx')
Objetivos.to_excel(Writer, sheet_name='Objetivossectoriales.xlsx')
indicadores_5w.to_excel(Writer, sheet_name='Principale.act.benef.xlsx')
muni_Alcanzado.to_excel(Writer, sheet_name='Municipios_Alcanzados(5w).xlsx')
dpto_Alcanzado.to_excel(Writer, sheet_name='Depto_Alcanzados(5w).xlsx')
sociosxmun.to_excel(Writer, sheet_name='SociosvsMpiovsInd.xlsx')
sociospi.to_excel(Writer, sheet_name='SociosPI.xlsx')


Writer.save()

