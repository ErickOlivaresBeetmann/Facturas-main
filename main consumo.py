# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import date,datetime, timedelta

import pyodbc
import pandas as pd
from bs4 import BeautifulSoup

def crear_conexion_SQL():
    """Establecer la conexión con el servidor SQL."""

    try:

        direccion_servidor = "tcp:beetmann-energy.database.windows.net"
        nombre_bd = "mercados"
        nombre_usuario = "adm"
        password = "MercadosBD20"
        
        cnxn = pyodbc.connect("DRIVER={SQL Server};SERVER="+direccion_servidor+";DATABASE="+nombre_bd+";UID="+nombre_usuario+";PWD="+password)
        print('Conexión a la base de datos creada.')
        return cnxn

    except:
        return print("Error en la conexión con la base de datos")





def main():
	    
	directory = 'files'
	data = {}
	lista_fecha = []
	lista_monto_horario=[]
	lista_potencia_mda=[]
	lista_precio=[]
    
    lista_fecha_mtr = []      
    lista_potencia_mtr=[]
    lista_precio_mtr=[]
    lista_fdp=[]
    lista_fuente_mtr=[]


	for i, f_name in enumerate(os.listdir(directory)):

		# Obtener archivo de factura en formato XML
		f = open(directory + '/' + f_name,'r', encoding='utf-8')
		xml_data = BeautifulSoup(f.read(),'xml')

		FUECD=xml_data.find('estadodecuenta')['FUECD']
		terminacion_fuf=['P00','C00']
		# Lista que contendrá las facturas, a posicion 0 tendra la factura por parte del participante y la posicion 1 del CENACE
		facturas=[]
		
		for terminacion in terminacion_fuf:
			cadena=FUECD+terminacion
			factura=xml_data.find("factura", {"fuf": cadena})
			facturas.append(factura)
			pass
		ful=FUECD+'-A02030'
		datos_mda=facturas[1].find('concepto',{"ful": ful})
		registros_hora_mda=datos_mda.find_all('registro')

		mda_dataFrame=pd.DataFrame()


		for registro in registros_hora_mda:
			lista_fecha.append(ful)
			lista_monto_horario.append(registro['MONTO_HORARIO'])
			lista_potencia_mda.append(registro['POTENCIA_MDA'])
			lista_precio.append(registro['PRECIO'])
       
        ful=FUECD+'-B02030'
        datos_mtr=facturas[0].find('concepto',{"ful": ful})
        fuente_mtr='p'
        if datos_mtr==None:
            print('MTR no encontrado en participante, buscando en fac. CENACE')
            ful=FUECD+'-B02030'
            datos_mtr=facturas[1].find('concepto',{"ful": ful})
            fuente_mtr='c'
    
        if datos_mtr==None:
            print('Error al encontrar datos MTR')
        else:
            registros_hora_mtr=datos_mtr.find_all('registro')
    
            # Data frame que contendrá los datos MTR
            mtr_dataFrame=pd.DataFrame()


            for registro in registros_hora_mtr:
                lista_fecha_mtr.append(ful)
                lista_monto_horario_mtr.append(registro['MONTO_HORARIO'])
                lista_potencia_mtr.append(registro['POTENCIA_MTR'])
                lista_precio_mtr.append(registro['PRECIO'])
                lista_fdp.append(registro['FDP'])
                lista_fuente_mtr.append(fuente_mtr)

    mtr_dataFrame['Fecha']=lista_fecha_mtr
    mtr_dataFrame['FDP']=lista_fdp
    mtr_dataFrame['Potencia horario']=lista_potencia_mtr
    mtr_dataFrame['Precio']=lista_precio_mtr
    mtr_dataFrame['Monto horario']=lista_monto_horario_mtr
    mtr_dataFrame['Fuente']=lista_fuente_mtr

    
	mda_dataFrame['fecha']=lista_fecha
	mda_dataFrame['Potencia horario']=lista_potencia_mda
	mda_dataFrame['Precio']=lista_precio
	mda_dataFrame['Monto horario']=lista_monto_horario


	pd.set_option('display.max_rows', None)
	pd.set_option('display.max_columns', None)
	pd.set_option('display.width', None)
	pd.set_option('display.max_colwidth', -1)
	print(mda_dataFrame)



	

	# Aqui va el resto del codigo


if __name__ == "__main__":
	main()
